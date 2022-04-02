package client

import (
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/lincx-911/lincxrpc/codec"
	"github.com/lincx-911/lincxrpc/protocol"
	"github.com/lincx-911/lincxrpc/registry"
	"github.com/lincx-911/lincxrpc/selector"
	"github.com/lincx-911/lincxrpc/transport"
)

var ErrorShutDown = errors.New("client is shut down")

type RPCClient interface {
	Go(ctx context.Context, serviceMethod string, arg interface{}, reply interface{}, done chan *Call) *Call
	Call(ctx context.Context, serviceMethod string, arg interface{}, reply interface{}) error
	Close() error
	IsShutDown() bool
	IsDegrade() bool
}

type SGClient interface {
	Go(ctx context.Context, serviceMethod string, arg interface{}, reply interface{}, done chan *Call) (*Call, error)
	Call(ctx context.Context, serviceMethod string, arg interface{}, reply interface{}) error
	Close() error
}

type Call struct {
	ServiceMethod string      //服务名.方法名
	Args          interface{} //参数
	Reply         interface{} //返回值
	Error         error       //错误信息
	Done          chan *Call  //在调用结束时调用
}

func (c *Call) done() {
	c.Done <- c
}

type sgClient struct {
	shutdown             bool
	option               SGOption
	clients              sync.Map //map[string]RPCClient
	clientsHeartbeatFail map[string]int
	breakers             sync.Map //map[string]CircuitBreaker
	watcher              registry.Watcher

	serversMu sync.RWMutex
	servers   []registry.Provider
	mu        sync.Mutex
}

func NewRPCClient(network, addr string, option Option) (RPCClient, error) {
	client := new(simpleClient)
	client.option = option
	client.network = network
	client.addr = addr
	client.codec = codec.GetCodec(option.SerializeType)

	tr := transport.NewTransport(option.TransportType)
	err := tr.Dial(network, addr, transport.DialOption{Timeout: option.DialTimeout})
	if err != nil {

		return nil, err
	}
	client.rwc = tr
	go client.input()
	if client.option.Heartbeat && client.option.HeartbeatInterval > 0 {
		go client.heartbeat()
	}
	log.Printf("successfully conneted to %s@%s\n", network, addr)
	return client, nil
}

func NewSGClient(option SGOption) SGClient {
	s := new(sgClient)
	s.option = option
	AddWrapper(&s.option, &MetaDataWrapper{})
	providers := s.option.Registry.GetServiceList()

	s.watcher = s.option.Registry.Watch()
	go s.watchService(s.watcher)
	s.serversMu.Lock()
	defer s.serversMu.Unlock()
	s.servers = append(s.servers, providers...)

	if s.option.Heartbeat {
		go s.heartbeat()
		s.option.SelectOption.Filters = append(s.option.SelectOption.Filters,
			selector.DegradeProviderFilter())
	}
	if s.option.Tagged && s.option.Tags != nil {
		s.option.SelectOption.Filters = append(s.option.SelectOption.Filters,
			selector.TaggedProviderFilter(s.option.Tags))
	}
	return s
}

func (c *sgClient) Go(ctx context.Context, serviceMethod string, arg interface{}, reply interface{}, done chan *Call) (*Call, error) {
	if c.shutdown {
		return nil, ErrorShutDown
	}
	_, client, err := c.selectClient(ctx, serviceMethod, arg)
	if err != nil {
		return nil, err
	}
	return c.wrapGo(client.Go)(ctx, serviceMethod, arg, reply, done), nil
}

func (c *sgClient) wrapGo(goFunc GoFunc) GoFunc {
	for _, wrapper := range c.option.Wrappers {
		goFunc = wrapper.WrapGo(&c.option, goFunc)
	}
	return goFunc
}

func (c *sgClient) Call(ctx context.Context, serviceMethod string, arg interface{}, reply interface{}) error {
	provider, rpcClient, err := c.selectClient(ctx, serviceMethod, arg)
	if err != nil && c.option.FailMode == FailFast {

		return err
	}
	var connectErr error

	switch c.option.FailMode {
	case FailRetry:
		retries := c.option.Retries
		for retries > 0 {
			retries--

			if rpcClient != nil {

				err = c.wrapCall(rpcClient.Call)(ctx, serviceMethod, arg, reply)
				if err == nil {
					// TODO circuit
					if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
						breaker.(CircuitBreaker).Success()
					}
				} else {
					if _, ok := err.(ServiceError); ok {
						// TODO circuit
						if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
							breaker.(CircuitBreaker).Fail(err)
						}
					}
				}
				return err
			}
			c.removeClient(provider.ProviderKey, rpcClient)
			rpcClient, connectErr = c.getclient(provider)

		}
		if err == nil {
			err = connectErr
		}
		if err == nil {
			// TODO circuit
			if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
				breaker.(CircuitBreaker).Success()
			}
		} else {
			// TODO circuit
			if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
				breaker.(CircuitBreaker).Fail(err)
			}
		}
		return err
	case FailOver:
		retries := c.option.Retries
		for retries > 0 {
			retries--

			if rpcClient != nil {
				err = c.wrapCall(rpcClient.Call)(ctx, serviceMethod, arg, reply)
				if err == nil {
					// TODO circuit
					if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
						breaker.(CircuitBreaker).Success()
					}
				} else {
					if _, ok := err.(ServiceError); ok {
						// TODO circuit
						if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
							breaker.(CircuitBreaker).Fail(err)
						}
					}
				}
				return err
			}
			c.removeClient(provider.ProviderKey, rpcClient)
			provider, rpcClient, connectErr = c.selectClient(ctx, serviceMethod, arg)
		}
		if err == nil {
			err = connectErr
		}
		if err == nil {
			// TODO circuit
			if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
				breaker.(CircuitBreaker).Success()
			}
		} else {
			// TODO circuit
			if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
				breaker.(CircuitBreaker).Fail(err)
			}
		}
		return err
	default:

		err = c.wrapCall(rpcClient.Call)(ctx, serviceMethod, arg, reply)
		if err != nil {
			if _, ok := err.(ServiceError); !ok {
				c.removeClient(provider.ProviderKey, rpcClient)
			}
		}

		if c.option.FailMode == FailSafe {
			err = nil
		}

		if err == nil {
			// TODO circuit
			if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
				breaker.(CircuitBreaker).Success()
			}
		} else {
			// TODO circuit
			if breaker, ok := c.breakers.Load(provider.ProviderKey); ok {
				breaker.(CircuitBreaker).Fail(err)
			}
		}

		return err
	}
}

func (c *sgClient) wrapCall(callFunc CallFunc) CallFunc {
	for _, wrapper := range c.option.Wrappers {
		callFunc = wrapper.WrapCall(&c.option, callFunc)
	}
	return callFunc
}

func (c *sgClient) Close() error {

	c.shutdown = true
	c.mu.Lock()

	c.clients.Range(func(key, value interface{}) bool {
		if client, ok := value.(*simpleClient); ok {
			c.removeClient(key.(string), client)
		}
		return true
	})
	c.mu.Unlock()
	go func() {
		c.option.Registry.Unwatch(c.watcher)
		c.watcher.Close()
	}()

	return nil
}

// ServiceError is an error from server.
type ServiceError string

func (e ServiceError) Error() string {
	return string(e)
}
func (c *sgClient) watchService(watcher registry.Watcher) {
	if watcher == nil {
		return
	}
	for {
		event, err := watcher.Next()
		if err != nil {
			log.Println("watch service error:" + err.Error())
			break
		}

		c.serversMu.Lock()
		c.servers = event.Providers
		c.serversMu.Unlock()
	}
}

func (c *sgClient) selectClient(ctx context.Context, serviceMethod string, arg interface{}) (provider registry.Provider, client RPCClient, err error) {

	provider, err = c.option.Selector.Next(ctx, c.providers(), serviceMethod, arg, c.option.SelectOption)
	if err != nil {
		return
	}

	client, err = c.getclient(provider)
	return
}

var ErrBreakerOpen = errors.New("breaker open")

func (c *sgClient) getclient(provider registry.Provider) (client RPCClient, err error) {
	key := provider.ProviderKey
	breaker, ok := c.breakers.Load(key)
	if ok && !breaker.(CircuitBreaker).AllowRequest() {
		return nil, ErrBreakerOpen
	}
	rc, ok := c.clients.Load(key)

	if ok {
		client = rc.(RPCClient)
		if !client.IsShutDown() {
			return
		} else {
			c.removeClient(key, client)
		}
	}
	rc, ok = c.clients.Load(key)
	if ok {
		client = rc.(RPCClient)
	} else {

		client, err = NewRPCClient(provider.Network, provider.Addr, c.option.Option)
		if err != nil {
			return
		}
		c.clients.Store(key, client)
		//TODO circuitbreak
		if c.option.CircuitBreakerThreshold > 0 && c.option.CircuitBreakerWindow > 0 {
			c.breakers.Store(key, NewDefaultCircuitBreaker(c.option.CircuitBreakerThreshold, c.option.CircuitBreakerWindow))
		}
	}

	return
}

func (c *sgClient) removeClient(clientKey string, client RPCClient) {
	c.clients.Delete(clientKey)
	if client != nil {
		client.Close()
	}
	c.breakers.Delete(clientKey)
}

func (c *sgClient) providers() []registry.Provider {
	c.serversMu.RLock()
	defer c.serversMu.RUnlock()
	return c.servers
}

func (c *sgClient) heartbeat() {
	c.mu.Lock()
	if c.clientsHeartbeatFail == nil {
		c.clientsHeartbeatFail = make(map[string]int)
	}
	c.mu.Unlock()
	if c.option.HeartbeatInterval <= 0 {
		return
	}
	//根据指定的时间间隔发送心跳
	t := time.NewTicker(c.option.HeartbeatInterval)
	for range t.C {
		if c.shutdown {
			t.Stop()
			return
		}
		//遍历每个RPCClient进行心跳检查
		c.clients.Range(func(key, value interface{}) bool {
			err := value.(RPCClient).Call(context.Background(), "", "", nil)
			c.mu.Lock()
			if err != nil {
				//心跳失败进行计数
				if fail, ok := c.clientsHeartbeatFail[key.(string)]; ok {
					fail++
					c.clientsHeartbeatFail[key.(string)] = fail
				} else {
					c.clientsHeartbeatFail[key.(string)] = 1
				}
			} else {
				// 心跳成功则进行恢复
				c.clientsHeartbeatFail[key.(string)] = 0
				c.serversMu.Lock()
				for i, p := range c.servers {
					if p.ProviderKey == key {
						delete(c.servers[i].Meta, protocol.ProviderDegradeKey)
					}
				}
				c.serversMu.Unlock()
			}
			c.mu.Unlock()

			//心跳失败次数超过阈值则进行降级
			if c.clientsHeartbeatFail[key.(string)] > c.option.HeartbeatDegradeThreshold {
				c.serversMu.Lock()
				for i, p := range c.servers {
					if p.ProviderKey == key {
						c.servers[i].Meta[protocol.ProviderDegradeKey] = true
					}
				}
				c.serversMu.Unlock()
			}
			return true
		})
	}
}
