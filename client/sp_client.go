package client

import (
	"context"
	"errors"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lincx-911/lincxrpc/codec"
	"github.com/lincx-911/lincxrpc/common/metadata"
	"github.com/lincx-911/lincxrpc/protocol"
)

type simpleClient struct {
	codec           codec.Codec
	rwc             io.ReadWriteCloser
	network         string
	addr            string
	pendingCalls    sync.Map
	mutex           sync.Mutex
	degraded        bool
	shutdown        bool
	option          Option
	seq             uint64
	heatbeatFailNum int
}

func (c *simpleClient) IsShutDown() bool {
	return c.shutdown
}

func (c *simpleClient) IsDegrade() bool {
	return c.degraded
}

func (c *simpleClient) Go(ctx context.Context, serviceMethod string, args interface{}, reply interface{}, done chan *Call) *Call {
	call := new(Call)
	call.ServiceMethod = serviceMethod
	call.Args = args
	call.Reply = reply

	if done == nil {
		done = make(chan *Call, 10) // buffered.
	} else {
		if cap(done) == 0 {
			log.Panic("rpc: done channel is unbuffered")
		}
	}
	call.Done = done

	c.send(ctx, call)

	return call
}

// send
// 将参数序列化并通过传输层的接口发送出去，
// 同时将请求缓存到pendingCalls中
func (c *simpleClient) send(ctx context.Context, call *Call) {
	seq := ctx.Value(protocol.RequestSeqKey).(uint64)
	c.pendingCalls.Store(seq, call)

	request := protocol.NewMessage(c.option.ProtocolType)
	request.Seq = seq
	if call.ServiceMethod != "" {
		request.MessageType = protocol.MessageTypeRequest
		serviceMethod := strings.SplitN(call.ServiceMethod, ".", 2)
		request.ServiceName = serviceMethod[0]
		request.MethodName = serviceMethod[1]
	} else {
		request.MessageType = protocol.MessageTypeHeartbeat
	}
	request.SerializeType = c.option.SerializeType
	request.CompressType = c.option.CompressType
	if meta := metadata.FromContext(ctx); meta != nil {
		request.MetaData = meta
	}
	requestData, err := c.codec.Encode(call.Args)
	if err != nil {
		log.Println("client encode error:" + err.Error())
		c.pendingCalls.Delete(seq)
		call.Error = err
		call.done()
		return
	}
	request.Data = requestData
	data := protocol.EncodeMessage(c.option.ProtocolType, request)

	_, err = c.rwc.Write(data)
	if err != nil {
		log.Println("client write error:" + err.Error())
		c.pendingCalls.Delete(seq)
		call.Error = err
		call.done()
		return
	}
}

func (c *simpleClient) Call(ctx context.Context, serviceMethod string, args interface{}, reply interface{}) error {
	seq := atomic.AddUint64(&c.seq, 1)
	ctx = context.WithValue(ctx, protocol.RequestSeqKey, seq)

	done := make(chan *Call, 1)
	call := c.Go(ctx, serviceMethod, args, reply, done)

	select {
	case <-ctx.Done():
		c.pendingCalls.Delete(seq)
		call.Error = errors.New("client request time out")
	case <-call.Done:
	}
	return call.Error
}

func (c *simpleClient) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.shutdown = true

	c.pendingCalls.Range(func(key, value interface{}) bool {
		call, ok := value.(*Call)
		if ok {
			call.Error = ErrorShutDown
			call.done()
		}
		c.pendingCalls.Delete(key)
		return true
	})
	return nil
}

func (c *simpleClient) input() {
	var err error
	var response *protocol.Message
	for err == nil {
		response, err = protocol.DecodeMessage(c.option.ProtocolType, c.rwc)
		if err != nil {
			break
		}
		seq := response.Seq
		callInreface, ok := c.pendingCalls.Load(seq)
		if !ok {
			// 该请求被清理掉了，maybe 是因为已经超时了
			continue
		}
		call := callInreface.(*Call)
		if response.MessageType != protocol.MessageTypeHeartbeat {
			have := response.ServiceName + "." + response.MethodName
			want := call.ServiceMethod
			if have != want {
				log.Fatalf("servicmethod not equal have:%s,want:%s", have, want)
			}
			c.pendingCalls.Delete(seq)
			switch {
			case response.Error != "":
				call.Error = ServiceError(response.Error)
				call.done()
			default:
				err = c.codec.Decode(response.Data, call.Reply)
				if err != nil {
					call.Error = errors.New("reading body " + err.Error())
				}
				call.done()
			}
		}
	}
	log.Println("input error, closing client, error: " + err.Error())
	c.Close()
}

func (c *simpleClient) heartbeat() {
	t := time.NewTicker(c.option.HeartbeatInterval)

	for range t.C {
		if c.shutdown {
			t.Stop()
			return
		}
		err := c.Call(context.Background(), "", nil, nil)
		if err != nil {
			log.Printf("failed to heartbeat to %s@%s\n", c.network, c.addr)
			c.mutex.Lock()
			c.heatbeatFailNum++
			c.mutex.Unlock()
		}

		// 降级处理
		if c.heatbeatFailNum > c.option.HeartbeatDegradeThreshold {
			c.mutex.Lock()
			c.degraded = true
			c.mutex.Unlock()
		}
	}
}
