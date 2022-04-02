package server

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/lincx-911/lincxrpc/common"
	"github.com/lincx-911/lincxrpc/common/metadata"
	"github.com/lincx-911/lincxrpc/protocol"
	"github.com/lincx-911/lincxrpc/registry"
	"github.com/lincx-911/lincxrpc/transport"
)

type DefaultServerWrapper struct {
	defaultServerInterceptor
}

func (w *DefaultServerWrapper) WrapServe(s *SGServer, serveFunc ServeFunc) ServeFunc {
	return func(network, addr string, meta map[string]interface{}) error {
		// 注册shutdownHook
		go func(s *SGServer) {
			ch := make(chan os.Signal, 1)
			signal.Notify(ch, syscall.SIGTERM)
			sig := <-ch
			if sig.String() == "terminated" {
				for _, hook := range s.Option.ShutDownHooks {
					hook(s)
				}
				os.Exit(0)
			}
		}(s)
		//服务端注册时，将我们设置的tags作为元数据注册到注册中心
		if meta == nil {
			meta = make(map[string]interface{})
		}

		if len(s.Option.Tags) > 0 {
			meta["tags"] = s.Option.Tags
		}
		meta["services"] = s.Services()
		// TODO registry
		if addr[0]==':'{
			addr = common.LocalIPV4()+addr
			s.addr = addr
		}
		
		
		provider := registry.Provider{
			ProviderKey: network + "@" + addr,
			Network:     network,
			Addr:        addr,
			Meta:        meta,
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption
		r.Register(rOpt, provider)
		log.Printf("registered provider %v for app %s", provider, rOpt)
		//启动http serve
		s.StartGateway()
		return serveFunc(network, addr, meta)
	}
}

func (w *DefaultServerWrapper) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		ctx = metadata.WithMeta(ctx, request.MetaData)
		// 正在处理的请求加1
		atomic.AddInt64(&s.requestInProcess, 1)
		requestFunc(ctx, request, response, tr)
		atomic.AddInt64(&s.requestInProcess, -1)
	}
}

func (w *DefaultServerWrapper) WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc {
	return func() error {
		
		provider := registry.Provider{
			ProviderKey: s.network + "@" + s.addr,
			Network:     s.network,
			Addr:        s.addr,
		}
		r := s.Option.Registry
		rOpt := s.Option.RegisterOption
		r.Unregister(rOpt, provider)
		log.Printf("unregistered provider %v for app %s", provider, rOpt)
		return closeFunc()
	}
}
