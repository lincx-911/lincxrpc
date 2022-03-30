package server

import (
	"context"

	"github.com/lincx-911/lincxrpc/protocol"
	"github.com/lincx-911/lincxrpc/transport"
)

type ServeFunc func(network, addr string, meta map[string]interface{}) error
type ServeTransportFunc func(tr transport.Transport)
type HandleRequestFunc func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport)
type AuthFunc func(key string) bool
type CloseFunc func() error

// Wrapper server拦截器
type Wrapper interface {
	WrapServe(s *SGServer, serveFunc ServeFunc) ServeFunc
	WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc
	WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc
	WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc
}

type defaultServerInterceptor struct {
}

func (defaultServerInterceptor) WrapServe(s *SGServer, serveFunc ServeFunc) ServeFunc {
	return serveFunc
}

func (defaultServerInterceptor) WrapServeTransport(s *SGServer, transportFunc ServeTransportFunc) ServeTransportFunc {
	return transportFunc
}

func (defaultServerInterceptor) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return requestFunc
}

func (defaultServerInterceptor) WrapClose(s *SGServer, closeFunc CloseFunc) CloseFunc {
	return closeFunc
}
