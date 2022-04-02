package server

import (
	"context"
	"github.com/lincx-911/lincxrpc/protocol"
	"github.com/lincx-911/lincxrpc/transport"
)

type ServerAuthInterceptor struct {
	defaultServerInterceptor
	authFunc AuthFunc
}

func (sai *ServerAuthInterceptor) WrapHandleRequest(s *SGServer, requestFunc HandleRequestFunc) HandleRequestFunc {
	return func(ctx context.Context, request *protocol.Message, response *protocol.Message, tr transport.Transport) {
		if auth, ok := ctx.Value(protocol.AuthKey).(string); ok {
			//鉴权通过则执行业务逻辑
			if sai.authFunc(auth) {
				requestFunc(ctx, response, response, tr)
				return
			}
		}
		//鉴权失败则返回异常
		s.writeErrorResponse(response, tr, "auth failed")
	}
}

