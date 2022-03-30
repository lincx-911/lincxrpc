package client

import (
	"context"
	"time"

	"github.com/lincx-911/lincxrpc/common/metadata"
	"github.com/lincx-911/lincxrpc/protocol"
)

// MetaDataWrapper meta拦截器
type MetaDataWrapper struct {
	defaultClientInterceptor
}

func (w *MetaDataWrapper) WrapCall(option *SGOption, callFunc CallFunc) CallFunc {
	return func(ctx context.Context, ServiceMethod string, arg interface{}, reply interface{}) error {
		ctx = wrapContext(ctx, option)
		return callFunc(ctx, ServiceMethod, arg, reply)
	}
}

func (w *MetaDataWrapper) WrapGo(option *SGOption, goFunc GoFunc) GoFunc {
	return func(ctx context.Context, ServiceMethod string, arg interface{}, reply interface{}, done chan *Call) *Call {
		ctx = wrapContext(ctx, option)

		return goFunc(ctx, ServiceMethod, arg, reply, done)
	}
}
// wrapContext 设置上下文ctx参数
func wrapContext(ctx context.Context, option *SGOption) context.Context {
	timeout := time.Duration(0)
	deadline, ok := ctx.Deadline()
	if ok {
		//已经有了deadline了，此次请求覆盖原始的超时时间
		timeout = time.Until(deadline)
	}
	if timeout == time.Duration(0) && option.RequestTimeout != time.Duration(0) {
		//ctx里没有设置超时时间，用option中设置的超时时间
		timeout = option.RequestTimeout
	}
	ctx, _ = context.WithTimeout(ctx, timeout)

	metaData := metadata.FromContext(ctx)
	metaData[protocol.RequestTimeoutKey] = uint64(timeout)

	if option.Auth != "" {
		metaData[protocol.AuthKey] = option.Auth
	}
	// 如果ctx中有设置auth 则 用ctx中的
	if auth, ok := ctx.Value(protocol.AuthKey).(string); ok {
		metaData[protocol.AuthKey] = auth
	}
	deadline, ok = ctx.Deadline()
	if ok {
		metaData[protocol.RequestDeadlineKey] = deadline
	}
	ctx = metadata.WithMeta(ctx, metaData)
	return ctx
}
