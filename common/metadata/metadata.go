package metadata

import (
	"context"

	"github.com/lincx-911/lincxrpc/protocol"
)

// FromContext 从ctx中读取meta数据 格式为map[string]interface{}
func FromContext(ctx context.Context) map[string]interface{} {
	mateData, ok := ctx.Value(protocol.MetaDataKey).(map[string]interface{})

	if !ok || mateData == nil {
		mateData = make(map[string]interface{})
	}
	return mateData
}

// WithMeta 将meta数据设置在ctx中
func WithMeta(ctx context.Context, meta map[string]interface{}) context.Context {
	return context.WithValue(ctx, protocol.MetaDataKey, meta)
}
