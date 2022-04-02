package server

import (
	"time"

	"github.com/lincx-911/lincxrpc/codec"
	"github.com/lincx-911/lincxrpc/protocol"
	"github.com/lincx-911/lincxrpc/registry"
	"github.com/lincx-911/lincxrpc/transport"
)

type ShutDownHook func(s *SGServer)

// Option server配置项
type Option struct {
	AppKey         string
	Registry       registry.Registry
	RegisterOption registry.RegisterOption
	ShutDownWait   time.Duration
	ShutDownHooks  []ShutDownHook
	Wrappers       []Wrapper
	Tags           map[string]string
	ProtocolType   protocol.ProtocolType
	SerializeType  codec.SerializeType
	CompressType   protocol.CompressType
	TransportType  transport.TransportType
	HttpsConf HttpsOption
}

// HttpsOption 配置https
type HttpsOption struct{
	On bool // 是否使用https,默认 false
	ServerCrtPath string // 服务器证书路径
	ServerKeyPath string // 服务器证书秘钥路径
	CaCerPath string // CA根证书路径
}

// DefaultOption 默认
var DefaultOption = Option{
	ShutDownWait:  time.Second * 12,
	ProtocolType:  protocol.Default,
	SerializeType: codec.MessagePackType,
	CompressType:  protocol.CompressTypeNone,
	TransportType: transport.TCPTransport,
	HttpsConf: HttpsOption{On: false,},
}
