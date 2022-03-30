package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/lincx-911/lincxrpc/codec"
)

//-------------------------------------------------------------------------------------------------
//|2byte|1byte  |4byte       |4byte        | header length |(total length - header length - 4byte)|
//-------------------------------------------------------------------------------------------------
//|magic|version|total length|header length|     header    |                    body              |
//-------------------------------------------------------------------------------------------------
var MAGIC = []byte{0xab, 0xba}

const (
	version = 0x00
)

type MessageType byte

// 请求类型
const (
	MessageTypeRequest MessageType = iota
	MessageTypeResponse
	MessageTypeHeartbeat
)

func ParseMessageType(name string) (MessageType, error) {
	switch name {
	case "request":
		return MessageTypeRequest, nil
	case "response":
		return MessageTypeResponse, nil
	case "heartbeat":
		return MessageTypeHeartbeat, nil
	default:
		return MessageTypeRequest, fmt.Errorf("type %s not found", name)
	}
}

func (messageType MessageType) String() string {
	switch messageType {
	case MessageTypeRequest:
		return "request"
	case MessageTypeResponse:
		return "response"
	case MessageTypeHeartbeat:
		return "heartbeat"
	default:
		return "unknown"
	}
}

type CompressType byte

const (
	CompressTypeNone CompressType = iota
)

func ParseCompressType(name string) (CompressType, error) {
	switch name {
	case "none":
		return CompressTypeNone, nil
	default:
		return CompressTypeNone, fmt.Errorf("type %s not found", name)
	}
}

func (compressType CompressType) String() string {
	switch compressType {
	case CompressTypeNone:
		return "none"
	default:
		return "unknown"
	}
}

type StatusCode byte

const (
	StatusOK StatusCode = iota
	StatusError
)

func (code StatusCode) String() string {
	switch code {
	case StatusOK:
		return "ok"
	case StatusError:
		return "error"
	default:
		return "unknown"
	}
}

func ParseStatusCode(name string) (StatusCode, error) {
	switch name {
	case "ok":
		return StatusOK, nil
	case "error":
		return StatusError, nil
	default:
		return StatusError, fmt.Errorf("type %s not found", name)
	}
}

type ProtocolType byte

const (
	Default ProtocolType = iota
)

// Protocol 定义了如何构造和序列化一个完整的消息体
type Protocol interface {
	NewMessage() *Message
	DecodeMessage(r io.Reader) (*Message, error)
	EncodeMessage(message *Message) []byte
}

var protocols = map[ProtocolType]Protocol{
	Default: &RPCProtocol{},
}

const (
	RequestSeqKey      string = "rpc_request_seq"
	RequestTimeoutKey  string = "rpc_request_timeout"
	MetaDataKey        string = "rpc_meta_data"
	AuthKey            string = "rpc_auth"
	RequestDeadlineKey string = "rpc_request_deadline"
	ProviderDegradeKey string = "rpc_provider_degrade"
)

// Header 消息头部
type Header struct {
	Seq           uint64                 // 序号，用于唯一标识请求或者响应
	MessageType   MessageType            //消息类型，用来标识一个消息是请求还是响应
	CompressType  CompressType           //压缩类型，用来标识一个消息的压缩方式
	SerializeType codec.SerializeType    //序列化类型，用来标识消息体采用的编码方式
	StatusCode    StatusCode             //状态类型，用来标识一个请求是否正常
	ServiceName   string                 //服务名称
	MethodName    string                 //方法名称
	Error         string                 //方法调用异常
	MetaData      map[string]interface{} //其他元数据
}

// Message 消息
type Message struct {
	*Header
	Data []byte
}

func (m *Message) Clone() *Message {
	header := m.Header
	res := new(Message)
	res.Header = header
	res.Data = m.Data
	return res
}
func (m *Message) Deadline() (time.Time, bool) {
	if m.MetaData == nil {
		return time.Now(), false
	} else {
		deadline, ok := m.MetaData[RequestDeadlineKey]
		if ok {
			switch deadline := deadline.(type) {
			case time.Time:
				return deadline, ok
			case *time.Time:
				return *deadline, ok
			default:
				return time.Now(), false
			}
		} else {
			return time.Now(), false
		}
	}
}

func NewMessage(t ProtocolType) *Message {
	return protocols[t].NewMessage()
}

func DecodeMessage(t ProtocolType, r io.Reader) (*Message, error) {
	return protocols[t].DecodeMessage(r)
}

func EncodeMessage(t ProtocolType, m *Message) []byte {
	return protocols[t].EncodeMessage(m)
}

type RPCProtocol struct {
}

func (rp *RPCProtocol) NewMessage() *Message {
	return &Message{Header: &Header{}}
}

func (rp *RPCProtocol) DecodeMessage(r io.Reader) (msg *Message, err error) {
	first3bytes := make([]byte, 3)
	_, err = io.ReadFull(r, first3bytes)
	if err != nil {
		return
	}
	if !checkMagic(first3bytes[:2]) {
		err = fmt.Errorf("wrong protocol")
		return
	}
	totalLenBytes := make([]byte, 4)
	_, err = io.ReadFull(r, totalLenBytes)
	if err != nil {
		return
	}
	totalLen := int(binary.BigEndian.Uint32(totalLenBytes))
	if totalLen < 4 {
		err = fmt.Errorf("invalid total length")
		return
	}
	data := make([]byte, totalLen)
	_, err = io.ReadFull(r, data)
	if err != nil {
		return
	}
	headerLen := int(binary.BigEndian.Uint32(data[:4]))
	headerBytes := data[4 : headerLen+4]
	header := &Header{}
	cc := codec.GetCodec(codec.MessagePackType)
	err = cc.Decode(headerBytes, header)
	if err != nil {
		return
	}
	msg = new(Message)
	msg.Header = header
	msg.Data = data[headerLen+4:]
	return
}

func (rp *RPCProtocol) EncodeMessage(message *Message) []byte {
	first3bytes := []byte{MAGIC[0], MAGIC[1], version}
	cc := codec.GetCodec(codec.MessagePackType)
	headerBytes, _ := cc.Encode(message.Header)
	totalLen := 4 + len(headerBytes) + len(message.Data)
	totalLenbytes := make([]byte, 4)
	binary.BigEndian.PutUint32(totalLenbytes, uint32(totalLen))

	data := make([]byte, totalLen+7) // 7:magicbyte+ versionbyte+totalbyte len
	start := 0
	copyFullWithOffset(data, first3bytes, &start)
	copyFullWithOffset(data, totalLenbytes, &start)

	headerLenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(headerLenBytes, uint32(len(headerBytes)))

	copyFullWithOffset(data, headerLenBytes, &start)
	copyFullWithOffset(data, headerBytes, &start)
	copyFullWithOffset(data, message.Data, &start)
	return data
}

func checkMagic(bytes []byte) bool {
	if len(bytes) < 2 {
		return false
	}
	return bytes[0] == MAGIC[0] && bytes[1] == MAGIC[1]
}

func copyFullWithOffset(dst []byte, src []byte, start *int) {
	copy(dst[*start:*start+len(src)], src)
	*start = *start + len(src)
}
