package codec

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/vmihailenco/msgpack/v5"

	proto "github.com/gogo/protobuf/proto"
	pb "google.golang.org/protobuf/proto"
)

// 序列化类型
type SerializeType byte

func (serializeType SerializeType) String() string {
	switch serializeType {
	case MessagePackType:
		return "messagepack"
	case GobType:
		return "gob"
	case JsonType:
		return "json"
	case ProtoBufType:
		return "protobuf"

	default:
		return "unknown"
	}
}

const (
	GobType SerializeType = iota
	JsonType
	ProtoBufType
	MessagePackType
)

func ParseSerializeType(name string) (SerializeType, error) {
	switch name {
	case "messagepack":
		return MessagePackType, nil
	case "gob":
		return GobType, nil
	case "json":
		return JsonType, nil
	case "protobuf":
		return ProtoBufType, nil
	default:
		return MessagePackType, fmt.Errorf("type %s not found", name)
	}
}

var codecs = map[SerializeType]Codec{
	GobType:         &GobCodec{},
	JsonType:        &JSONCodec{},
	ProtoBufType:    &PBCodec{},
	MessagePackType: &MessagePackCodec{},
}

// Codec 序列化/反序列化接口
type Codec interface {
	Encode(i interface{}) ([]byte, error)
	Decode(data []byte, i interface{}) error
}

func GetCodec(st SerializeType) Codec {
	return codecs[st]
}

// GobCodec gob
type GobCodec struct {
}

// Encode encode an object into slice of bytes
func (g *GobCodec) Encode(i interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(i)
	return buf.Bytes(), err
}

// Decode decode an object from slice og byte
func (g *GobCodec) Decode(data []byte, value interface{}) error {
	buf := bytes.NewBuffer(data)
	err := gob.NewDecoder(buf).Decode(value)
	return err
}

// JSONCodec json
type JSONCodec struct{}

// Encode encode an object into slice of bytes
func (j *JSONCodec) Encode(i interface{}) ([]byte, error) {
	return json.Marshal(i)
}

// Decode decode an object from slice og byte
func (j *JSONCodec) Decode(data []byte, i interface{}) error {
	d := json.NewDecoder(bytes.NewBuffer(data))
	d.UseNumber()
	return d.Decode(i)
}

// PBCode protobuf
type PBCodec struct{}

// Encode encode an object into slice of bytes
func (p *PBCodec) Encode(i interface{}) ([]byte, error) {
	if m, ok := i.(proto.Marshaler); ok {
		return m.Marshal()
	}
	if m, ok := i.(pb.Message); ok {
		return pb.Marshal(m)
	}
	return nil, fmt.Errorf("%T is not a proto.Marshaler or pb.Message", i)
}

// Decode decode an object from slice og byte
func (p *PBCodec) Decode(data []byte, i interface{}) error {
	if m, ok := i.(proto.Unmarshaler); ok {
		return m.Unmarshal(data)
	}

	if m, ok := i.(pb.Message); ok {
		return pb.Unmarshal(data, m)
	}
	return fmt.Errorf("%T is not a proto.Marshaler or pb.Message", i)
}

// MessagePackCodec uses messagepack marshaler and unmarshaler.
type MessagePackCodec struct{}

// Encode encode an object into slice of bytes
func (m *MessagePackCodec) Encode(i interface{}) ([]byte, error) {
	return msgpack.Marshal(i)
}

// Decode decode an object from slice og byte
func (m *MessagePackCodec) Decode(data []byte, i interface{}) error {
	return msgpack.Unmarshal(data, i)
}
