package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/lincx-911/lincxrpc/codec"
	"github.com/lincx-911/lincxrpc/common/metadata"
	"github.com/lincx-911/lincxrpc/protocol"
)

const (
	HEADER_SEQ            = "rpc-header-seq"            //序号, 用来唯一标识请求或响应
	HEADER_MESSAGE_TYPE   = "rpc-header-message_type"   //消息类型，用来标识一个消息是请求还是响应
	HEADER_COMPRESS_TYPE  = "rpc-header-compress_type"  //压缩类型，用来标识一个消息的压缩方式
	HEADER_SERIALIZE_TYPE = "rpc-header-serialize_type" //序列化类型，用来标识消息体采用的编码方式
	HEADER_STATUS_CODE    = "rpc-header-status_code"    //状态类型，用来标识一个请求是正常还是异常
	HEADER_SERVICE_NAME   = "rpc-header-service_name"   //服务名
	HEADER_METHOD_NAME    = "rpc-header-method_name"    //方法名
	HEADER_ERROR          = "rpc-header-error"          //方法调用发生的异常
	HEADER_META_DATA      = "rpc-header-meta_data"      //其他元数据

)

var(
	HttpSeverUrl string = "/lincxrpc/invoke" // http服务的路由,
	HttpPort int = 5080 // http服务监听的端口号
)

func (s *SGServer) StartGateway() {
	port := HttpPort
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	for err != nil && strings.Contains(err.Error(), "Only one usage of each socket address") {
		port++
		ln, err = net.Listen("tcp", ":"+strconv.Itoa(port))
	}
	if err != nil {
		log.Printf("error listening gateway: %s", err.Error())
		return
	}
	log.Printf("gateway listenning on " + strconv.Itoa(port))
	go func() {
		err := http.Serve(ln, s)
		if err != nil {
			log.Printf("error serving http %s", err.Error())
		}
	}()
}
// StartHttps 启动https
func (s *SGServer)StartHttps(port int,svrcrtPath,svrkeyPath,caCerPath string)error{
	pool := x509.NewCertPool()
	caCrt,err:=ioutil.ReadFile(caCerPath)
	if err!=nil{
		log.Printf("error serving https %s",err.Error())
		return err
	}
	pool.AppendCertsFromPEM(caCrt)
	server := &http.Server{
		Addr: ":"+strconv.Itoa(port),
		Handler: s,
		TLSConfig: &tls.Config{
			ClientCAs: pool,
			ClientAuth: tls.RequireAndVerifyClientCert,
		},
	}
	if err = server.ListenAndServeTLS(svrcrtPath,svrkeyPath);err!=nil{
		log.Printf("error serving https ListenAndServeTLS %v",err)
		return err
	}
	return nil
}

// ServeHTTP 处理请求
func (s *SGServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != HttpSeverUrl {
		w.WriteHeader(404)
		return
	}
	
	if r.Method != "POST" {
		w.WriteHeader(405)
		return
	}
	request := protocol.NewMessage(s.Option.ProtocolType)
	request, err := parseHeader(request, r)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	request, err = parseBody(request, r)
	if err != nil {
		w.WriteHeader(400)
		return
	}
	ctx := metadata.WithMeta(context.Background(), request.MetaData)
	response := request.Clone()
	response.MessageType = protocol.MessageTypeResponse
	response = s.process(ctx, request, response)
	s.writeHttpResponse(response, w, r)
}

func parseHeader(message *protocol.Message, request *http.Request) (*protocol.Message, error) {
	headerSeq := request.Header.Get(HEADER_SEQ)
	seq, err := strconv.ParseUint(headerSeq, 10, 64)
	if err != nil {
		return nil, err
	}
	message.Seq = seq

	headerMsgType := request.Header.Get(HEADER_MESSAGE_TYPE)
	msgType, err := protocol.ParseMessageType(headerMsgType)
	if err != nil {
		return nil, err
	}
	message.MessageType = msgType

	headerCompressType := request.Header.Get(HEADER_COMPRESS_TYPE)
	compressType, err := protocol.ParseCompressType(headerCompressType)
	if err != nil {
		return nil, err
	}
	message.CompressType = compressType

	headerSerializeType := request.Header.Get(HEADER_SERIALIZE_TYPE)
	serializeType, err := codec.ParseSerializeType(headerSerializeType)
	if err != nil {
		return nil, err
	}
	message.SerializeType = serializeType

	headerStatusCode := request.Header.Get(HEADER_STATUS_CODE)
	statusCode, err := protocol.ParseStatusCode(headerStatusCode)
	if err != nil {
		return nil, err
	}
	message.StatusCode = statusCode

	serviceName := request.Header.Get(HEADER_SERVICE_NAME)
	message.ServiceName = serviceName

	methodName := request.Header.Get(HEADER_METHOD_NAME)
	message.MethodName = methodName

	errorMsg := request.Header.Get(HEADER_ERROR)
	message.Error = errorMsg

	headerMeta := request.Header.Get(HEADER_META_DATA)
	meta := make(map[string]interface{})
	err = json.Unmarshal([]byte(headerMeta), &meta)
	if err != nil {
		return nil, err
	}
	message.MetaData = meta

	return message, nil
}



func parseBody(message *protocol.Message, request *http.Request) (*protocol.Message, error) {
	data, err := ioutil.ReadAll(request.Body)
	if err != nil {
		return nil, err
	}
	message.Data = data
	return message, nil
}

func (s *SGServer) writeHttpResponse(message *protocol.Message, rw http.ResponseWriter, r *http.Request) {
	header := rw.Header()
	header.Set(HEADER_SEQ, strconv.FormatUint(message.Seq, 10))
	header.Set(HEADER_MESSAGE_TYPE, message.MessageType.String())
	header.Set(HEADER_COMPRESS_TYPE, message.CompressType.String())
	header.Set(HEADER_SERIALIZE_TYPE, message.SerializeType.String())
	header.Set(HEADER_STATUS_CODE, message.StatusCode.String())
	header.Set(HEADER_SERVICE_NAME, message.ServiceName)
	header.Set(HEADER_METHOD_NAME, message.MethodName)
	header.Set(HEADER_ERROR, message.Error)
	metaDataJson, _ := json.Marshal(message.MetaData)
	header.Set(HEADER_META_DATA, string(metaDataJson))

	_, _ = rw.Write(message.Data)
}
