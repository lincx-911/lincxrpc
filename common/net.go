package common

import (
	"errors"
	"log"
	"math"
	"net"
	"net/http"
	"strings"
)

var localIPV4 string

type IpType byte
const(
	IPV4 IpType = iota
	IPV6
)

func init() {
	addr,err:=ExternalIPV4()
	if err!=nil{
		log.Fatalf("check net interface error:%v\n", err.Error())
	}
	localIPV4 = addr
}

// ExternalIPV4 获取本机的ipv4地址
func ExternalIPV4()(string,error){
	return getExternalIp(IPV4)
}
// ExternalIPV6 获取本机的ipv6地址
func ExternalIPV6()(string,error){
	return getExternalIp(IPV6)
}
// getExternalIP 根据类型获取地址
func getExternalIp(t IpType)(string,error){
	ifaces ,err := net.Interfaces()
	if err!=nil{
		return "",err
	}
	for _,iface:=range ifaces{
		if iface.Flags&net.FlagUp==0{
			continue //interface down
		}
		if iface.Flags&net.FlagLoopback!=0{
			continue //loopback interface
		}
		addrs,err:=iface.Addrs()
		if err!=nil{
			return "",err
		}
		for _,addr := range addrs{
			var ip net.IP
			switch v := addr.(type){
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip==nil || ip.IsLoopback(){
				continue
			}
			if t==IPV4{
				ip = ip.To4()
			}else {
				ip = ip.To16()
			}
			if ip==nil{
				continue // not an ip address
			}
			return ip.String(),nil
		}
	}
	return "",errors.New("not connect to the network")
}

// LocalIPV4 获取主机ipv4地址
func LocalIPV4() string {
	return localIPV4
}

// ClientIP 尽最大努力实现获取客户端 IP 的算法。
// 解析 X-Real-IP 和 X-Forwarded-For 以便于反向代理（nginx 或 haproxy）可以正常工作。
func GetClientIP(r *http.Request)string{
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	ip := strings.TrimSpace(strings.Split(xForwardedFor,",")[0])
	if ip != ""{
		return ip
	}
	ip = strings.TrimSpace(r.Header.Get("X-Real-Ip"))
	if ip != ""{
		return ip
	}
	if ip,_,err := net.SplitHostPort(strings.TrimSpace(r.RemoteAddr));err==nil{
		return ip
	}
	return ""
}

// ClientPublicIP 尽最大努力实现获取客户端公网 IP 的算法。
// 解析 X-Real-IP 和 X-Forwarded-For 以便于反向代理（nginx 或 haproxy）可以正常工作。
func GetPublicClientIP(r *http.Request)string{
	var ip string
	for _,ip = range strings.Split(r.Header.Get("X-Forwarded-For"), ","){
		if ip = strings.TrimSpace(ip); ip != "" && !HasLocalIPAddr(ip) {
			return ip
		}
	}
	if ip = strings.TrimSpace(r.Header.Get("X-Real-Ip")); ip != "" && !HasLocalIPAddr(ip) {
		return ip
	}
	if ip = RemoteIP(r); !HasLocalIPAddr(ip) {
		return ip
	}
	return ""
}

// RemoteIP 通过 RemoteAddr 获取 IP 地址， 只是一个快速解析方法。
func RemoteIP(r *http.Request) string {
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	return ip
}

// HasLocalIPAddr 检测 IP 地址字符串是否是内网地址
func HasLocalIPAddr(ip string) bool {
	return HasLocalIP(net.ParseIP(ip))
}

// HasLocalIP 检测 IP 地址是否是内网地址
func HasLocalIP(ip net.IP)bool{
	if ip.IsLoopback(){
		return true
	}

	ip4 := ip.To4()
	if ip4==nil{
		return false
	}
	return ip4[0]==10 || //10.0.0.0/8
	(ip4[0]==172 && ip4[1]>=16&&ip4[1]<=31) ||// 172.16.0.0/12
	(ip4[0]==169 && ip4[1]==254)|| // 169.254.0.0/16
	(ip4[0]==192&&ip4[1]==168)// 192.168.0.0/16
}

// Long2IPString 把数值转为ip字符串
func Long2IPString(i uint) (string, error) {
	if i > math.MaxUint32 {
		return "", errors.New("beyond the scope of ipv4")
	}

	ip := make(net.IP, net.IPv4len)
	ip[0] = byte(i >> 24)
	ip[1] = byte(i >> 16)
	ip[2] = byte(i >> 8)
	ip[3] = byte(i)

	return ip.String(), nil
}

// IP2Long 把net.IP转为数值
func IP2Long(ip net.IP) (uint, error) {
	b := ip.To4()
	if b == nil {
		return 0, errors.New("invalid ipv4 format")
	}

	return uint(b[3]) | uint(b[2])<<8 | uint(b[1])<<16 | uint(b[0])<<24, nil
}

// Long2IP 把数值转为net.IP
func Long2IP(i uint) (net.IP, error) {
	if i > math.MaxUint32 {
		return nil, errors.New("beyond the scope of ipv4")
	}

	ip := make(net.IP, net.IPv4len)
	ip[0] = byte(i >> 24)
	ip[1] = byte(i >> 16)
	ip[2] = byte(i >> 8)
	ip[3] = byte(i)

	return ip, nil
}