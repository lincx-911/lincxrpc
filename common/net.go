package common

import (
	"errors"
	"log"
	"net"
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

func LocalIPV4() string {
	return localIPV4
}
