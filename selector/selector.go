package selector

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/lincx-911/lincxrpc/common/metadata"
	"github.com/lincx-911/lincxrpc/protocol"
	"github.com/lincx-911/lincxrpc/registry"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var ErrEmptyProviderList = errors.New("provider list is empty")

type Filter func(ctx context.Context, provider registry.Provider, serviceMethod string, arg interface{}) bool

type SelectOption struct {
	Filters []Filter
}

type Selector interface {
	Next(ctx context.Context, providers []registry.Provider, ServiceMethod string, arg interface{}, opt SelectOption) (rp registry.Provider, err error)
}

// RandomSelector 随机负载均衡
type RandomSelector struct {
}

var RandomSelectorInstance = RandomSelector{}

// DegradeProviderFilter 降级
func DegradeProviderFilter() Filter {
	return func(ctx context.Context, provider registry.Provider, serviceMethod string, arg interface{}) bool {
		_, degrade := provider.Meta[protocol.ProviderDegradeKey]
		return !degrade
	}
}

// TaggedProviderFilter 基于tags进行过滤
func TaggedProviderFilter(tags map[string]string) Filter {
	return func(ctx context.Context, provider registry.Provider, ServiceMethod string, arg interface{}) bool {
		if tags == nil {
			return true
		}

		if provider.Meta == nil {
			return true
		}	
		providerTags:= convertMapiface2Mapstring(provider.Meta["tags"])
		if len(providerTags)==0{
			return false
		}
		for k, v := range tags {
			if tag, ok := providerTags[k]; ok {
				if tag != v {
					return false
				}	
			} else {
				return false
			}
		}
		return true
	}
}

func (RandomSelector) Next(ctx context.Context, providers []registry.Provider, ServiceMethod string, arg interface{}, opt SelectOption) (rp registry.Provider, err error) {
	filters := combineFilter(opt.Filters)
	list := make([]registry.Provider, 0)
	for _, p := range providers {

		if filters(ctx, p, ServiceMethod, arg) {
			list = append(list, p)
		}
	}
	if len(list) == 0 {
		err = ErrEmptyProviderList
		return
	}
	idx := rand.Intn(len(list))
	rp = list[idx]
	return
}

func combineFilter(filters []Filter) Filter {
	return func(ctx context.Context, provider registry.Provider, serviceMethod string, arg interface{}) bool {
		for _, f := range filters {
			if !f(ctx, provider, serviceMethod, arg) {
				return false
			}
		}
		return true
	}
}

func convertMapiface2Mapstring(data interface{})map[string]string{
	var ok bool
	
	res, ok := data.(map[string]string)
	if ok {
		return res
	}
	res1 := make(map[string]string)
	datamap,ok := data.(map[string]interface{})
	if !ok || len(datamap)==0{
		return res1
	}
	for k,v := range datamap{
		strKey := fmt.Sprintf("%v", k)
        strValue := fmt.Sprintf("%v", v)
		res1[strKey] = strValue
	}
	return res1
}

func NewRandomSelector() Selector {
	return RandomSelectorInstance
}

// AppointedSelector 指定某个服务端
type AppointedSelector struct {

}

var AppointedSelectorInstance = AppointedSelector{}

func NewAppointedSelector()Selector{
	return AppointedSelectorInstance
}

func(AppointedSelector)Next(ctx context.Context, providers []registry.Provider, ServiceMethod string, arg interface{}, opt SelectOption) (rp registry.Provider, err error) {
	filters := combineFilter(opt.Filters)
	meta:=metadata.FromContext(ctx)
	if len(meta)==0{
		err = fmt.Errorf("meta is nil")
		return
	}
	server :=meta["server"].(string)
	if len(server)==0{
		err = fmt.Errorf("meta server is nil")
		return
	}
	for _, p := range providers {
		if p.Addr == server && filters(ctx, p, ServiceMethod, arg) {
			rp = p
			break
		}
	}
	
	return 
}