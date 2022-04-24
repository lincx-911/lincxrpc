package selector

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"time"

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

// HashSelector 一致哈希
type HashSelector struct {
	//排序的hash虚拟结点
	hashSortedNodes []uint32
	//虚拟结点对应的结点信息
	circle map[uint32]*registry.Provider
	//已绑定的结点
	nodes map[string]bool
	//map读写锁
	sync.RWMutex
	//虚拟结点数
	virtualNodeCount int
}

var HashSelectorInstance = HashSelector{}

func NewHashSelector()Selector{
	return &HashSelectorInstance
}

func(hs *HashSelector)Next(ctx context.Context, providers []registry.Provider, ServiceMethod string, arg interface{}, opt SelectOption) (rp registry.Provider, err error) {
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
	
	return 
}
func (c *HashSelector) hashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func (c *HashSelector) Add(node *registry.Provider, virtualNodeCount int) error {
	if node.ProviderKey == "" {
		return nil
	}
	c.Lock()
	defer c.Unlock()

	if c.circle == nil {
		c.circle = map[uint32]*registry.Provider{}
	}
	if c.nodes == nil {
		c.nodes = map[string]bool{}
	}

	if _, ok := c.nodes[node.ProviderKey]; ok {
		return errors.New("node already existed")
	}
	c.nodes[node.ProviderKey] = true
	//增加虚拟结点
	for i := 0; i < virtualNodeCount; i++ {
		virtualKey := c.hashKey(node.ProviderKey + strconv.Itoa(i))
		c.circle[virtualKey] = node
		c.hashSortedNodes = append(c.hashSortedNodes, virtualKey)
	}

	//虚拟结点排序
	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *HashSelector) GetNode(key string) *registry.Provider {
	c.RLock()
	defer c.RUnlock()

	hash := c.hashKey(key)
	i := c.getPosition(hash)

	return c.circle[c.hashSortedNodes[i]]
}

func (c *HashSelector) getPosition(hash uint32) int {
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool { return c.hashSortedNodes[i] >= hash })
	if i < len(c.hashSortedNodes) {
		if i == len(c.hashSortedNodes)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.hashSortedNodes) - 1
	}
}

