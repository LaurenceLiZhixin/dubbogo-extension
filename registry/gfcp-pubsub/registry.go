package gfcp_pubsub

import (
	"strconv"

	"dubbo.apache.org/dubbo-go/v3/common"
	"dubbo.apache.org/dubbo-go/v3/common/constant"
	"dubbo.apache.org/dubbo-go/v3/common/extension"
	"dubbo.apache.org/dubbo-go/v3/registry"
	perrors "github.com/pkg/errors"
)

var localIP = ""

func init() {
	extension.SetRegistry("gfcp-pubsub", newGFCPPubSubRegistry)
}

// newGFCPPubSubRegistry will create new instance
func newGFCPPubSubRegistry(url *common.URL) (registry.Registry, error) {
	return &GFCPPubSubRegistry{}, nil
}

type GFCPPubSubRegistry struct {
}

// Register will register the service @url to its polaris registry center.
func (pr *GFCPPubSubRegistry) Register(url *common.URL) error {
	return nil
}

// UnRegister returns nil if unregister successfully. If not, returns an error.
func (pr *GFCPPubSubRegistry) UnRegister(conf *common.URL) error {
	return nil
}

// Subscribe returns nil if subscribing registry successfully. If not returns an error.
func (pr *GFCPPubSubRegistry) Subscribe(url *common.URL, notifyListener registry.NotifyListener) error {
	role, _ := strconv.Atoi(url.GetParam(constant.RegistryRoleKey, ""))
	if role != common.CONSUMER {
		return nil
	}
	redisUrl, _ := common.NewURL("gfcp-pubsub://redis-server:6379")
	notifyListener.Notify(&registry.ServiceEvent{
		Service: redisUrl,
	})
	return nil
}

// UnSubscribe returns nil if unsubscribing registry successfully. If not returns an error.
func (pr *GFCPPubSubRegistry) UnSubscribe(url *common.URL, notifyListener registry.NotifyListener) error {
	// TODO wait polaris support it
	return perrors.New("UnSubscribe not support in polarisRegistry")
}

// GetURL returns polaris registry's url.
func (pr *GFCPPubSubRegistry) GetURL() *common.URL {
	return nil
}

// Destroy stop polaris registry.
func (pr *GFCPPubSubRegistry) Destroy() {
	return
}

// IsAvailable always return true when use polaris
func (pr *GFCPPubSubRegistry) IsAvailable() bool {
	return true
}
