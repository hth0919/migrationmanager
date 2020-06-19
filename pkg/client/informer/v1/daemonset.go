package v1

import (
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	"github.com/hth0919/migrationmanager/pkg/client/informer/internalinterfaces"
	keticlient "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"time"
)

// DaemonSetInformer provides access to a shared informer and lister for
// DaemonSets.
type DaemonSetInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() lister.DaemonSetLister
}

type daemonSetInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewDaemonSetInformer constructs a new informer for DaemonSet type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewDaemonSetInformer(client keticlient.KetiV1Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredDaemonSetInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredDaemonSetInformer constructs a new informer for DaemonSet type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredDaemonSetInformer(client keticlient.KetiV1Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.DaemonSets(namespace).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.DaemonSets(namespace).Watch(options)
			},
		},
		&ketiv1.DaemonSet{},
		resyncPeriod,
		indexers,
	)
}

func (f *daemonSetInformer) defaultInformer(keticlient keticlient.KetiV1Interface, client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredDaemonSetInformer(keticlient, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *daemonSetInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&ketiv1.DaemonSet{}, f.defaultInformer)
}

func (f *daemonSetInformer) Lister() lister.DaemonSetLister {
	return lister.NewDaemonSetLister(f.Informer().GetIndexer())
}
