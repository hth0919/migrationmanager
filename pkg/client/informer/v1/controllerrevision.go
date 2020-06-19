package v1

import (
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	"github.com/hth0919/migrationmanager/pkg/client/informer/internalinterfaces"
	clientset "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	_ "k8s.io/client-go/informers/internalinterfaces"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"time"
)

type ControllerRevisionInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() lister.ControllerRevisionLister
}

type controllerRevisionInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewControllerRevisionInformer constructs a new inf for ControllerRevision type.
// Always prefer using an inf factory to get a shared inf instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewControllerRevisionInformer(client clientset.KetiV1Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredControllerRevisionInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredControllerRevisionInformer constructs a new inf for ControllerRevision type.
// Always prefer using an inf factory to get a shared inf instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredControllerRevisionInformer(client clientset.KetiV1Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.ControllerRevisions(namespace).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.ControllerRevisions(namespace).Watch(options)
			},
		},
		&ketiv1.ControllerRevision{},
		resyncPeriod,
		indexers,
	)
}

func (f *controllerRevisionInformer) defaultInformer(keticlient clientset.KetiV1Interface, client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredControllerRevisionInformer(keticlient, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *controllerRevisionInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&ketiv1.ControllerRevision{}, f.defaultInformer)
}

func (f *controllerRevisionInformer) Lister() lister.ControllerRevisionLister {
	return lister.NewControllerRevisionLister(f.Informer().GetIndexer())
}
