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

// PodTemplateInformer provides access to a shared informer and lister for
// PodTemplates.
type PodTemplateInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() lister.PodTemplateLister
}

type podTemplateInformer struct {
	factory          internalinterfaces.SharedInformerFactory
	tweakListOptions internalinterfaces.TweakListOptionsFunc
	namespace        string
}

// NewPodTemplateInformer constructs a new informer for PodTemplate type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewPodTemplateInformer(client keticlient.KetiV1Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers) cache.SharedIndexInformer {
	return NewFilteredPodTemplateInformer(client, namespace, resyncPeriod, indexers, nil)
}

// NewFilteredPodTemplateInformer constructs a new informer for PodTemplate type.
// Always prefer using an informer factory to get a shared informer instead of getting an independent
// one. This reduces memory footprint and number of connections to the server.
func NewFilteredPodTemplateInformer(client keticlient.KetiV1Interface, namespace string, resyncPeriod time.Duration, indexers cache.Indexers, tweakListOptions internalinterfaces.TweakListOptionsFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.PodTemplates(namespace).List(options)
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				if tweakListOptions != nil {
					tweakListOptions(&options)
				}
				return client.PodTemplates(namespace).Watch(options)
			},
		},
		&ketiv1.PodTemplate{},
		resyncPeriod,
		indexers,
	)
}

func (f *podTemplateInformer) defaultInformer(keticlient keticlient.KetiV1Interface, client kubernetes.Interface, resyncPeriod time.Duration) cache.SharedIndexInformer {
	return NewFilteredPodTemplateInformer(keticlient, f.namespace, resyncPeriod, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc}, f.tweakListOptions)
}

func (f *podTemplateInformer) Informer() cache.SharedIndexInformer {
	return f.factory.InformerFor(&ketiv1.PodTemplate{}, f.defaultInformer)
}

func (f *podTemplateInformer) Lister() lister.PodTemplateLister {
	return lister.NewPodTemplateLister(f.Informer().GetIndexer())
}