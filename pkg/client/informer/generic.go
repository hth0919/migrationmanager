package informer

import (
	"fmt"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
)

// GenericInformer is type of SharedIndexInformer which will locate and delegate to other
// sharedInformers based on type
type GenericInformer interface {
	Informer() cache.SharedIndexInformer
	Lister() cache.GenericLister
}

type genericInformer struct {
	informer cache.SharedIndexInformer
	resource schema.GroupResource
}

// Informer returns the SharedIndexInformer.
func (f *genericInformer) Informer() cache.SharedIndexInformer {
	return f.informer
}

// Lister returns the GenericLister.
func (f *genericInformer) Lister() cache.GenericLister {
	return cache.NewGenericLister(f.Informer().GetIndexer(), f.resource)
}

// ForResource gives generic access to a shared informer of the matching type
// TODO extend this to unknown resources with a client pool
func (f *sharedInformerFactory) ForResource(resource schema.GroupVersionResource) (GenericInformer, error) {
	switch resource {

		// Group=keti.migration, Version=v1
	case ketiv1.SchemeGroupVersion.WithResource("controllerrevisions"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Keti().V1().ControllerRevisions().Informer()}, nil
	case ketiv1.SchemeGroupVersion.WithResource("daemonsets"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Keti().V1().DaemonSets().Informer()}, nil
	case ketiv1.SchemeGroupVersion.WithResource("deployments"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Keti().V1().Deployments().Informer()}, nil
	case ketiv1.SchemeGroupVersion.WithResource("replicasets"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Keti().V1().ReplicaSets().Informer()}, nil
	case ketiv1.SchemeGroupVersion.WithResource("statefulsets"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Keti().V1().StatefulSets().Informer()}, nil
	case ketiv1.SchemeGroupVersion.WithResource("pods"):
		return &genericInformer{resource: resource.GroupResource(), informer: f.Keti().V1().Pods().Informer()}, nil

	}

	return nil, fmt.Errorf("no informer found for %v", resource)
}

