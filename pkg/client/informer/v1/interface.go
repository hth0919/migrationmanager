package v1

import (
	"github.com/hth0919/migrationmanager/pkg/client/informer/internalinterfaces"
	_ "k8s.io/client-go/informers/internalinterfaces"
)

type Interface interface {
	// ControllerRevisions returns a ControllerRevisionInformer.
	ControllerRevisions() ControllerRevisionInformer
	// DaemonSets returns a DaemonSetInformer.
	DaemonSets() DaemonSetInformer
	// Deployments returns a DeploymentInformer.
	Deployments() DeploymentInformer
	// ReplicaSets returns a ReplicaSetInformer.
	ReplicaSets() ReplicaSetInformer
	// StatefulSets returns a StatefulSetInformer.
	StatefulSets() StatefulSetInformer
	Pods() PodInformer
}

type version struct {
	factory          internalinterfaces.SharedInformerFactory
	namespace        string
	tweakListOptions internalinterfaces.TweakListOptionsFunc
}

// New returns a new Interface.
func New(f internalinterfaces.SharedInformerFactory, namespace string, tweakListOptions internalinterfaces.TweakListOptionsFunc) Interface {
	return &version{factory: f, namespace: namespace, tweakListOptions: tweakListOptions}
}

// ControllerRevisions returns a ControllerRevisionInformer.
func (v *version) ControllerRevisions() ControllerRevisionInformer {
	return &controllerRevisionInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// DaemonSets returns a DaemonSetInformer.
func (v *version) DaemonSets() DaemonSetInformer {
	return &daemonSetInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// Deployments returns a DeploymentInformer.
func (v *version) Deployments() DeploymentInformer {
	return &deploymentInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// ReplicaSets returns a ReplicaSetInformer.
func (v *version) ReplicaSets() ReplicaSetInformer {
	return &replicaSetInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

// StatefulSets returns a StatefulSetInformer.
func (v *version) StatefulSets() StatefulSetInformer {
	return &statefulSetInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}
}

func (v *version) Pods() PodInformer {
	return &podInformer{factory: v.factory, namespace: v.namespace, tweakListOptions: v.tweakListOptions}

}