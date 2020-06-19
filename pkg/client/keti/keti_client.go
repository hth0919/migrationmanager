package keti

import (
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)


type KetiV1Interface interface {
	RESTClient() rest.Interface
	ControllerRevisionsGetter
	DaemonSetsGetter
	DeploymentsGetter
	ReplicaSetsGetter
	StatefulSetsGetter
	PodsGetter
	PodTemplatesGetter
}

type KetiV1Client struct {
	restClient rest.Interface
}

func NewForConfig(c *rest.Config) (*KetiV1Client, error) {
	config := *c
	config.ContentConfig.GroupVersion = &schema.GroupVersion{Group: ketiv1.GroupName, Version: ketiv1.GroupVersion}
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}

	return &KetiV1Client{restClient: client}, nil
}

func (c *KetiV1Client) RESTClient() rest.Interface {
	if c == nil {
		return nil
	}
	return c.restClient
}

func (c *KetiV1Client) ControllerRevisions(namespace string) ControllerRevisionInterface {
	return newControllerRevisions(c, namespace)
}

func (c *KetiV1Client) DaemonSets(namespace string) DaemonSetInterface {
	return newDaemonSets(c, namespace)
}

func (c *KetiV1Client) Deployments(namespace string) DeploymentInterface {
	return newDeployments(c, namespace)
}

func (c *KetiV1Client) ReplicaSets(namespace string) ReplicaSetInterface {
	return newReplicaSets(c, namespace)
}

func (c *KetiV1Client) StatefulSets(namespace string) StatefulSetInterface {
	return newStatefulSets(c, namespace)
}

func (c *KetiV1Client) Pods(namespace string) PodInterface {
	return newPods(c, namespace)
}

func (c *KetiV1Client) PodTemplates(namespace string) PodTemplateInterface {
	return newPodTemplates(c, namespace)
}

