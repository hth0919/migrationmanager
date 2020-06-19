package keti


import (
	"time"

	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	scheme "k8s.io/client-go/kubernetes/scheme"
	rest "k8s.io/client-go/rest"
)

// PodTemplatesGetter has a method to return a PodTemplateInterface.
// A group's client should implement this interface.
type PodTemplatesGetter interface {
	PodTemplates(namespace string) PodTemplateInterface
}

// PodTemplateInterface has methods to work with PodTemplate resources.
type PodTemplateInterface interface {
	Create(*ketiv1.PodTemplate) (*ketiv1.PodTemplate, error)
	Update(*ketiv1.PodTemplate) (*ketiv1.PodTemplate, error)
	Delete(name string, options *metav1.DeleteOptions) error
	DeleteCollection(options *metav1.DeleteOptions, listOptions metav1.ListOptions) error
	Get(name string, options metav1.GetOptions) (*ketiv1.PodTemplate, error)
	List(opts metav1.ListOptions) (*ketiv1.PodTemplateList, error)
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *ketiv1.PodTemplate, err error)
}

// podTemplates implements PodTemplateInterface
type podTemplates struct {
	client rest.Interface
	ns     string
}

// newPodTemplates returns a PodTemplates
func newPodTemplates(c *KetiV1Client, namespace string) *podTemplates {
	return &podTemplates{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the podTemplate, and returns the corresponding podTemplate object, and an error if there is any.
func (c *podTemplates) Get(name string, options metav1.GetOptions) (result *ketiv1.PodTemplate, err error) {
	result = &ketiv1.PodTemplate{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("podtemplates").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of PodTemplates that match those selectors.
func (c *podTemplates) List(opts metav1.ListOptions) (result *ketiv1.PodTemplateList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &ketiv1.PodTemplateList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("podtemplates").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested podTemplates.
func (c *podTemplates) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("podtemplates").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch()
}

// Create takes the representation of a podTemplate and creates it.  Returns the server's representation of the podTemplate, and an error, if there is any.
func (c *podTemplates) Create(podTemplate *ketiv1.PodTemplate) (result *ketiv1.PodTemplate, err error) {
	result = &ketiv1.PodTemplate{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("podtemplates").
		Body(podTemplate).
		Do().
		Into(result)
	return
}

// Update takes the representation of a podTemplate and updates it. Returns the server's representation of the podTemplate, and an error, if there is any.
func (c *podTemplates) Update(podTemplate *ketiv1.PodTemplate) (result *ketiv1.PodTemplate, err error) {
	result = &ketiv1.PodTemplate{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("podtemplates").
		Name(podTemplate.Name).
		Body(podTemplate).
		Do().
		Into(result)
	return
}

// Delete takes name of the podTemplate and deletes it. Returns an error if one occurs.
func (c *podTemplates) Delete(name string, options *metav1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("podtemplates").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *podTemplates) DeleteCollection(options *metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	var timeout time.Duration
	if listOptions.TimeoutSeconds != nil {
		timeout = time.Duration(*listOptions.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("podtemplates").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Timeout(timeout).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched podTemplate.
func (c *podTemplates) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *ketiv1.PodTemplate, err error) {
	result = &ketiv1.PodTemplate{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("podtemplates").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}

