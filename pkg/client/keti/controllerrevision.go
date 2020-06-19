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

// ControllerRevisionsGetter has a method to return a ControllerRevisionInterface.
// A group's client should implement this interface.
type ControllerRevisionsGetter interface {
	ControllerRevisions(namespace string) ControllerRevisionInterface
}

// ControllerRevisionInterface has methods to work with ControllerRevision resources.
type ControllerRevisionInterface interface {
	Create(*ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error)
	Update(*ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error)
	Delete(name string, options *metav1.DeleteOptions) error
	DeleteCollection(options *metav1.DeleteOptions, listOptions metav1.ListOptions) error
	Get(name string, options metav1.GetOptions) (*ketiv1.ControllerRevision, error)
	List(opts metav1.ListOptions) (*ketiv1.ControllerRevisionList, error)
	Watch(opts metav1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *ketiv1.ControllerRevision, err error)
}

// controllerRevisions implements ControllerRevisionInterface
type controllerRevisions struct {
	client rest.Interface
	ns     string
}

// newControllerRevisions returns a ControllerRevisions
func newControllerRevisions(c *KetiV1Client, namespace string) *controllerRevisions {
	return &controllerRevisions{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the controllerRevision, and returns the corresponding controllerRevision object, and an error if there is any.
func (c *controllerRevisions) Get(name string, options metav1.GetOptions) (result *ketiv1.ControllerRevision, err error) {
	result = &ketiv1.ControllerRevision{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("controllerrevisions").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result) 
	return
}

// List takes label and field selectors, and returns the list of ControllerRevisions that match those selectors.
func (c *controllerRevisions) List(opts metav1.ListOptions) (result *ketiv1.ControllerRevisionList, err error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	result = &ketiv1.ControllerRevisionList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("controllerrevisions").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested controllerRevisions.
func (c *controllerRevisions) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("controllerrevisions").
		VersionedParams(&opts, scheme.ParameterCodec).
		Timeout(timeout).
		Watch()
}

// Create takes the representation of a controllerRevision and creates it.  Returns the server's representation of the controllerRevision, and an error, if there is any.
func (c *controllerRevisions) Create(controllerRevision *ketiv1.ControllerRevision) (result *ketiv1.ControllerRevision, err error) {
	result = &ketiv1.ControllerRevision{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("controllerrevisions").
		Body(controllerRevision).
		Do().
		Into(result)
	return
}

// Update takes the representation of a controllerRevision and updates it. Returns the server's representation of the controllerRevision, and an error, if there is any.
func (c *controllerRevisions) Update(controllerRevision *ketiv1.ControllerRevision) (result *ketiv1.ControllerRevision, err error) {
	result = &ketiv1.ControllerRevision{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("controllerrevisions").
		Name(controllerRevision.Name).
		Body(controllerRevision).
		Do().
		Into(result)
	return
}

// Delete takes name of the controllerRevision and deletes it. Returns an error if one occurs.
func (c *controllerRevisions) Delete(name string, options *metav1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("controllerrevisions").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *controllerRevisions) DeleteCollection(options *metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	var timeout time.Duration
	if listOptions.TimeoutSeconds != nil {
		timeout = time.Duration(*listOptions.TimeoutSeconds) * time.Second
	}
	return c.client.Delete().
		Namespace(c.ns).
		Resource("controllerrevisions").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Timeout(timeout).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched controllerRevision.
func (c *controllerRevisions) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *ketiv1.ControllerRevision, err error) {
	result = &ketiv1.ControllerRevision{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("controllerrevisions").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}

