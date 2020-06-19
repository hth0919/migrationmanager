package pod

import (
	"context"
	"encoding/json"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	keticlient "github.com/hth0919/migrationmanager/pkg/client/keti"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/klog"
	"math/rand"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_pod")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Pod Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	client, err :=keticlient.NewForConfig(mgr.GetConfig())
	if err != nil {
		klog.Errorln(err)
	}

	return &ReconcilePod{client: mgr.GetClient(), scheme: mgr.GetScheme(), keticlient: client}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("pod-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource Pod
	err = c.Watch(&source.Kind{Type: &ketiv1.Pod{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner Pod
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &ketiv1.Pod{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcilePod implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcilePod{}

// ReconcilePod reconciles a Pod object
type ReconcilePod struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	keticlient keticlient.KetiV1Interface
}

// Reconcile reads that state of the cluster for a Pod object and makes changes based on the state read
// and what is in the Pod.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcilePod) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling Pod")

	// Fetch the Pod instance
	instance := &ketiv1.Pod{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	// Define a new Pod object
	pod := newPodForCR(instance)

	Patchbytes, err := json.Marshal(pod)
	klog.Infoln("before : ", instance)
	result , err := r.keticlient.Pods(request.Namespace).Patch(pod.Name,"application/merge-patch+json", Patchbytes)
	if err != nil {
		klog.Errorln(err)
	}
	klog.Infoln("after : ", result)
	klog.Infoln("Create Migration Pod ", result.Name)


	return reconcile.Result{}, nil
}

// newPodForCR returns a busybox pod with the same name/namespace as the cr
func newPodForCR(cr *ketiv1.Pod) *ketiv1.Pod {
	cr = podSpecInit(cr)
	cr = podObjectMetaInit(cr)
	cr = podTypeMetaInit(cr)
	return cr
}


func podSpecInit(cr *ketiv1.Pod) *ketiv1.Pod{
	s := cr.Spec
	if s.RestartPolicy == "" {
		s.RestartPolicy = "Always"
	}
	if s.TerminationGracePeriodSeconds == nil {
		t := new(int64)
		*t = int64(30)
		s.TerminationGracePeriodSeconds = t
	}
	if s.DNSPolicy == "" {
		s.DNSPolicy = "ClusterFirst"
	}
	if s.EnableServiceLinks == nil {
		e := new(bool)
		*e = true
		s.EnableServiceLinks = e
	}
	if s.NodeName == "" {
		if s.Affinity == nil {
			rand.Seed(time.Now().UnixNano())
			config, err := rest.InClusterConfig()
			if err != nil {
				klog.Errorln(err.Error())
			}
			// creates the clientset
			clientset, err := kubernetes.NewForConfig(config)
			if err != nil {
				klog.Errorln(err.Error())
			}
			nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
			if err != nil {
				klog.Errorln(err.Error())
			}
			s.NodeName = nodes.Items[rand.Intn(len(nodes.Items))].Name
			s.NodeName = "cloudedge2-worker2"
		}else {
			s.NodeName = s.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchFields[0].Values[0]
		}

	}
	//추후 삭제 예정
	//s.NodeName = "cloudedge2-worker2"
	cr.Spec = s
	return cr
}

func podTypeMetaInit(cr *ketiv1.Pod) *ketiv1.Pod{
	tm := cr.TypeMeta
	tm.APIVersion = "keti.migration/v1"
	tm.Kind = "Pod"
	cr.TypeMeta = tm
	return cr
}

func podObjectMetaInit(cr *ketiv1.Pod) *ketiv1.Pod{
	if cr.Labels["keti.checkpoint.type"] == "" {
		cr.Labels["keti.checkpoint.type"] = "Pod"
		cr.Labels["keti.checkpoint.name"] = cr.Name
	}
	cr.Labels["app"]= cr.Name
	return cr
}