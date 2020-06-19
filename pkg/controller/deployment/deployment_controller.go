package deployment

import (
	"context"
	"fmt"
	clientset "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	"github.com/hth0919/migrationmanager/pkg/controller/util"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"reflect"
	"time"

	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubeclient "k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_deployment")
var controllerKind = ketiv1.SchemeGroupVersion.WithKind("Deployment")
/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Deployment Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	client, err :=clientset.NewForConfig(mgr.GetConfig())
	if err != nil {
		klog.Errorln(err)
	}
	kclient, err := kubeclient.NewForConfig(mgr.GetConfig())
	if err != nil {
		klog.Errorln(err)
	}
	dc := &ReconcileDeployment{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
		KetiClient: client,
		KubeClient: kclient,
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "deployment"),
	}
	dc.rsControl = util.RealRSControl{
		KubeClient: kclient,
	}
	dc.syncHandler = dc.Reconcile
	dc.enqueueDeployment = dc.enqueue

	return dc
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("deployment-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource Deployment
	err = c.Watch(&source.Kind{Type: &ketiv1.Deployment{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner Deployment
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &ketiv1.Deployment{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileDeployment implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileDeployment{}

// ReconcileDeployment reconciles a Deployment object
type ReconcileDeployment struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	rsControl util.RSControlInterface
	client client.Client
	scheme *runtime.Scheme
	KetiClient clientset.KetiV1Interface
	KubeClient kubeclient.Interface
	syncHandler func(request reconcile.Request) (reconcile.Result, error)
	enqueueDeployment func(deployment *ketiv1.Deployment)
	dLister lister.DeploymentLister
	rsLister lister.ReplicaSetLister
	podLister lister.PodLister
	dListerSynced cache.InformerSynced
	rsListerSynced cache.InformerSynced
	podListerSynced cache.InformerSynced
	queue workqueue.RateLimitingInterface
}

// Reconcile reads that state of the cluster for a Deployment object and makes changes based on the state read
// and what is in the Deployment.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileDeployment) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling Deployment")
	key := request.Namespace + "/" + request.Name
	startTime := time.Now()
	klog.V(4).Infof("Started syncing deployment %q (%v)", key, startTime)
	defer func() {
		klog.V(4).Infof("Finished syncing deployment %q (%v)", key, time.Since(startTime))
	}()

	// Fetch the Deployment instance
	deployment := &ketiv1.Deployment{}
	err := r.client.Get(context.TODO(), request.NamespacedName, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.V(2).Infof("Deployment %v has been deleted", key)
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	r.initDeployment(deployment)
	d := deployment.DeepCopy()
	everything := metav1.LabelSelector{}
	if reflect.DeepEqual(d.Spec.Selector, &everything) {
		if d.Status.ObservedGeneration < d.Generation {
			d.Status.ObservedGeneration = d.Generation
			dep, err := r.KetiClient.Deployments(d.Namespace).UpdateStatus(d)
			if err != nil {
				klog.Errorln(err)
			}
			klog.Infoln(dep.Name, "Update State Complete")
		}
		return reconcile.Result{}, nil
	}
	rsList, err := r.getReplicaSetsForDeployment(d)
	if err != nil {
		return reconcile.Result{}, err
	}
	// List all Pods owned by this Deployment, grouped by their ReplicaSet.
	// Current uses of the podMap are:
	//
	// * check if a Pod is labeled correctly with the pod-template-hash label.
	// * check that no old Pods are running in the middle of Recreate Deployments.
	podMap, err := r.getPodMapForDeployment(d, rsList)
	if err != nil {
		return reconcile.Result{}, err
	}

	if d.DeletionTimestamp != nil {
		return reconcile.Result{}, r.syncStatusOnly(d, rsList)
	}

	// Update deployment conditions with an Unknown condition when pausing/resuming
	// a deployment. In this way, we can be sure that we won't timeout when a user
	// resumes a Deployment with a set progressDeadlineSeconds.
	if err = r.checkPausedConditions(d); err != nil {
		return reconcile.Result{},err
	}

	if d.Spec.Paused {
		return reconcile.Result{}, r.sync(d, rsList)
	}

	// rollback is not re-entrant in case the underlying replica sets are updated with a new
	// revision so we should ensure that we won't proceed to update replica sets until we
	// make sure that the deployment has cleaned up its rollback spec in subsequent enqueues.
	if getRollbackTo(d) != nil {
		return reconcile.Result{}, r.rollback(d, rsList)
	}

	scalingEvent, err := r.isScalingEvent(d, rsList)
	if err != nil {
		return reconcile.Result{}, err
	}
	if scalingEvent {
		return reconcile.Result{}, r.sync(d, rsList)
	}

	switch d.Spec.Strategy.Type {
	case ketiv1.RecreateDeploymentStrategyType:
		return reconcile.Result{}, r.rolloutRecreate(d, rsList, podMap)
	case ketiv1.RollingUpdateDeploymentStrategyType:
		return reconcile.Result{}, r.rolloutRolling(d, rsList)
	}
	return reconcile.Result{}, fmt.Errorf("unexpected deployment strategy type: %s", d.Spec.Strategy.Type)
}
func (r *ReconcileDeployment) getReplicaSetsForDeployment(d *ketiv1.Deployment) ([]*ketiv1.ReplicaSet, error) {
	// List all ReplicaSets to find those we own but that no longer match our
	// selector. They will be orphaned by ClaimReplicaSets().
	replicasets, err := r.KetiClient.ReplicaSets(d.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	tempRS := replicasets.Items
	rsList := make([]*ketiv1.ReplicaSet, 0, len(tempRS))
	for _, rs := range tempRS {
		rsList = append(rsList, &rs)
	}
	klog.Infoln("rs length",len(rsList))

	deploymentSelector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, fmt.Errorf("deployment %s/%s has invalid label selector: %v", d.Namespace, d.Name, err)
	}
	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing ReplicaSets (see #42639).
	canAdoptFunc := util.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := r.KetiClient.Deployments(d.Namespace).Get(d.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != d.UID {
			return nil, fmt.Errorf("original Deployment %v/%v is gone: got uid %v, wanted %v", d.Namespace, d.Name, fresh.UID, d.UID)
		}
		return fresh, nil
	})
	cm := util.NewReplicaSetControllerRefManager(r.rsControl, d, deploymentSelector, controllerKind, canAdoptFunc)
	return cm.ClaimReplicaSets(rsList)
}

func (r *ReconcileDeployment) getPodMapForDeployment(d *ketiv1.Deployment, rsList []*ketiv1.ReplicaSet) (map[types.UID][]*ketiv1.Pod, error) {
	// Get all Pods that potentially belong to this Deployment.
	selector, err := metav1.LabelSelectorAsSelector(d.Spec.Selector)
	if err != nil {
		return nil, err
	}
	klog.Infoln(selector.String())
	Pods, err := r.KetiClient.Pods(d.Namespace).List(metav1.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		klog.Errorln(err)
		return nil, err
	}

	// Ignore inactive pods.
	tempPods := Pods.Items
	pods := make([]*ketiv1.Pod, 0, len(tempPods))
	for _, pod := range tempPods {
		pods = append(pods, &pod)
	}
	klog.Infoln("pod length", len(pods))
	// Group Pods by their controller (if it's in rsList).
	podMap := make(map[types.UID][]*ketiv1.Pod, len(rsList))
	for _, rs := range rsList {
		podMap[rs.UID] = []*ketiv1.Pod{}
	}
	for _, pod := range pods {
		// Do not ignore inactive Pods because Recreate Deployments need to verify that no
		// Pods from older versions are running before spinning up new Pods.
		controllerRef := metav1.GetControllerOf(pod)
		if controllerRef == nil {
			continue
		}
		// Only append if we care about this UID.
		if _, ok := podMap[controllerRef.UID]; ok {
			podMap[controllerRef.UID] = append(podMap[controllerRef.UID], pod)
		}
	}
	return podMap, nil
}

func (r *ReconcileDeployment) enqueueRateLimited(deployment *ketiv1.Deployment) {
	key, err := util.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", deployment, err))
		return
	}

	r.queue.AddRateLimited(key)
}

func (r *ReconcileDeployment) enqueueAfter(deployment *ketiv1.Deployment, after time.Duration) {
	key, err := util.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", deployment, err))
		return
	}

	r.queue.AddAfter(key, after)
}
func (r *ReconcileDeployment) enqueue(deployment *ketiv1.Deployment) {
	key, err := util.KeyFunc(deployment)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", deployment, err))
		return
	}

	r.queue.Add(key)
}

func(r *ReconcileDeployment) initDeployment(deployment *ketiv1.Deployment) {

	if deployment.Spec.RevisionHistoryLimit == nil {
		rv := new(int32)
		*rv = int32(10)
		deployment.Spec.RevisionHistoryLimit = rv
	}
	if deployment.Spec.ProgressDeadlineSeconds == nil {
		pdl := new(int32)
		*pdl = int32(600)
		deployment.Spec.ProgressDeadlineSeconds = pdl
	}
	if deployment.Spec.Strategy.Type == "" {

		deployment.Spec.Strategy.Type = ketiv1.RollingUpdateDeploymentStrategyType

	}
	if deployment.Spec.Strategy.RollingUpdate == nil {
		deployment.Spec.Strategy.RollingUpdate = new(ketiv1.RollingUpdateDeployment)
		deployment.Spec.Strategy.RollingUpdate.MaxSurge = new(intstr.IntOrString)
		deployment.Spec.Strategy.RollingUpdate.MaxSurge.Type = intstr.String
		deployment.Spec.Strategy.RollingUpdate.MaxSurge.StrVal = "25%"
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable = new(intstr.IntOrString)
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.Type = intstr.String
		deployment.Spec.Strategy.RollingUpdate.MaxUnavailable.StrVal = "25%"
	}



}
