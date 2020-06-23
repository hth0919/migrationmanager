package statefulset

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/hth0919/migrationmanager/pkg/client/informer"
	clientset "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	"github.com/hth0919/migrationmanager/pkg/controller/history"
	"github.com/hth0919/migrationmanager/pkg/controller/util"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubeinformer "k8s.io/client-go/informers"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
	kubectrlmgrconfig "k8s.io/kubernetes/pkg/controller/apis/config"
	kubectrlmgrconfigscheme "k8s.io/kubernetes/pkg/controller/apis/config/scheme"
	"math/rand"
	"time"

	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_statefulset")


/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new StatefulSet Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}
func NewDefaultComponentConfig() (kubectrlmgrconfig.KubeControllerManagerConfiguration, error) {
	versioned := kubectrlmgrconfigv1alpha1.KubeControllerManagerConfiguration{}
	kubectrlmgrconfigscheme.Scheme.Default(&versioned)

	internal := kubectrlmgrconfig.KubeControllerManagerConfiguration{}
	if err := kubectrlmgrconfigscheme.Scheme.Convert(&versioned, &internal, nil); err != nil {
		return internal, err
	}
	return internal, nil
}
func ResyncPeriod(c kubectrlmgrconfig.KubeControllerManagerConfiguration) func() time.Duration {
	return func() time.Duration {
		factor := rand.Float64() + 1
		return time.Duration(float64(c.Generic.MinResyncPeriod.Nanoseconds()) * factor)
	}
}
// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(klog.Infof)
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "keti-statefulset-controller"})

	client, err :=clientset.NewForConfig(mgr.GetConfig())
	if err != nil {
		klog.Errorln(err)
	}
	kclient, err := kubeclient.NewForConfig(mgr.GetConfig())
	if err != nil {
		klog.Errorln(err)
	}

	componentConfig, err := NewDefaultComponentConfig()
	if err != nil {
		klog.Errorln(err)
	}

	informerFactory := informer.NewSharedInformerFactory(kclient, client, ResyncPeriod(componentConfig)())
	kubeinformerFactory := kubeinformer.NewSharedInformerFactory(kclient, ResyncPeriod(componentConfig)())
	setInformer := informerFactory.Keti().V1().StatefulSets()
	podInformer := informerFactory.Keti().V1().Pods()
	pvcInformer := kubeinformerFactory.Core().V1().PersistentVolumeClaims()
	revInformer := informerFactory.Keti().V1().ControllerRevisions()
	ss := &ReconcileStatefulSet{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
		KetiClient: client,
		KubeClient: kclient,
		control: NewDefaultStatefulSetControl(
			NewRealStatefulPodControl(
				kclient,
				client,
				setInformer.Lister(),
				podInformer.Lister(),
				pvcInformer.Lister(),
				recorder),
			NewRealStatefulSetStatusUpdater(client, setInformer.Lister()),
			history.NewHistory(client, revInformer.Lister()),
			recorder,
		),
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "statefulset"),
		podControl:      util.RealPodControl{KetiClient: client, KubeClient: kclient, Recorder: recorder},

	}
	ss.podLister = podInformer.Lister()
	ss.podListerSynced = podInformer.Informer().HasSynced
	ss.setLister = setInformer.Lister()
	ss.setListerSynced = setInformer.Informer().HasSynced
	return ss
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("statefulset-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource StatefulSet
	err = c.Watch(&source.Kind{Type: &ketiv1.StatefulSet{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner StatefulSet
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &ketiv1.StatefulSet{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileStatefulSet implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileStatefulSet{}
var controllerKind = ketiv1.SchemeGroupVersion.WithKind("StatefulSet")

// ReconcileStatefulSet reconciles a StatefulSet object
type ReconcileStatefulSet struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	KetiClient clientset.KetiV1Interface
	KubeClient kubeclient.Interface
	scheme *runtime.Scheme
	control StatefulSetControlInterface
	// podControl is used for patching pods.
	podControl util.PodControlInterface
	// podLister is able to list/get pods from a shared inf's store
	podLister lister.PodLister
	// podListerSynced returns true if the pod shared inf has synced at least once
	podListerSynced cache.InformerSynced
	// setLister is able to list/get stateful sets from a shared inf's store
	setLister lister.StatefulSetLister
	// setListerSynced returns true if the stateful set shared inf has synced at least once
	setListerSynced cache.InformerSynced
	// pvcListerSynced returns true if the pvc shared inf has synced at least once
	pvcListerSynced cache.InformerSynced
	// revListerSynced returns true if the rev shared inf has synced at least once
	revListerSynced cache.InformerSynced
	// StatefulSets that need to be synced.
	queue workqueue.RateLimitingInterface
}

// Reconcile reads that state of the cluster for a StatefulSet object and makes changes based on the state read
// and what is in the StatefulSet.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileStatefulSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling StatefulSet")
	key := request.Namespace + "/" + request.Name
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing statefulset %q (%v)", key, time.Since(startTime))
	}()

	ss := &ketiv1.StatefulSet{}
	err := r.client.Get(context.TODO(), request.NamespacedName, ss)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Infof("%v %v has been deleted", ss.Kind, key)
			return reconcile.Result{}, nil
		}
		utilruntime.HandleError(fmt.Errorf("unable to retrieve StatefulSet %v from store: %v", key, err))
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	set := r.initStatefulSet(ss)


	selector, err := metav1.LabelSelectorAsSelector(set.Spec.Selector)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error converting StatefulSet %v selector: %v", key, err))
		// This is a non-transient error, so don't retry.
		return reconcile.Result{}, nil
	}

	if err := r.adoptOrphanRevisions(set); err != nil {
		return reconcile.Result{}, err
	}

	pods, err := r.getPodsForStatefulSet(set, selector)
	if err != nil {
		return reconcile.Result{}, err
	}

	return reconcile.Result{}, r.syncStatefulSet(set, pods)
}
func (r *ReconcileStatefulSet) initStatefulSet(set *ketiv1.StatefulSet) *ketiv1.StatefulSet{
	if set.Spec.PodManagementPolicy == "" {
		set.Spec.PodManagementPolicy = ketiv1.OrderedReadyPodManagement
	}
	if set.Spec.RevisionHistoryLimit == nil {
		rhl := new(int32)
		*rhl = 10
		set.Spec.RevisionHistoryLimit = rhl
	}
	if set.Spec.UpdateStrategy.Type == "" {
		set.Spec.UpdateStrategy.Type = ketiv1.RollingUpdateStatefulSetStrategyType
	}
	if set.Spec.UpdateStrategy.RollingUpdate == nil {
		russ := new(ketiv1.RollingUpdateStatefulSetStrategy)
		partition := new(int32)
		*partition = 0
		russ.Partition = partition
		set.Spec.UpdateStrategy.RollingUpdate = russ
	}
	for i:=0;i<len(set.Spec.VolumeClaimTemplates);i++{
		temp := r.initVolumeClaim(set.Spec.VolumeClaimTemplates[i])
		set.Spec.VolumeClaimTemplates[i] = temp
	}
	data, err := json.Marshal(set)
	if err != nil {
		klog.Errorln(err)
	}
	r.KetiClient.StatefulSets(set.Namespace).Patch(set.Name, "application/merge-patch+json", data)
	return set
}
func (r *ReconcileStatefulSet) initVolumeClaim(pvc corev1.PersistentVolumeClaim) corev1.PersistentVolumeClaim {
	pvc.APIVersion = "v1"
	pvc.Kind = "PersistentVolumeClaim"
	vm := new(corev1.PersistentVolumeMode)
	*vm = corev1.PersistentVolumeFilesystem
	pvc.Spec.VolumeMode = vm
	return pvc
}
// syncStatefulSet syncs a tuple of (statefulset, []*v1.Pod).
func (r *ReconcileStatefulSet) syncStatefulSet(set *ketiv1.StatefulSet, pods []*ketiv1.Pod) error {
	klog.V(4).Infof("Syncing StatefulSet %v/%v with %d pods", set.Namespace, set.Name, len(pods))
	// TODO: investigate where we mutate the set during the update as it is not obvious.
	if err := r.control.UpdateStatefulSet(set.DeepCopy(), pods); err != nil {
		return err
	}
	klog.V(4).Infof("Successfully synced StatefulSet %s/%s successful", set.Namespace, set.Name)
	return nil
}

// adoptOrphanRevisions adopts any orphaned ControllerRevisions matched by set's Selector.
func (r *ReconcileStatefulSet) adoptOrphanRevisions(set *ketiv1.StatefulSet) error {
	revisions, err := r.control.ListRevisions(set)
	if err != nil {
		return err
	}
	hasOrphans := false
	for i := range revisions {
		if metav1.GetControllerOf(revisions[i]) == nil {
			hasOrphans = true
			break
		}
	}
	if hasOrphans {
		fresh, err := r.KetiClient.StatefulSets(set.Namespace).Get(set.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if fresh.UID != set.UID {
			return fmt.Errorf("original StatefulSet %v/%v is gone: got uid %v, wanted %v", set.Namespace, set.Name, fresh.UID, set.UID)
		}
		return r.control.AdoptOrphanRevisions(set, revisions)
	}
	return nil
}

// getPodsForStatefulSet returns the Pods that a given StatefulSet should manage.
// It also reconciles ControllerRef by adopting/orphaning.
//
// NOTE: Returned Pods are pointers to objects from the cache.
//       If you need to modify one, you need to copy it first.
func (r *ReconcileStatefulSet) getPodsForStatefulSet(set *ketiv1.StatefulSet, selector labels.Selector) ([]*ketiv1.Pod, error) {
	// List all pods to include the pods that don't match the selector anymore but
	// has a ControllerRef pointing to this StatefulSet.
	Pods, err := r.KetiClient.Pods(set.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	// Ignore inactive pods.
	tempPods := Pods.Items
	pods := make([]*ketiv1.Pod, 0, len(tempPods))
	for _, pod := range tempPods {
		pods = append(pods, &pod)
	}

	filter := func(pod *ketiv1.Pod) bool {
		// Only claim if it matches our StatefulSet name. Otherwise release/ignore.
		return isMemberOf(set, pod)
	}

	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing Pods (see #42639).
	canAdoptFunc := util.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := r.KetiClient.StatefulSets(set.Namespace).Get(set.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != set.UID {
			return nil, fmt.Errorf("original StatefulSet %v/%v is gone: got uid %v, wanted %v", set.Namespace, set.Name, fresh.UID, set.UID)
		}
		return fresh, nil
	})

	cm := util.NewPodControllerRefManager(r.podControl, set, selector, controllerKind, canAdoptFunc)
	return cm.ClaimPods(pods, filter)
}