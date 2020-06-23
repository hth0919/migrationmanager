package daemonset

import (
	"context"
	"encoding/json"
	"fmt"
	clientset "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	"github.com/hth0919/migrationmanager/pkg/controller/util"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	kubeclient "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/algorithm/predicates"
	"k8s.io/utils/integer"
	"reflect"
	"sort"
	"sync"
	"time"

	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	daemonutil "github.com/hth0919/migrationmanager/pkg/controller/daemonset/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	schedulernodeinfo "k8s.io/kubernetes/pkg/scheduler/nodeinfo"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)
const (
	// BurstReplicas is a rate limiter for booting pods on a lot of pods.
	// The value of 250 is chosen b/c values that are too high can cause registry DoS issues.
	BurstReplicas = 250

	// StatusUpdateRetries limits the number of retries if sending a status update to API server fails.
	StatusUpdateRetries = 1

	// BackoffGCInterval is the time that has to pass before next iteration of backoff GC is run
	BackoffGCInterval = 1 * time.Minute
)
const (
	// SelectingAllReason is added to an event when a DaemonSet selects all Pods.
	SelectingAllReason = "SelectingAll"
	// FailedPlacementReason is added to an event when a DaemonSet can't schedule a Pod to a specified node.
	FailedPlacementReason = "FailedPlacement"
	// FailedDaemonPodReason is added to an event when the status of a Pod of a DaemonSet is 'Failed'.
	FailedDaemonPodReason = "FailedDaemonPod"
)

const (
	TypeLabel = "keti.checkpoint.type"
	NameLabel = "keti.checkpoint.name"
)

var log = logf.Log.WithName("controller_daemonset")
// controllerKind contains the schema.GroupVersionKind for this controller type.
var controllerKind = ketiv1.SchemeGroupVersion.WithKind("DaemonSet")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new DaemonSet Controller and adds it to the Manager. The Manager will set fields on the Controller
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
	ds := &ReconcileDaemonSet{
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
		KetiClient: client,
		KubeClient: kclient,
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "daemonset"),
		burstReplicas: BurstReplicas,
		expectations: util.NewControllerExpectations(),
		failedPodsBackoff: flowcontrol.NewBackOff(1*time.Second, 15*time.Minute),
	}
	ds.crControl = util.RealControllerRevisionControl{
		KubeClient: kclient,
		KetiClient: client,
	}
	ds.podControl = util.RealPodControl{
		KubeClient: kclient,
		KetiClient: client,
		Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "keti-daemonset-controller"}),
	}
	ds.syncHandler = ds.Reconcile
	ds.enqueueDaemonSet = ds.enqueue
	return ds
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("daemonset-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource DaemonSet
	err = c.Watch(&source.Kind{Type: &ketiv1.DaemonSet{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner DaemonSet
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &ketiv1.DaemonSet{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileDaemonSet implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileDaemonSet{}

// ReconcileDaemonSet reconciles a DaemonSet object
type ReconcileDaemonSet struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
	KetiClient clientset.KetiV1Interface
	KubeClient kubeclient.Interface
	podControl    util.PodControlInterface
	crControl     util.ControllerRevisionControlInterface
	burstReplicas int

	// To allow injection of syncDaemonSet for testing.
	syncHandler func(request reconcile.Request) (reconcile.Result, error)
	// used for unit testing
	enqueueDaemonSet func(ds *ketiv1.DaemonSet)
	// A TTLCache of pod creates/deletes each ds expects to see
	expectations util.ControllerExpectationsInterface
	// dsLister can list/get daemonsets from the shared inf's store
	dsLister lister.DaemonSetLister
	// dsStoreSynced returns true if the daemonset store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	dsStoreSynced cache.InformerSynced
	// historyLister get list/get history from the shared informers's store
	historyLister lister.ControllerRevisionLister
	// historyStoreSynced returns true if the history store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	historyStoreSynced cache.InformerSynced
	// podLister get list/get pods from the shared informers's store
	podLister lister.PodLister
	// podNodeIndex indexes pods by their nodeName
	podNodeIndex cache.Indexer
	// podStoreSynced returns true if the pod store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	podStoreSynced cache.InformerSynced
	// nodeLister can list/get nodes from the shared inf's store
	nodeLister corelisters.NodeLister
	// nodeStoreSynced returns true if the node store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	nodeStoreSynced cache.InformerSynced

	// DaemonSet keys that need to be synced.
	queue workqueue.RateLimitingInterface

	failedPodsBackoff *flowcontrol.Backoff
}
func (r *ReconcileDaemonSet) enqueue(ds *ketiv1.DaemonSet) {
	key, err := util.KeyFunc(ds)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", ds, err))
		return
	}

	// TODO: Handle overlapping controllers better. See comment in ReplicationManager.
	r.queue.Add(key)
}

func (r *ReconcileDaemonSet) enqueueRateLimited(ds *ketiv1.DaemonSet) {
	key, err := util.KeyFunc(ds)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", ds, err))
		return
	}

	r.queue.AddRateLimited(key)
}

func (r *ReconcileDaemonSet) enqueueDaemonSetAfter(obj interface{}, after time.Duration) {
	key, err := util.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %+v: %v", obj, err))
		return
	}

	// TODO: Handle overlapping controllers better. See comment in ReplicationManager.
	r.queue.AddAfter(key, after)
}
// Reconcile reads that state of the cluster for a DaemonSet object and makes changes based on the state read
// and what is in the DaemonSet.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileDaemonSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling DaemonSet")
	key := request.Namespace + "/" + request.Name
	startTime := time.Now()
	defer func() {
		klog.V(4).Infof("Finished syncing daemon set %q (%v)", key, time.Since(startTime))
	}()

	// Fetch the DaemonSet instance
	ds := &ketiv1.DaemonSet{}
	err := r.client.Get(context.TODO(), request.NamespacedName, ds)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			klog.V(3).Infof("daemon set has been deleted %v", key)
			r.expectations.DeleteExpectations(key)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	r.initDaemonset(ds)
	nodes, err := r.KubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("couldn't get list of nodes when syncing daemon set %#v: %v", ds, err)
	}
	tempnode := nodes.Items
	nodeList := make([]*corev1.Node, 0, len(tempnode))
	for _, rs := range tempnode {
		nodeList = append(nodeList, &rs)
	}
	klog.Infoln("node length",len(nodeList))

	everything := metav1.LabelSelector{}
	if reflect.DeepEqual(ds.Spec.Selector, &everything) {
		klog.Infoln(ds.Name, corev1.EventTypeWarning, SelectingAllReason, "This daemon set is selecting all pods. A non-empty selector is required.")
		return reconcile.Result{}, nil
	}

	// Don't process a daemon set until all its creations and deletions have been processed.
	// For example if daemon set foo asked for 3 new daemon pods in the previous call to manage,
	// then we do not want to call manage on foo until the daemon pods have been created.
	dsKey, err := util.KeyFunc(ds)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("couldn't get key for object %#v: %v", ds, err)
	}

	// If the DaemonSet is being deleted (either by foreground deletion or
	// orphan deletion), we cannot be sure if the DaemonSet history objects
	// it owned still exist -- those history objects can either be deleted
	// or orphaned. Garbage collector doesn't guarantee that it will delete
	// DaemonSet pods before deleting DaemonSet history objects, because
	// DaemonSet history doesn't own DaemonSet pods. We cannot reliably
	// calculate the status of a DaemonSet being deleted. Therefore, return
	// here without updating status for the DaemonSet being deleted.
	if ds.DeletionTimestamp != nil {
		return reconcile.Result{}, nil
	}

	// Construct histories of the DaemonSet, and get the hash of current history
	cur, old, err := r.constructHistory(ds)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to construct revisions of DaemonSet: %v", err)
	}
	hash := cur.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey]

	if !r.expectations.SatisfiedExpectations(dsKey) {
		// Only update status. Don't raise observedGeneration since controller didn't process object of that generation.
		return reconcile.Result{}, r.updateDaemonSetStatus(ds, nodeList, hash, false)
	}

	err = r.manage(ds, nodeList, hash)
	if err != nil {
		return reconcile.Result{}, err
	}

	// Process rolling updates if we're ready.
	if r.expectations.SatisfiedExpectations(dsKey) {
		switch ds.Spec.UpdateStrategy.Type {
		case ketiv1.OnDeleteDaemonSetStrategyType:
		case ketiv1.RollingUpdateDaemonSetStrategyType:
			err = r.rollingUpdate(ds, nodeList, hash)
		}
		if err != nil {
			return reconcile.Result{}, err
		}
	}

	err = r.cleanupHistory(ds, old)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to clean up revisions of DaemonSet: %v", err)
	}
	//r.setNodeName(ds, nodes)
	return reconcile.Result{}, r.updateDaemonSetStatus(ds, nodeList, hash, true)
}
func (r *ReconcileDaemonSet)setNodeName(ds *ketiv1.DaemonSet, nodelist *corev1.NodeList){
	pods , err := r.KetiClient.Pods(metav1.NamespaceAll).List(metav1.ListOptions{})
	if err != nil {
		klog.Errorln(err)
	}
	dspods := make([]*ketiv1.Pod,0,len(pods.Items))
	for _, pod := range pods.Items {
		if pod.Labels[TypeLabel] == "DaemonSet" && pod.Labels[NameLabel] == ds.Name{
			dspods = append(dspods, &pod)
		}
	}
	klog.Infoln("set nodeName to dsPods....", len(dspods))
	for i:=0;i<len(nodelist.Items);i++ {
		dspods[i].Spec.NodeName = nodelist.Items[i].Name
		klog.Infoln(nodelist.Items[i].Name)
		Patchbytes, err := json.Marshal(dspods[i])
		klog.Infoln("before : ", dspods[i].Spec.NodeName)
		result , err := r.KetiClient.Pods(dspods[i].Namespace).Patch(dspods[i].Name,"application/merge-patch+json", Patchbytes)
		if err != nil {
			klog.Errorln(err)
		}
		klog.Infoln("after : ", result.Spec.NodeName)
	}
}
// newPodForCR returns a busybox pod with the same name/namespace as the cr
func (r *ReconcileDaemonSet)initDaemonset(ds *ketiv1.DaemonSet){
	if ds.Spec.RevisionHistoryLimit == nil {
		rv := new(int32)
		*rv = int32(10)
		ds.Spec.RevisionHistoryLimit = rv
	}
	if ds.Spec.UpdateStrategy.Type == "" {

		ds.Spec.UpdateStrategy.Type = ketiv1.RollingUpdateDaemonSetStrategyType

	}
	if ds.Spec.UpdateStrategy.RollingUpdate == nil {
		ds.Spec.UpdateStrategy.RollingUpdate = new(ketiv1.RollingUpdateDaemonSet)
		ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable = new(intstr.IntOrString)
		ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.Type = intstr.String
		ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable.StrVal = "25%"
	}
}
func (r *ReconcileDaemonSet) getNodesToDaemonPods(ds *ketiv1.DaemonSet) (map[string][]*ketiv1.Pod, error) {
	claimedPods, err := r.getDaemonPods(ds)
	if err != nil {
		return nil, err
	}
	// Group Pods by Node name.
	nodeToDaemonPods := make(map[string][]*ketiv1.Pod)
	for _, pod := range claimedPods {
		nodeName, err := daemonutil.GetTargetNodeName(pod)
		if err != nil {
			klog.Warningf("Failed to get target node name of Pod %v/%v in DaemonSet %v/%v",
				pod.Namespace, pod.Name, ds.Namespace, ds.Name)
			continue
		}

		nodeToDaemonPods[nodeName] = append(nodeToDaemonPods[nodeName], pod)
	}

	return nodeToDaemonPods, nil
}

// getDaemonPods returns daemon pods owned by the given ds.
// This also reconciles ControllerRef by adopting/orphaning.
// Note that returned Pods are pointers to objects in the cache.
// If you want to modify one, you need to deep-copy it first.
func (r *ReconcileDaemonSet) getDaemonPods(ds *ketiv1.DaemonSet) ([]*ketiv1.Pod, error) {
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return nil, err
	}

	// List all pods to include those that don't match the selector anymore but
	// have a ControllerRef pointing to this controller.
	allpods, err := r.KetiClient.Pods(ds.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// Ignore inactive pods.
	tempPods := allpods.Items
	pods := make([]*ketiv1.Pod, 0, len(tempPods))
	for _, pod := range tempPods {
		pods = append(pods, &pod)
	}
	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing Pods (see #42639).
	dsNotDeleted := util.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := r.KetiClient.DaemonSets(ds.Namespace).Get(ds.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != ds.UID {
			return nil, fmt.Errorf("original DaemonSet %v/%v is gone: got uid %v, wanted %v", ds.Namespace, ds.Name, fresh.UID, ds.UID)
		}
		return fresh, nil
	})

	// Use ControllerRefManager to adopt/orphan as needed.
	cm := util.NewPodControllerRefManager(r.podControl, ds, selector, controllerKind, dsNotDeleted)
	return cm.ClaimPods(pods)
}

// nodeShouldRunDaemonPod checks a set of preconditions against a (node,daemonset) and returns a
// summary. Returned booleans are:
// * wantToRun:
//     Returns true when a user would expect a pod to run on this node and ignores conditions
//     such as DiskPressure or insufficient resource that would cause a daemonset pod not to schedule.
//     This is primarily used to populate daemonset status.
// * shouldSchedule:
//     Returns true when a daemonset should be scheduled to a node if a daemonset pod is not already
//     running on that node.
// * shouldContinueRunning:
//     Returns true when a daemonset should continue running on a node if a daemonset pod is already
//     running on that node.
func (r *ReconcileDaemonSet) nodeShouldRunDaemonPod(node *corev1.Node, ds *ketiv1.DaemonSet) (wantToRun, shouldSchedule, shouldContinueRunning bool, err error) {
	newPod := NewPod(ds, node.Name)

	// Because these bools require an && of all their required conditions, we start
	// with all bools set to true and set a bool to false if a condition is not met.
	// A bool should probably not be set to true after this line.
	wantToRun, shouldSchedule, shouldContinueRunning = true, true, true
	// If the daemon set specifies a node name, check that it matches with node.Name.
	if !(ds.Spec.Template.Spec.NodeName == "" || ds.Spec.Template.Spec.NodeName == node.Name) {
		return false, false, false, nil
	}

	reasons, nodeInfo, err := r.simulate(newPod, node, ds)
	if err != nil {
		klog.Warningf("DaemonSet Predicates failed on node %s for ds '%s/%s' due to unexpected error: %v", node.Name, ds.ObjectMeta.Namespace, ds.ObjectMeta.Name, err)
		return false, false, false, err
	}

	// TODO(k82cn): When 'ScheduleDaemonSetPods' upgrade to beta or GA, remove unnecessary check on failure reason,
	//              e.g. InsufficientResourceError; and simplify "wantToRun, shouldSchedule, shouldContinueRunning"
	//              into one result, e.g. selectedNode.
	for _, r := range reasons {
		klog.V(4).Infof("DaemonSet Predicates failed on node %s for ds '%s/%s' for reason: %v", node.Name, ds.ObjectMeta.Namespace, ds.ObjectMeta.Name, r.GetReason())
		switch reason := r.(type) {
		case *predicates.PredicateFailureError:
			// we try to partition predicates into two partitions here: intentional on the part of the operator and not.
			switch reason {
			// intentional
			case
				predicates.ErrNodeSelectorNotMatch,
				predicates.ErrPodNotMatchHostName,
				predicates.ErrNodeLabelPresenceViolated,
				// this one is probably intentional since it's a workaround for not having
				// pod hard anti affinity.
				predicates.ErrPodNotFitsHostPorts:
				return false, false, false, nil
			case predicates.ErrTaintsTolerationsNotMatch:
				// DaemonSet is expected to respect taints and tolerations
				fitsNoExecute, _, err := predicates.PodToleratesNodeNoExecuteTaints((*corev1.Pod)(newPod), nil, nodeInfo)
				if err != nil {
					return false, false, false, err
				}
				if !fitsNoExecute {
					return false, false, false, nil
				}
				wantToRun, shouldSchedule = false, false
			// unexpected
			case
				predicates.ErrPodAffinityNotMatch,
				predicates.ErrServiceAffinityViolated:
				klog.Warningf("unexpected predicate failure reason: %s", reason.GetReason())
				return false, false, false, fmt.Errorf("unexpected reason: DaemonSet Predicates should not return reason %s", reason.GetReason())
			default:
				klog.V(4).Infof("unknown predicate failure reason: %s", reason.GetReason())
				wantToRun, shouldSchedule, shouldContinueRunning = false, false, false
				klog.Infoln(ds.Name, corev1.EventTypeWarning, FailedPlacementReason, "failed to place pod on %q: %s", node.ObjectMeta.Name, reason.GetReason())
			}
		}
	}
	return
}

func (r *ReconcileDaemonSet) simulate(newPod *ketiv1.Pod, node *corev1.Node, ds *ketiv1.DaemonSet) ([]predicates.PredicateFailureReason, *schedulernodeinfo.NodeInfo, error) {
	objects, err := r.indexByPodNodeName(node.Name)
	if err != nil {
		return nil, nil, err
	}

	nodeInfo := schedulernodeinfo.NewNodeInfo()
	nodeInfo.SetNode(node)

	for _, pod := range objects {
		if metav1.IsControlledBy((*corev1.Pod)(pod), ds) {
			continue
		}
		nodeInfo.AddPod((*corev1.Pod)(pod))
	}

	_, reasons, err := Predicates(newPod, nodeInfo)
	return reasons, nodeInfo, err
}

func (r *ReconcileDaemonSet)indexByPodNodeName(indexedValue string) ([]*ketiv1.Pod, error) {
	pods, err := r.KetiClient.Pods(metav1.NamespaceAll).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	podlist := make([]*ketiv1.Pod,0,len(pods.Items))

	for _,pod := range pods.Items {
		if pod.Spec.NodeName == indexedValue {
			podlist = append(podlist, &pod)
		}
	}
	return podlist, nil
}
func (r *ReconcileDaemonSet) updateDaemonSetStatus(ds *ketiv1.DaemonSet, nodeList []*corev1.Node, hash string, updateObservedGen bool) error {
	klog.V(4).Infof("Updating daemon set status")
	nodeToDaemonPods, err := r.getNodesToDaemonPods(ds)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	var desiredNumberScheduled, currentNumberScheduled, numberMisscheduled, numberReady, updatedNumberScheduled, numberAvailable int
	for _, node := range nodeList {
		wantToRun, _, _, err := r.nodeShouldRunDaemonPod(node, ds)
		if err != nil {
			return err
		}

		scheduled := len(nodeToDaemonPods[node.Name]) > 0

		if wantToRun {
			desiredNumberScheduled++
			if scheduled {
				currentNumberScheduled++
				// Sort the daemon pods by creation time, so that the oldest is first.
				daemonPods, _ := nodeToDaemonPods[node.Name]
				sort.Sort(podByCreationTimestampAndPhase(daemonPods))
				pod := daemonPods[0]
				if podutil.IsPodReady((*corev1.Pod)(pod)) {
					numberReady++
					if podutil.IsPodAvailable((*corev1.Pod)(pod), ds.Spec.MinReadySeconds, metav1.Now()) {
						numberAvailable++
					}
				}
				// If the returned error is not nil we have a parse error.
				// The controller handles this via the hash.
				generation, err := daemonutil.GetTemplateGeneration(ds)
				if err != nil {
					generation = nil
				}
				if daemonutil.IsPodUpdated(pod, hash, generation) {
					updatedNumberScheduled++
				}
			}
		} else {
			if scheduled {
				numberMisscheduled++
			}
		}
	}
	numberUnavailable := desiredNumberScheduled - numberAvailable

	err = storeDaemonSetStatus(r.KetiClient.DaemonSets(ds.Namespace), ds, desiredNumberScheduled, currentNumberScheduled, numberMisscheduled, numberReady, updatedNumberScheduled, numberAvailable, numberUnavailable, updateObservedGen)
	if err != nil {
		return fmt.Errorf("error storing status for daemon set %#v: %v", ds, err)
	}

	// Resync the DaemonSet after MinReadySeconds as a last line of defense to guard against clock-skew.
	if ds.Spec.MinReadySeconds > 0 && numberReady != numberAvailable {
		r.enqueueDaemonSetAfter(ds, time.Duration(ds.Spec.MinReadySeconds)*time.Second)
	}
	return nil
}

// manage manages the scheduling and running of Pods of ds on nodes.
// After figuring out which nodes should run a Pod of ds but not yet running one and
// which nodes should not run a Pod of ds but currently running one, it calls function
// syncNodes with a list of pods to remove and a list of nodes to run a Pod of ds.
func (r *ReconcileDaemonSet) manage(ds *ketiv1.DaemonSet, nodeList []*corev1.Node, hash string) error {
	// Find out the pods which are created for the nodes by DaemonSet.
	nodeToDaemonPods, err := r.getNodesToDaemonPods(ds)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	// For each node, if the node is running the daemon pod but isn't supposed to, kill the daemon
	// pod. If the node is supposed to run the daemon pod, but isn't, create the daemon pod on the node.
	var nodesNeedingDaemonPods, podsToDelete []string
	for _, node := range nodeList {
		nodesNeedingDaemonPodsOnNode, podsToDeleteOnNode, err := r.podsShouldBeOnNode(
			node, nodeToDaemonPods, ds)

		if err != nil {
			continue
		}

		nodesNeedingDaemonPods = append(nodesNeedingDaemonPods, nodesNeedingDaemonPodsOnNode...)
		podsToDelete = append(podsToDelete, podsToDeleteOnNode...)
	}

	// Remove unscheduled pods assigned to not existing nodes when daemonset pods are scheduled by scheduler.
	// If node doesn't exist then pods are never scheduled and can't be deleted by PodGCController.
	podsToDelete = append(podsToDelete, getUnscheduledPodsWithoutNode(nodeList, nodeToDaemonPods)...)

	// Label new pods using the hash label value of the current history when creating them
	if err = r.syncNodes(ds, podsToDelete, nodesNeedingDaemonPods, hash); err != nil {
		return err
	}

	return nil
}
// podsShouldBeOnNode figures out the DaemonSet pods to be created and deleted on the given node:
//   - nodesNeedingDaemonPods: the pods need to start on the node
//   - podsToDelete: the Pods need to be deleted on the node
//   - err: unexpected error
func (r *ReconcileDaemonSet) podsShouldBeOnNode(
	node *corev1.Node,
	nodeToDaemonPods map[string][]*ketiv1.Pod,
	ds *ketiv1.DaemonSet,
) (nodesNeedingDaemonPods, podsToDelete []string, err error) {

	_, shouldSchedule, shouldContinueRunning, err := r.nodeShouldRunDaemonPod(node, ds)
	if err != nil {
		return
	}

	daemonPods, exists := nodeToDaemonPods[node.Name]

	switch {
	case shouldSchedule && !exists:
		// If daemon pod is supposed to be running on node, but isn't, create daemon pod.
		nodesNeedingDaemonPods = append(nodesNeedingDaemonPods, node.Name)
	case shouldContinueRunning:
		// If a daemon pod failed, delete it
		// If there's non-daemon pods left on this node, we will create it in the next sync loop
		var daemonPodsRunning []*ketiv1.Pod
		for _, pod := range daemonPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			if pod.Status.Phase == corev1.PodFailed {
				// This is a critical place where DS is often fighting with kubelet that rejects pods.
				// We need to avoid hot looping and backoff.
				backoffKey := failedPodsBackoffKey(ds, node.Name)

				now := r.failedPodsBackoff.Clock.Now()
				inBackoff := r.failedPodsBackoff.IsInBackOffSinceUpdate(backoffKey, now)
				if inBackoff {
					delay := r.failedPodsBackoff.Get(backoffKey)
					klog.V(4).Infof("Deleting failed pod %s/%s on node %s has been limited by backoff - %v remaining",
						pod.Namespace, pod.Name, node.Name, delay)
					r.enqueueDaemonSetAfter(ds, delay)
					continue
				}

				r.failedPodsBackoff.Next(backoffKey, now)

				msg := fmt.Sprintf("Found failed daemon pod %s/%s on node %s, will try to kill it", pod.Namespace, pod.Name, node.Name)
				klog.V(2).Infof(msg)
				// Emit an event so that it's discoverable to users.
				klog.Infoln(ds.Name, corev1.EventTypeWarning, FailedDaemonPodReason, msg)
				podsToDelete = append(podsToDelete, pod.Name)
			} else {
				daemonPodsRunning = append(daemonPodsRunning, pod)
			}
		}
		// If daemon pod is supposed to be running on node, but more than 1 daemon pod is running, delete the excess daemon pods.
		// Sort the daemon pods by creation time, so the oldest is preserved.
		if len(daemonPodsRunning) > 1 {
			sort.Sort(podByCreationTimestampAndPhase(daemonPodsRunning))
			for i := 1; i < len(daemonPodsRunning); i++ {
				podsToDelete = append(podsToDelete, daemonPodsRunning[i].Name)
			}
		}
	case !shouldContinueRunning && exists:
		// If daemon pod isn't supposed to run on node, but it is, delete all daemon pods on node.
		for _, pod := range daemonPods {
			if pod.DeletionTimestamp != nil {
				continue
			}
			podsToDelete = append(podsToDelete, pod.Name)
		}
	}

	return nodesNeedingDaemonPods, podsToDelete, nil
}
// syncNodes deletes given pods and creates new daemon set pods on the given nodes
// returns slice with errors if any
func (r *ReconcileDaemonSet) syncNodes(ds *ketiv1.DaemonSet, podsToDelete, nodesNeedingDaemonPods []string, hash string) error {
	nodes, err := r.KubeClient.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		klog.Errorln(err)
	}
	nodename := make([]string, 0, len(nodes.Items))
	for _, node := range nodes.Items{
		nodename = append(nodename, node.Name)
	}
	nodesNeedingDaemonPods = nodename
	// We need to set expectations before creating/deleting pods to avoid race conditions.
	dsKey, err := util.KeyFunc(ds)
	if err != nil {
		return fmt.Errorf("couldn't get key for object %#v: %v", ds, err)
	}

	createDiff := len(nodesNeedingDaemonPods)
	deleteDiff := len(podsToDelete)

	if createDiff > r.burstReplicas {
		createDiff = r.burstReplicas
	}
	if deleteDiff > r.burstReplicas {
		deleteDiff = r.burstReplicas
	}

	r.expectations.SetExpectations(dsKey, createDiff, deleteDiff)

	// error channel to communicate back failures.  make the buffer big enough to avoid any blocking
	errCh := make(chan error, createDiff+deleteDiff)

	klog.V(4).Infof("Nodes needing daemon pods for daemon set %s: %+v, creating %d", ds.Name, nodesNeedingDaemonPods, createDiff)
	createWait := sync.WaitGroup{}
	// If the returned error is not nil we have a parse error.
	// The controller handles this via the hash.
	generation, err := daemonutil.GetTemplateGeneration(ds)
	if err != nil {
		generation = nil
	}
	template := daemonutil.CreatePodTemplate(ds.Spec.Template, generation, hash)
	// Batch the pod creates. Batch sizes start at SlowStartInitialBatchSize
	// and double with each successful iteration in a kind of "slow start".
	// This handles attempts to start large numbers of pods that would
	// likely all fail with the same error. For example a project with a
	// low quota that attempts to create a large number of pods will be
	// prevented from spamming the API service with the pod create requests
	// after one of its pods fails.  Conveniently, this also prevents the
	// event spam that those failures would generate.
	batchSize := integer.IntMin(createDiff, util.SlowStartInitialBatchSize)
	for pos := 0; createDiff > pos; batchSize, pos = integer.IntMin(2*batchSize, createDiff-(pos+batchSize)), pos+batchSize {
		errorCount := len(errCh)
		createWait.Add(batchSize)
		for i := pos; i < pos+batchSize; i++ {
			go func(ix int) {
				defer createWait.Done()

				podTemplate := template.DeepCopy()
				// The pod's NodeAffinity will be updated to make sure the Pod is bound
				// to the target node by default scheduler. It is safe to do so because there
				// should be no conflicting node affinity with the target node.
				podTemplate.Spec.Affinity = daemonutil.ReplaceDaemonSetPodNodeNameNodeAffinity(
					podTemplate.Spec.Affinity, nodesNeedingDaemonPods[ix])

				err := r.podControl.CreatePodsWithControllerRef(ix, ds.Name, ds.Namespace, podTemplate, ds, metav1.NewControllerRef(ds, controllerKind))

				if err != nil {
					if errors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
						// If the namespace is being torn down, we can safely ignore
						// this error since all subsequent creations will fail.
						return
					}
				}
				if err != nil {
					klog.V(2).Infof("Failed creation, decrementing expectations for set %q/%q", ds.Namespace, ds.Name)
					r.expectations.CreationObserved(dsKey)
					errCh <- err
					utilruntime.HandleError(err)
				}
			}(i)
		}
		createWait.Wait()
		// any skipped pods that we never attempted to start shouldn't be expected.
		skippedPods := createDiff - (batchSize + pos)
		if errorCount < len(errCh) && skippedPods > 0 {
			klog.V(2).Infof("Slow-start failure. Skipping creation of %d pods, decrementing expectations for set %q/%q", skippedPods, ds.Namespace, ds.Name)
			r.expectations.LowerExpectations(dsKey, skippedPods, 0)
			// The skipped pods will be retried later. The next controller resync will
			// retry the slow start process.
			break
		}
	}

	klog.V(4).Infof("Pods to delete for daemon set %s: %+v, deleting %d", ds.Name, podsToDelete, deleteDiff)
	deleteWait := sync.WaitGroup{}
	deleteWait.Add(deleteDiff)
	for i := 0; i < deleteDiff; i++ {
		go func(ix int) {
			defer deleteWait.Done()
			if err := r.podControl.DeletePod(ds.Namespace, podsToDelete[ix], ds); err != nil {
				klog.V(2).Infof("Failed deletion, decrementing expectations for set %q/%q", ds.Namespace, ds.Name)
				r.expectations.DeletionObserved(dsKey)
				errCh <- err
				utilruntime.HandleError(err)
			}
		}(i)
	}
	deleteWait.Wait()

	// collect errors if any for proper reporting/retry logic in the controller
	errors := []error{}
	close(errCh)
	for err := range errCh {
		errors = append(errors, err)
	}
	return utilerrors.NewAggregate(errors)
}

type podByCreationTimestampAndPhase []*ketiv1.Pod

func (o podByCreationTimestampAndPhase) Len() int      { return len(o) }
func (o podByCreationTimestampAndPhase) Swap(i, j int) { o[i], o[j] = o[j], o[i] }

func (o podByCreationTimestampAndPhase) Less(i, j int) bool {
	// Scheduled Pod first
	if len(o[i].Spec.NodeName) != 0 && len(o[j].Spec.NodeName) == 0 {
		return true
	}

	if len(o[i].Spec.NodeName) == 0 && len(o[j].Spec.NodeName) != 0 {
		return false
	}

	if o[i].CreationTimestamp.Equal(&o[j].CreationTimestamp) {
		return o[i].Name < o[j].Name
	}
	return o[i].CreationTimestamp.Before(&o[j].CreationTimestamp)
}
func failedPodsBackoffKey(ds *ketiv1.DaemonSet, nodeName string) string {
	return fmt.Sprintf("%s/%d/%s", ds.UID, ds.Status.ObservedGeneration, nodeName)
}
// NewPod creates a new pod
func NewPod(ds *ketiv1.DaemonSet, nodeName string) *ketiv1.Pod {
	newPod := &ketiv1.Pod{Spec: ds.Spec.Template.Spec, ObjectMeta: ds.Spec.Template.ObjectMeta}
	newPod.Namespace = ds.Namespace
	newPod.Spec.NodeName = nodeName
	newPod.Labels["keti.checkpoint.type"] = "Daemonset"
	newPod.Labels["keti.checkpoint.name"] = newPod.Name

	// Added default tolerations for DaemonSet pods.
	daemonutil.AddOrUpdateDaemonPodTolerations(&newPod.Spec)

	return newPod
}
// Predicates checks if a DaemonSet's pod can be scheduled on a node using GeneralPredicates
// and PodToleratesNodeTaints predicate
func Predicates(pod *ketiv1.Pod, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []predicates.PredicateFailureReason, error) {
	var predicateFails []predicates.PredicateFailureReason

	fit, reasons, err := checkNodeFitness(pod, nil, nodeInfo)
	if err != nil {
		return false, predicateFails, err
	}
	if !fit {
		predicateFails = append(predicateFails, reasons...)
	}

	return len(predicateFails) == 0, predicateFails, nil
}

// checkNodeFitness runs a set of predicates that select candidate nodes for the DaemonSet;
// the predicates include:
//   - PodFitsHost: checks pod's NodeName against node
//   - PodMatchNodeSelector: checks pod's NodeSelector and NodeAffinity against node
//   - PodToleratesNodeTaints: exclude tainted node unless pod has specific toleration
func checkNodeFitness(pod *ketiv1.Pod, meta predicates.Metadata, nodeInfo *schedulernodeinfo.NodeInfo) (bool, []predicates.PredicateFailureReason, error) {
	var predicateFails []predicates.PredicateFailureReason
	fit, reasons, err := predicates.PodFitsHost((*corev1.Pod)(pod), meta, nodeInfo)
	if err != nil {
		return false, predicateFails, err
	}
	if !fit {
		predicateFails = append(predicateFails, reasons...)
	}

	fit, reasons, err = predicates.PodMatchNodeSelector((*corev1.Pod)(pod), meta, nodeInfo)
	if err != nil {
		return false, predicateFails, err
	}
	if !fit {
		predicateFails = append(predicateFails, reasons...)
	}

	fit, reasons, err = predicates.PodToleratesNodeTaints((*corev1.Pod)(pod), nil, nodeInfo)
	if err != nil {
		return false, predicateFails, err
	}
	if !fit {
		predicateFails = append(predicateFails, reasons...)
	}
	return len(predicateFails) == 0, predicateFails, nil
}
func storeDaemonSetStatus(dsClient clientset.DaemonSetInterface, ds *ketiv1.DaemonSet, desiredNumberScheduled, currentNumberScheduled, numberMisscheduled, numberReady, updatedNumberScheduled, numberAvailable, numberUnavailable int, updateObservedGen bool) error {
	if int(ds.Status.DesiredNumberScheduled) == desiredNumberScheduled &&
		int(ds.Status.CurrentNumberScheduled) == currentNumberScheduled &&
		int(ds.Status.NumberMisscheduled) == numberMisscheduled &&
		int(ds.Status.NumberReady) == numberReady &&
		int(ds.Status.UpdatedNumberScheduled) == updatedNumberScheduled &&
		int(ds.Status.NumberAvailable) == numberAvailable &&
		int(ds.Status.NumberUnavailable) == numberUnavailable &&
		ds.Status.ObservedGeneration >= ds.Generation {
		return nil
	}

	toUpdate := ds.DeepCopy()

	var updateErr, getErr error
	for i := 0; i < StatusUpdateRetries; i++ {
		if updateObservedGen {
			toUpdate.Status.ObservedGeneration = ds.Generation
		}
		toUpdate.Status.DesiredNumberScheduled = int32(desiredNumberScheduled)
		toUpdate.Status.CurrentNumberScheduled = int32(currentNumberScheduled)
		toUpdate.Status.NumberMisscheduled = int32(numberMisscheduled)
		toUpdate.Status.NumberReady = int32(numberReady)
		toUpdate.Status.UpdatedNumberScheduled = int32(updatedNumberScheduled)
		toUpdate.Status.NumberAvailable = int32(numberAvailable)
		toUpdate.Status.NumberUnavailable = int32(numberUnavailable)

		if _, updateErr = dsClient.UpdateStatus(toUpdate); updateErr == nil {
			return nil
		}

		// Update the set with the latest resource version for the next poll
		if toUpdate, getErr = dsClient.Get(ds.Name, metav1.GetOptions{}); getErr != nil {
			// If the GET fails we can't trust status.Replicas anymore. This error
			// is bound to be more interesting than the update failure.
			return getErr
		}
	}
	return updateErr
}

// getUnscheduledPodsWithoutNode returns list of unscheduled pods assigned to not existing nodes.
// Returned pods can't be deleted by PodGCController so they should be deleted by DaemonSetController.
func getUnscheduledPodsWithoutNode(runningNodesList []*corev1.Node, nodeToDaemonPods map[string][]*ketiv1.Pod) []string {
	var results []string
	isNodeRunning := make(map[string]bool)
	for _, node := range runningNodesList {
		isNodeRunning[node.Name] = true
	}
	for n, pods := range nodeToDaemonPods {
		if !isNodeRunning[n] {
			for _, pod := range pods {
				if len(pod.Spec.NodeName) == 0 {
					results = append(results, pod.Name)
				}
			}
		}
	}
	return results
}
