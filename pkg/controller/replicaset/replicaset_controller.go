package replicaset

import (
	"context"
	"fmt"
	"github.com/hth0919/migrationmanager/pkg/controller/util"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"
	"k8s.io/utils/integer"
	"sort"
	"strings"
	"sync"
	"time"

	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	clientset "github.com/hth0919/migrationmanager/pkg/client/keti"
	lister "github.com/hth0919/migrationmanager/pkg/client/lister"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_replicaset")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new ReplicaSet Controller and adds it to the Manager. The Manager will set fields on the Controller
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
	kubeclient, err := kubernetes.NewForConfig(mgr.GetConfig())
	if err != nil {
		klog.Errorln(err)
	}
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeclient.CoreV1().Events("")})
	pc := util.RealPodControl{
		KubeClient: kubeclient,
		KetiClient: client,
		Recorder:   eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "keti-replicaset-controller"}),
	}
	rrs := &ReconcileReplicaSet{
		GroupVersionKind: ketiv1.SchemeGroupVersion.WithKind("ReplicaSet"),
		client: mgr.GetClient(),
		scheme: mgr.GetScheme(),
		expectations:     util.NewUIDTrackingControllerExpectations(util.NewControllerExpectations()),
		queue:            workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "mig-replicaset"),
		kubeClient: client,
		burstReplicas: 10,
		podControl: pc,
	}
	return rrs
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("replicaset-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource ReplicaSet
	err = c.Watch(&source.Kind{Type: &ketiv1.ReplicaSet{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner ReplicaSet
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &ketiv1.ReplicaSet{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileReplicaSet implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileReplicaSet{}

// ReconcileReplicaSet reconciles a ReplicaSet object
type ReconcileReplicaSet struct {
	schema.GroupVersionKind
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client     client.Client
	scheme     *runtime.Scheme
	podLister  lister.PodLister
	kubeClient clientset.KetiV1Interface
	podControl util.PodControlInterface
	// A ReplicaSet is temporarily suspended after creating/deleting these many replicas.
	// It resumes normal action after observing the watch events for them.
	burstReplicas int
	// To allow injection of syncReplicaSet for testing.
	syncHandler func(rsKey string) error

	// A TTLCache of pod creates/deletes each rc expects to see.
	expectations *util.UIDTrackingControllerExpectations

	// A store of ReplicaSets, populated by the shared inf passed to NewReplicaSetController
	rsLister lister.ReplicaSetLister
	// rsListerSynced returns true if the pod store has been synced at least once.
	// Added as a member to the struct to allow injection for testing.
	rsListerSynced cache.InformerSynced
	// Added as a member to the struct to allow injection for testing.
	podListerSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

// Reconcile reads that state of the cluster for a ReplicaSet object and makes changes based on the state read
// and what is in the ReplicaSet.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileReplicaSet) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling ReplicaSet")
	key := request.Namespace + "/" + request.Name
	// Fetch the Pod instance
	instance := &ketiv1.ReplicaSet{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			klog.Infof("%v %v has been deleted", instance.Kind, key)
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}
	startTime := time.Now()
	defer func() {
		klog.Infof("Finished syncing %v %q (%v)", instance.Kind, key, time.Since(startTime))
	}()
	rsNeedsSync := r.expectations.SatisfiedExpectations(key)
	selector, err := metav1.LabelSelectorAsSelector(instance.Spec.Selector)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("error converting pod selector to selector: %v", err))
		return reconcile.Result{},nil
	}

	// list all pods to include the pods that don't match the rs`s selector
	// anymore but has the stale controller ref.
	// TODO: Do the List and Filter in a single pass, or use an index.
	Pods, err := r.kubeClient.Pods(request.Namespace).List(metav1.ListOptions{})
	if err != nil {
		klog.Errorln(err)
		return reconcile.Result{}, err
	}

	// Ignore inactive pods.
	tempPods := Pods.Items
	klog.Infoln(tempPods)
	allPods := make([]*ketiv1.Pod, 0, len(tempPods))
	for _, pod := range tempPods {
		allPods = append(allPods, &pod)
	}
	klog.Infoln(len(allPods))
	filteredPods := util.FilterActivePods(allPods)

	// NOTE: filteredPods are pointing to objects from cache - if you need to
	// modify them, you need to copy it first.
	filteredPods, err = r.claimPods(instance, selector, filteredPods)
	if err != nil {
		return reconcile.Result{}, err
	}

	var manageReplicasErr error
	if rsNeedsSync && instance.DeletionTimestamp == nil {
		manageReplicasErr = r.manageReplicas(filteredPods, instance)
	}
	instance = instance.DeepCopy()
	newStatus := calculateStatus(instance, filteredPods, manageReplicasErr)

	// Always updates status as pods come up or die.
	updatedRS, err := updateReplicaSetStatus(r.kubeClient.ReplicaSets(instance.Namespace), instance, newStatus)
	if err != nil {
		// Multiple things could lead to this update failing. Requeuing the replica set ensures
		// Returning an error causes a requeue without forcing a hotloop
		return reconcile.Result{}, err
	}
	// Resync the ReplicaSet after MinReadySeconds as a last line of defense to guard against clock-skew.
	if manageReplicasErr == nil && updatedRS.Spec.MinReadySeconds > 0 &&
		updatedRS.Status.ReadyReplicas == *(updatedRS.Spec.Replicas) &&
		updatedRS.Status.AvailableReplicas != *(updatedRS.Spec.Replicas) {
		r.queue.AddAfter(key, time.Duration(updatedRS.Spec.MinReadySeconds)*time.Second)
	}
	return reconcile.Result{}, manageReplicasErr
}

func (r *ReconcileReplicaSet) claimPods(rs *ketiv1.ReplicaSet, selector labels.Selector, filteredPods []*ketiv1.Pod) ([]*ketiv1.Pod, error) {
	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing Pods (see #42639).
	canAdoptFunc := util.RecheckDeletionTimestamp(func() (metav1.Object, error) {
		fresh, err := r.kubeClient.ReplicaSets(rs.Namespace).Get(rs.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		if fresh.UID != rs.UID {
			return nil, fmt.Errorf("original %v %v/%v is gone: got uid %v, wanted %v", rs.Kind, rs.Namespace, rs.Name, fresh.UID, rs.UID)
		}
		return fresh, nil
	})
	cm := util.NewPodControllerRefManager(r.podControl, rs, selector, r.GroupVersionKind, canAdoptFunc)
	return cm.ClaimPods(filteredPods)
}


// manageReplicas checks and updates replicas for the given ReplicaSet.
// Does NOT modify <filteredPods>.
// It will requeue the replica set in case of an error while creating/deleting pods.
func (r *ReconcileReplicaSet) manageReplicas(filteredPods []*ketiv1.Pod, rs *ketiv1.ReplicaSet) error {
	diff := len(filteredPods) - int(*(rs.Spec.Replicas))
	klog.Infoln(diff,  " = " , filteredPods, "-", *(rs.Spec.Replicas))
	rsKey, err := util.KeyFunc(rs)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for %v %#v: %v", r.Kind, rs, err))
		return nil
	}
	if diff < 0 {
		diff *= -1
		if diff > r.burstReplicas {
			diff = r.burstReplicas
		}
		// TODO: Track UIDs of creates just like deletes. The problem currently
		// is we'd need to wait on the result of a create to record the pod's
		// UID, which would require locking *across* the create, which will turn
		// into a performance bottleneck. We should generate a UID for the pod
		// beforehand and store it via ExpectCreations.
		r.expectations.ExpectCreations(rsKey, diff)
		klog.Infof("Too few replicas for %v %s/%s, need %d, creating %d", r.Kind, rs.Namespace, rs.Name, *(rs.Spec.Replicas), diff)
		// Batch the pod creates. Batch sizes start at SlowStartInitialBatchSize
		// and double with each successful iteration in a kind of "slow start".
		// This handles attempts to start large numbers of pods that would
		// likely all fail with the same error. For example a project with a
		// low quota that attempts to create a large number of pods will be
		// prevented from spamming the API service with the pod create requests
		// after one of its pods fails.  Conveniently, this also prevents the
		// event spam that those failures would generate.
		successfulCreations, err := slowStartBatch(diff, util.SlowStartInitialBatchSize, func(number int) error {
			klog.Infoln("GOGO")
			err := r.podControl.CreatePodsWithControllerRef(number, rs.Name, rs.Namespace, &rs.Spec.Template, rs, metav1.NewControllerRef(rs, r.GroupVersionKind))
			if err != nil {
				if errors.HasStatusCause(err, corev1.NamespaceTerminatingCause) {
					// if the namespace is being terminated, we don't have to do
					// anything because any creation will fail
					return nil
				}
			}
			return err
		})

		// Any skipped pods that we never attempted to start shouldn't be expected.
		// The skipped pods will be retried later. The next controller resync will
		// retry the slow start process.
		if skippedPods := diff - successfulCreations; skippedPods > 0 {
			klog.Infof("Slow-start failure. Skipping creation of %d pods, decrementing expectations for %v %v/%v", skippedPods, r.Kind, rs.Namespace, rs.Name)
			for i := 0; i < skippedPods; i++ {
				// Decrement the expected number of creates because the inf won't observe this pod
				r.expectations.CreationObserved(rsKey)
			}
		}
		return err
	} else if diff > 0 {
		if diff > r.burstReplicas {
			diff = r.burstReplicas
		}
		klog.Infof("Too many replicas for %v %s/%s, need %d, deleting %d", r.Kind, rs.Namespace, rs.Name, *(rs.Spec.Replicas), diff)

		relatedPods, err := r.getIndirectlyRelatedPods(rs)
		utilruntime.HandleError(err)

		// Choose which Pods to delete, preferring those in earlier phases of startup.
		podsToDelete := getPodsToDelete(filteredPods, relatedPods, diff)

		// Snapshot the UIDs (ns/name) of the pods we're expecting to see
		// deleted, so we know to record their expectations exactly once either
		// when we see it as an update of the deletion timestamp, or as a delete.
		// Note that if the labels on a pod/rs change in a way that the pod gets
		// orphaned, the rs will only wake up after the expectations have
		// expired even if other pods are deleted.
		r.expectations.ExpectDeletions(rsKey, getPodKeys(podsToDelete))

		errCh := make(chan error, diff)
		var wg sync.WaitGroup
		wg.Add(diff)
		for _, pod := range podsToDelete {
			go func(targetPod *ketiv1.Pod) {
				defer wg.Done()
				if err := r.podControl.DeletePod(rs.Namespace, targetPod.Name, rs); err != nil {
					// Decrement the expected number of deletes because the inf won't observe this deletion
					podKey := util.PodKey(targetPod)
					klog.Infof("Failed to delete %v, decrementing expectations for %v %s/%s", podKey, r.Kind, rs.Namespace, rs.Name)
					r.expectations.DeletionObserved(rsKey, podKey)
					errCh <- err
				}
			}(pod)
		}
		wg.Wait()

		select {
		case err := <-errCh:
			// all errors have been reported before and they're likely to be the same, so we'll only return the first one we hit.
			if err != nil {
				return err
			}
		default:
		}
	}

	return nil
}

func (r *ReconcileReplicaSet) getIndirectlyRelatedPods(rs *ketiv1.ReplicaSet) ([]*ketiv1.Pod, error) {
	var relatedPods []*ketiv1.Pod
	seen := make(map[types.UID]*ketiv1.ReplicaSet)
	for _, relatedRS := range r.getReplicaSetsWithSameController(rs) {
		selector, err := metav1.LabelSelectorAsSelector(relatedRS.Spec.Selector)
		if err != nil {
			return nil, err
		}
		pods, err := r.podLister.Pods(relatedRS.Namespace).List(selector)
		if err != nil {
			return nil, err
		}
		for _, pod := range pods {
			if otherRS, found := seen[pod.UID]; found {
				klog.Infof("Pod %s/%s is owned by both %v %s/%s and %v %s/%s", pod.Namespace, pod.Name, r.Kind, otherRS.Namespace, otherRS.Name, r.Kind, relatedRS.Namespace, relatedRS.Name)
				continue
			}
			seen[pod.UID] = relatedRS
			relatedPods = append(relatedPods, pod)
		}
	}
	if klog.V(0) {
		var relatedNames []string
		for _, related := range relatedPods {
			relatedNames = append(relatedNames, related.Name)
		}
		klog.Infof("Found %d related pods for %v %s/%s: %v", len(relatedPods), r.Kind, rs.Namespace, rs.Name, strings.Join(relatedNames, ", "))
	}
	return relatedPods, nil
}
func (r *ReconcileReplicaSet) getReplicaSetsWithSameController(rs *ketiv1.ReplicaSet) []*ketiv1.ReplicaSet {
	controllerRef := metav1.GetControllerOf(rs)
	if controllerRef == nil {
		utilruntime.HandleError(fmt.Errorf("ReplicaSet has no controller: %v", rs))
		return nil
	}

	allRSs, err := r.rsLister.ReplicaSets(rs.Namespace).List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(err)
		return nil
	}

	var relatedRSs []*ketiv1.ReplicaSet
	for _, r := range allRSs {
		if ref := metav1.GetControllerOf(r); ref != nil && ref.UID == controllerRef.UID {
			relatedRSs = append(relatedRSs, r)
		}
	}

	if klog.V(0){
		var related string
		if len(relatedRSs) > 0 {
			var relatedNames []string
			for _, r := range relatedRSs {
				relatedNames = append(relatedNames, r.Name)
			}
			related = ": " + strings.Join(relatedNames, ", ")
		}
		klog.Infof("Found %d related %vs for %v %s/%s%s", len(relatedRSs), r.Kind, r.Kind, rs.Namespace, rs.Name, related)
	}

	return relatedRSs
}

// getPodReplicaSets returns a list of ReplicaSets matching the given pod.
func (r *ReconcileReplicaSet) getPodReplicaSets(pod *ketiv1.Pod) []*ketiv1.ReplicaSet {
	rss, err := r.rsLister.GetPodReplicaSets(pod)
	if err != nil {
		return nil
	}
	if len(rss) > 1 {
		// ControllerRef will ensure we don't do anything crazy, but more than one
		// item in this list nevertheless constitutes user error.
		utilruntime.HandleError(fmt.Errorf("user error! more than one %v is selecting pods with labels: %+v", r.Kind, pod.Labels))
	}
	return rss
}


func slowStartBatch(count int, initialBatchSize int, fn func(number int) error) (int, error) {
	remaining := count
	successes := 0
	for batchSize := integer.IntMin(remaining, BurstReplicas); batchSize > 0; batchSize = integer.IntMin(2*batchSize, remaining) {
		errCh := make(chan error, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for i := 0; i < batchSize; i++ {
			if batchSize<remaining {
				wg.Done()
			 	continue
			}
			n := i + 1
			go func() {
				defer wg.Done()
				if err := fn(n); err != nil {
					errCh <- err
				}
				time.Sleep(time.Second*3)
			}()
		}
		wg.Wait()
		curSuccesses := batchSize - len(errCh)
		successes += curSuccesses
		if len(errCh) > 0 {
			return successes, <-errCh
		}
		remaining -= batchSize
	}
	return successes, nil
}

func getPodsToDelete(filteredPods, relatedPods []*ketiv1.Pod, diff int) []*ketiv1.Pod {
	// No need to sort pods if we are about to delete all of them.
	// diff will always be <= len(filteredPods), so not need to handle > case.
	if diff < len(filteredPods) {
		podsWithRanks := getPodsRankedByRelatedPodsOnSameNode(filteredPods, relatedPods)
		sort.Sort(podsWithRanks)
	}
	return filteredPods[:diff]
}

func getPodsRankedByRelatedPodsOnSameNode(podsToRank, relatedPods []*ketiv1.Pod) util.ActivePodsWithRanks {
	podsOnNode := make(map[string]int)
	for _, pod := range relatedPods {
		if util.IsPodActive(pod) {
			podsOnNode[pod.Spec.NodeName]++
		}
	}
	ranks := make([]int, len(podsToRank))
	for i, pod := range podsToRank {
		ranks[i] = podsOnNode[pod.Spec.NodeName]
	}
	return util.ActivePodsWithRanks{Pods: podsToRank, Rank: ranks}
}

func getPodKeys(pods []*ketiv1.Pod) []string {
	podKeys := make([]string, 0, len(pods))
	for _, pod := range pods {
		podKeys = append(podKeys, util.PodKey(pod))
	}
	return podKeys
}