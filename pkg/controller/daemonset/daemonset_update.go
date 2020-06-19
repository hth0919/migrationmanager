package daemonset

import (
	"bytes"
	"encoding/json"
	"fmt"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	daemonutil "github.com/hth0919/migrationmanager/pkg/controller/daemonset/util"
	"github.com/hth0919/migrationmanager/pkg/controller/util"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	intstrutil "k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	labelsutil "k8s.io/kubernetes/pkg/util/labels"
	"reflect"
	"sort"
)

func (r *ReconcileDaemonSet) constructHistory(ds *ketiv1.DaemonSet) (cur *ketiv1.ControllerRevision, old []*ketiv1.ControllerRevision, err error) {
	var histories []*ketiv1.ControllerRevision
	var currentHistories []*ketiv1.ControllerRevision
	histories, err = r.controlledHistories(ds)
	if err != nil {
		return nil, nil, err
	}
	for _, history := range histories {
		// Add the unique label if it's not already added to the history
		// We use history name instead of computing hash, so that we don't need to worry about hash collision
		if _, ok := history.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey]; !ok {
			toUpdate := history.DeepCopy()
			toUpdate.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey] = toUpdate.Name
			history, err = r.KetiClient.ControllerRevisions(ds.Namespace).Update(toUpdate)
			if err != nil {
				return nil, nil, err
			}
		}
		// Compare histories with ds to separate cur and old history
		found := false
		found, err = Match(ds, history)
		if err != nil {
			return nil, nil, err
		}
		if found {
			currentHistories = append(currentHistories, history)
		} else {
			old = append(old, history)
		}
	}

	currRevision := maxRevision(old) + 1
	switch len(currentHistories) {
	case 0:
		// Create a new history if the current one isn't found
		cur, err = r.snapshot(ds, currRevision)
		if err != nil {
			return nil, nil, err
		}
	default:
		cur, err = r.dedupCurHistories(ds, currentHistories)
		if err != nil {
			return nil, nil, err
		}
		// Update revision number if necessary
		if cur.Revision < currRevision {
			toUpdate := cur.DeepCopy()
			toUpdate.Revision = currRevision
			_, err = r.KetiClient.ControllerRevisions(ds.Namespace).Update(toUpdate)
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return cur, old, err
}

func (r *ReconcileDaemonSet) controlledHistories(ds *ketiv1.DaemonSet) ([]*ketiv1.ControllerRevision, error) {
	selector, err := metav1.LabelSelectorAsSelector(ds.Spec.Selector)
	if err != nil {
		return nil, err
	}
	h, err := r.KetiClient.ControllerRevisions(ds.Namespace).List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	temph := h.Items
	histories := make([]*ketiv1.ControllerRevision, 0, len(temph))
	for _, rs := range temph {
		histories = append(histories, &rs)
	}
	klog.Infoln("history length",len(histories))
	// If any adoptions are attempted, we should first recheck for deletion with
	// an uncached quorum read sometime after listing Pods (see #42639).
	canAdoptFunc := util.RecheckDeletionTimestamp(func() (metav1.Object, error) {
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
	cm := util.NewControllerRevisionControllerRefManager(r.crControl, ds, selector, controllerKind, canAdoptFunc)
	return cm.ClaimControllerRevisions(histories)
}

func (r *ReconcileDaemonSet) rollingUpdate(ds *ketiv1.DaemonSet, nodeList []*corev1.Node, hash string) error {
	nodeToDaemonPods, err := r.getNodesToDaemonPods(ds)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	_, oldPods := r.getAllDaemonSetPods(ds, nodeToDaemonPods, hash)
	maxUnavailable, numUnavailable, err := r.getUnavailableNumbers(ds, nodeList, nodeToDaemonPods)
	if err != nil {
		return fmt.Errorf("couldn't get unavailable numbers: %v", err)
	}
	oldAvailablePods, oldUnavailablePods := daemonutil.SplitByAvailablePods(ds.Spec.MinReadySeconds, oldPods)

	// for oldPods delete all not running pods
	var oldPodsToDelete []string
	klog.V(4).Infof("Marking all unavailable old pods for deletion")
	for _, pod := range oldUnavailablePods {
		// Skip terminating pods. We won't delete them again
		if pod.DeletionTimestamp != nil {
			continue
		}
		klog.V(4).Infof("Marking pod %s/%s for deletion", ds.Name, pod.Name)
		oldPodsToDelete = append(oldPodsToDelete, pod.Name)
	}

	klog.V(4).Infof("Marking old pods for deletion")
	for _, pod := range oldAvailablePods {
		if numUnavailable >= maxUnavailable {
			klog.V(4).Infof("Number of unavailable DaemonSet pods: %d, is equal to or exceeds allowed maximum: %d", numUnavailable, maxUnavailable)
			break
		}
		klog.V(4).Infof("Marking pod %s/%s for deletion", ds.Name, pod.Name)
		oldPodsToDelete = append(oldPodsToDelete, pod.Name)
		numUnavailable++
	}
	return r.syncNodes(ds, oldPodsToDelete, []string{}, hash)
}
func (r *ReconcileDaemonSet) cleanupHistory(ds *ketiv1.DaemonSet, old []*ketiv1.ControllerRevision) error {
	nodesToDaemonPods, err := r.getNodesToDaemonPods(ds)
	if err != nil {
		return fmt.Errorf("couldn't get node to daemon pod mapping for daemon set %q: %v", ds.Name, err)
	}

	toKeep := int(*ds.Spec.RevisionHistoryLimit)
	toKill := len(old) - toKeep
	if toKill <= 0 {
		return nil
	}

	// Find all hashes of live pods
	liveHashes := make(map[string]bool)
	for _, pods := range nodesToDaemonPods {
		for _, pod := range pods {
			if hash := pod.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey]; len(hash) > 0 {
				liveHashes[hash] = true
			}
		}
	}

	// Clean up old history from smallest to highest revision (from oldest to newest)
	sort.Sort(historiesByRevision(old))
	for _, history := range old {
		if toKill <= 0 {
			break
		}
		if hash := history.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey]; liveHashes[hash] {
			continue
		}
		// Clean up
		err := r.KetiClient.ControllerRevisions(ds.Namespace).Delete(history.Name, nil)
		if err != nil {
			return err
		}
		toKill--
	}
	return nil
}

// maxRevision returns the max revision number of the given list of histories
func maxRevision(histories []*ketiv1.ControllerRevision) int64 {
	max := int64(0)
	for _, history := range histories {
		if history.Revision > max {
			max = history.Revision
		}
	}
	return max
}

func (r *ReconcileDaemonSet) dedupCurHistories(ds *ketiv1.DaemonSet, curHistories []*ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error) {
	if len(curHistories) == 1 {
		return curHistories[0], nil
	}
	var maxRevision int64
	var keepCur *ketiv1.ControllerRevision
	for _, cur := range curHistories {
		if cur.Revision >= maxRevision {
			keepCur = cur
			maxRevision = cur.Revision
		}
	}
	// Clean up duplicates and relabel pods
	for _, cur := range curHistories {
		if cur.Name == keepCur.Name {
			continue
		}
		// Relabel pods before dedup
		pods, err := r.getDaemonPods(ds)
		if err != nil {
			return nil, err
		}
		for _, pod := range pods {
			if pod.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey] != keepCur.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey] {
				toUpdate := pod.DeepCopy()
				if toUpdate.Labels == nil {
					toUpdate.Labels = make(map[string]string)
				}
				toUpdate.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey] = keepCur.Labels[ketiv1.DefaultDaemonSetUniqueLabelKey]
				_, err = r.KetiClient.Pods(ds.Namespace).Update(toUpdate)
				if err != nil {
					return nil, err
				}
			}
		}
		// Remove duplicates
		err = r.KetiClient.ControllerRevisions(ds.Namespace).Delete(cur.Name, nil)
		if err != nil {
			return nil, err
		}
	}
	return keepCur, nil
}



// Match check if the given DaemonSet's template matches the template stored in the given history.
func Match(ds *ketiv1.DaemonSet, history *ketiv1.ControllerRevision) (bool, error) {
	patch, err := getPatch(ds)
	if err != nil {
		return false, err
	}
	return bytes.Equal(patch, history.Data.Raw), nil
}

// getPatch returns a strategic merge patch that can be applied to restore a Daemonset to a
// previous version. If the returned error is nil the patch is valid. The current state that we save is just the
// PodSpecTemplate. We can modify this later to encompass more state (or less) and remain compatible with previously
// recorded patches.
func getPatch(ds *ketiv1.DaemonSet) ([]byte, error) {
	dsBytes, err := json.Marshal(ds)
	if err != nil {
		return nil, err
	}
	var raw map[string]interface{}
	err = json.Unmarshal(dsBytes, &raw)
	if err != nil {
		return nil, err
	}
	objCopy := make(map[string]interface{})
	specCopy := make(map[string]interface{})

	// Create a patch of the DaemonSet that replaces spec.template
	spec := raw["spec"].(map[string]interface{})
	template := spec["template"].(map[string]interface{})
	specCopy["template"] = template
	template["$patch"] = "replace"
	objCopy["spec"] = specCopy
	patch, err := json.Marshal(objCopy)
	return patch, err
}

func (r *ReconcileDaemonSet) snapshot(ds *ketiv1.DaemonSet, revision int64) (*ketiv1.ControllerRevision, error) {
	patch, err := getPatch(ds)
	if err != nil {
		return nil, err
	}
	hash := util.ComputeHash(&ds.Spec.Template, ds.Status.CollisionCount)
	name := ds.Name + "-" + hash
	history := &ketiv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ds.Namespace,
			Labels:          labelsutil.CloneAndAddLabel(ds.Spec.Template.Labels, ketiv1.DefaultDaemonSetUniqueLabelKey, hash),
			Annotations:     ds.Annotations,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(ds, controllerKind)},
		},
		Data:     runtime.RawExtension{Raw: patch},
		Revision: revision,
	}

	history, err = r.KetiClient.ControllerRevisions(ds.Namespace).Create(history)
	if outerErr := err; errors.IsAlreadyExists(outerErr) {
		// TODO: Is it okay to get from historyLister?
		existedHistory, getErr := r.KetiClient.ControllerRevisions(ds.Namespace).Get(name, metav1.GetOptions{})
		if getErr != nil {
			return nil, getErr
		}
		// Check if we already created it
		done, matchErr := Match(ds, existedHistory)
		if matchErr != nil {
			return nil, matchErr
		}
		if done {
			return existedHistory, nil
		}

		// Handle name collisions between different history
		// Get the latest DaemonSet from the API server to make sure collision count is only increased when necessary
		currDS, getErr := r.KetiClient.DaemonSets(ds.Namespace).Get(ds.Name, metav1.GetOptions{})
		if getErr != nil {
			return nil, getErr
		}
		// If the collision count used to compute hash was in fact stale, there's no need to bump collision count; retry again
		if !reflect.DeepEqual(currDS.Status.CollisionCount, ds.Status.CollisionCount) {
			return nil, fmt.Errorf("found a stale collision count (%d, expected %d) of DaemonSet %q while processing; will retry until it is updated", ds.Status.CollisionCount, currDS.Status.CollisionCount, ds.Name)
		}
		if currDS.Status.CollisionCount == nil {
			currDS.Status.CollisionCount = new(int32)
		}
		*currDS.Status.CollisionCount++
		_, updateErr := r.KetiClient.DaemonSets(ds.Namespace).UpdateStatus(currDS)
		if updateErr != nil {
			return nil, updateErr
		}
		klog.V(2).Infof("Found a hash collision for DaemonSet %q - bumping collisionCount to %d to resolve it", ds.Name, *currDS.Status.CollisionCount)
		return nil, outerErr
	}
	return history, err
}

func (r *ReconcileDaemonSet) getAllDaemonSetPods(ds *ketiv1.DaemonSet, nodeToDaemonPods map[string][]*ketiv1.Pod, hash string) ([]*ketiv1.Pod, []*ketiv1.Pod) {
	var newPods []*ketiv1.Pod
	var oldPods []*ketiv1.Pod

	for _, pods := range nodeToDaemonPods {
		for _, pod := range pods {
			// If the returned error is not nil we have a parse error.
			// The controller handles this via the hash.
			generation, err := daemonutil.GetTemplateGeneration(ds)
			if err != nil {
				generation = nil
			}
			if daemonutil.IsPodUpdated(pod, hash, generation) {
				newPods = append(newPods, pod)
			} else {
				oldPods = append(oldPods, pod)
			}
		}
	}
	return newPods, oldPods
}

func (r *ReconcileDaemonSet) getUnavailableNumbers(ds *ketiv1.DaemonSet, nodeList []*corev1.Node, nodeToDaemonPods map[string][]*ketiv1.Pod) (int, int, error) {
	klog.V(4).Infof("Getting unavailable numbers")
	var numUnavailable, desiredNumberScheduled int
	for i := range nodeList {
		node := nodeList[i]
		wantToRun, _, _, err := r.nodeShouldRunDaemonPod(node, ds)
		if err != nil {
			return -1, -1, err
		}
		if !wantToRun {
			continue
		}
		desiredNumberScheduled++
		daemonPods, exists := nodeToDaemonPods[node.Name]
		if !exists {
			numUnavailable++
			continue
		}
		available := false
		for _, pod := range daemonPods {
			//for the purposes of update we ensure that the Pod is both available and not terminating

			if podutil.IsPodAvailable((*corev1.Pod)(pod), ds.Spec.MinReadySeconds, metav1.Now()) && pod.DeletionTimestamp == nil {
				available = true
				break
			}
		}
		if !available {
			numUnavailable++
		}
	}
	maxUnavailable, err := intstrutil.GetValueFromIntOrPercent(ds.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable, desiredNumberScheduled, true)
	if err != nil {
		return -1, -1, fmt.Errorf("invalid value for MaxUnavailable: %v", err)
	}
	klog.V(4).Infof(" DaemonSet %s/%s, maxUnavailable: %d, numUnavailable: %d", ds.Namespace, ds.Name, maxUnavailable, numUnavailable)
	return maxUnavailable, numUnavailable, nil
}

type historiesByRevision []*ketiv1.ControllerRevision

func (h historiesByRevision) Len() int      { return len(h) }
func (h historiesByRevision) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h historiesByRevision) Less(i, j int) bool {
	return h[i].Revision < h[j].Revision
}

