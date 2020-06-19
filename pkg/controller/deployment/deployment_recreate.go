package deployment

import (
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	deploymentutil "github.com/hth0919/migrationmanager/pkg/controller/deployment/util"
	"github.com/hth0919/migrationmanager/pkg/controller/util"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// rolloutRecreate implements the logic for recreating a replica set.
func (r *ReconcileDeployment) rolloutRecreate(d *ketiv1.Deployment, rsList []*ketiv1.ReplicaSet, podMap map[types.UID][]*ketiv1.Pod) error {
	// Don't create a new RS if not already existed, so that we avoid scaling up before scaling down.
	newRS, oldRSs, err := r.getAllReplicaSetsAndSyncRevision(d, rsList, false)
	if err != nil {
		return err
	}
	allRSs := append(oldRSs, newRS)
	activeOldRSs := util.FilterActiveReplicaSets(oldRSs)

	// scale down old replica sets.
	scaledDown, err := r.scaleDownOldReplicaSetsForRecreate(activeOldRSs, d)
	if err != nil {
		return err
	}
	if scaledDown {
		// Update DeploymentStatus.
		return r.syncRolloutStatus(allRSs, newRS, d)
	}

	// Do not process a deployment when it has old pods running.
	if oldPodsRunning(newRS, oldRSs, podMap) {
		return r.syncRolloutStatus(allRSs, newRS, d)
	}

	// If we need to create a new RS, create it now.
	if newRS == nil {
		newRS, oldRSs, err = r.getAllReplicaSetsAndSyncRevision(d, rsList, true)
		if err != nil {
			return err
		}
		allRSs = append(oldRSs, newRS)
	}

	// scale up new replica set.
	if _, err := r.scaleUpNewReplicaSetForRecreate(newRS, d); err != nil {
		return err
	}

	if deploymentutil.DeploymentComplete(d, &d.Status) {
		if err := r.cleanupDeployment(oldRSs, d); err != nil {
			return err
		}
	}

	// Sync deployment status.
	return r.syncRolloutStatus(allRSs, newRS, d)
}

// scaleDownOldReplicaSetsForRecreate scales down old replica sets when deployment strategy is "Recreate".
func (r *ReconcileDeployment) scaleDownOldReplicaSetsForRecreate(oldRSs []*ketiv1.ReplicaSet, deployment *ketiv1.Deployment) (bool, error) {
	scaled := false
	for i := range oldRSs {
		rs := oldRSs[i]
		// Scaling not required.
		if *(rs.Spec.Replicas) == 0 {
			continue
		}
		scaledRS, updatedRS, err := r.scaleReplicaSetAndRecordEvent(rs, 0, deployment)
		if err != nil {
			return false, err
		}
		if scaledRS {
			oldRSs[i] = updatedRS
			scaled = true
		}
	}
	return scaled, nil
}

// oldPodsRunning returns whether there are old pods running or any of the old ReplicaSets thinks that it runs pods.
func oldPodsRunning(newRS *ketiv1.ReplicaSet, oldRSs []*ketiv1.ReplicaSet, podMap map[types.UID][]*ketiv1.Pod) bool {
	if oldPods := deploymentutil.GetActualReplicaCountForReplicaSets(oldRSs); oldPods > 0 {
		return true
	}
	for rsUID, podList := range podMap {
		// If the pods belong to the new ReplicaSet, ignore.
		if newRS != nil && newRS.UID == rsUID {
			continue
		}
		for _, pod := range podList {
			switch pod.Status.Phase {
			case v1.PodFailed, v1.PodSucceeded:
				// Don't count pods in terminal state.
				continue
			case v1.PodUnknown:
				// This happens in situation like when the node is temporarily disconnected from the cluster.
				// If we can't be sure that the pod is not running, we have to count it.
				return true
			default:
				// Pod is not in terminal phase.
				return true
			}
		}
	}
	return false
}

// scaleUpNewReplicaSetForRecreate scales up new replica set when deployment strategy is "Recreate".
func (r *ReconcileDeployment) scaleUpNewReplicaSetForRecreate(newRS *ketiv1.ReplicaSet, deployment *ketiv1.Deployment) (bool, error) {
	scaled, _, err := r.scaleReplicaSetAndRecordEvent(newRS, *(deployment.Spec.Replicas), deployment)
	return scaled, err
}
