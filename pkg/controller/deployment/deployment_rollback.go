package deployment

import (
	"fmt"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	deploymentutil "github.com/hth0919/migrationmanager/pkg/controller/deployment/util"
	v1 "k8s.io/api/core/v1"
	extensions "k8s.io/api/extensions/v1beta1"
	"k8s.io/klog"
	"strconv"
)

func (r *ReconcileDeployment) rollback(d *ketiv1.Deployment, rsList []*ketiv1.ReplicaSet) error {
	newRS, allOldRSs, err := r.getAllReplicaSetsAndSyncRevision(d, rsList, true)
	if err != nil {
		return err
	}

	allRSs := append(allOldRSs, newRS)
	rollbackTo := getRollbackTo(d)
	// If rollback revision is 0, rollback to the last revision
	if rollbackTo.Revision == 0 {
		if rollbackTo.Revision = deploymentutil.LastRevision(allRSs); rollbackTo.Revision == 0 {
			// If we still can't find the last revision, gives up rollback
			r.emitRollbackWarningEvent(d, deploymentutil.RollbackRevisionNotFound, "Unable to find last revision.")
			// Gives up rollback
			return r.updateDeploymentAndClearRollbackTo(d)
		}
	}
	for _, rs := range allRSs {
		v, err := deploymentutil.Revision(rs)
		if err != nil {
			klog.V(4).Infof("Unable to extract revision from deployment's replica set %q: %v", rs.Name, err)
			continue
		}
		if v == rollbackTo.Revision {
			klog.V(4).Infof("Found replica set %q with desired revision %d", rs.Name, v)
			// rollback by copying podTemplate.Spec from the replica set
			// revision number will be incremented during the next getAllReplicaSetsAndSyncRevision call
			// no-op if the spec matches current deployment's podTemplate.Spec
			performedRollback, err := r.rollbackToTemplate(d, rs)
			if performedRollback && err == nil {
				r.emitRollbackNormalEvent(d, fmt.Sprintf("Rolled back deployment %q to revision %d", d.Name, rollbackTo.Revision))
			}
			return err
		}
	}
	r.emitRollbackWarningEvent(d, deploymentutil.RollbackRevisionNotFound, "Unable to find the revision to rollback to.")
	// Gives up rollback
	return r.updateDeploymentAndClearRollbackTo(d)
}

func (r *ReconcileDeployment) rollbackToTemplate(d *ketiv1.Deployment, rs *ketiv1.ReplicaSet) (bool, error) {
	performedRollback := false
	if !deploymentutil.EqualIgnoreHash(&d.Spec.Template, &rs.Spec.Template) {
		klog.V(4).Infof("Rolling back deployment %q to template spec %+v", d.Name, rs.Spec.Template.Spec)
		deploymentutil.SetFromReplicaSetTemplate(d, rs.Spec.Template)
		// set RS (the old RS we'll rolling back to) annotations back to the deployment;
		// otherwise, the deployment's current annotations (should be the same as current new RS) will be copied to the RS after the rollback.
		//
		// For example,
		// A Deployment has old RS1 with annotation {change-cause:create}, and new RS2 {change-cause:edit}.
		// Note that both annotations are copied from Deployment, and the Deployment should be annotated {change-cause:edit} as well.
		// Now, rollback Deployment to RS1, we should update Deployment's pod-template and also copy annotation from RS1.
		// Deployment is now annotated {change-cause:create}, and we have new RS1 {change-cause:create}, old RS2 {change-cause:edit}.
		//
		// If we don't copy the annotations back from RS to deployment on rollback, the Deployment will stay as {change-cause:edit},
		// and new RS1 becomes {change-cause:edit} (copied from deployment after rollback), old RS2 {change-cause:edit}, which is not correct.
		deploymentutil.SetDeploymentAnnotationsTo(d, rs)
		performedRollback = true
	} else {
		klog.V(4).Infof("Rolling back to a revision that contains the same template as current deployment %q, skipping rollback...", d.Name)
		eventMsg := fmt.Sprintf("The rollback revision contains the same template as current deployment %q", d.Name)
		r.emitRollbackWarningEvent(d, deploymentutil.RollbackTemplateUnchanged, eventMsg)
	}

	return performedRollback, r.updateDeploymentAndClearRollbackTo(d)
}
func (r *ReconcileDeployment) emitRollbackWarningEvent(d *ketiv1.Deployment, reason, message string) {
	klog.Infoln("Deployment ", d.Name, v1.EventTypeWarning, reason, message)
}

func (r *ReconcileDeployment) updateDeploymentAndClearRollbackTo(d *ketiv1.Deployment) error {
	klog.V(4).Infof("Cleans up rollbackTo of deployment %q", d.Name)
	setRollbackTo(d, nil)
	_, err := r.KetiClient.Deployments(d.Namespace).Update(d)
	return err
}

func (r *ReconcileDeployment) emitRollbackNormalEvent(d *ketiv1.Deployment, message string) {
	klog.Infoln("Deployment ", d.Name, v1.EventTypeNormal, deploymentutil.RollbackDone, message)
}
func getRollbackTo(d *ketiv1.Deployment) *extensions.RollbackConfig {
	// Extract the annotation used for round-tripping the deprecated RollbackTo field.
	revision := d.Annotations[ketiv1.DeprecatedRollbackTo]
	if revision == "" {
		return nil
	}
	revision64, err := strconv.ParseInt(revision, 10, 64)
	if err != nil {
		// If it's invalid, ignore it.
		return nil
	}
	return &extensions.RollbackConfig{
		Revision: revision64,
	}
}

func setRollbackTo(d *ketiv1.Deployment, rollbackTo *extensions.RollbackConfig) {
	if rollbackTo == nil {
		delete(d.Annotations, ketiv1.DeprecatedRollbackTo)
		return
	}
	if d.Annotations == nil {
		d.Annotations = make(map[string]string)
	}
	d.Annotations[ketiv1.DeprecatedRollbackTo] = strconv.FormatInt(rollbackTo.Revision, 10)
}