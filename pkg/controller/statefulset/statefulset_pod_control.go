package statefulset

import (
	"fmt"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	keticlient "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog"
	"strings"
)

// StatefulPodControlInterface defines the interface that StatefulSetController uses to create, update, and delete Pods,
// and to update the Status of a StatefulSet. It follows the design paradigms used for PodControl, but its
// implementation provides for PVC creation, ordered Pod creation, ordered Pod termination, and Pod identity enforcement.
// Like controller.PodControlInterface, it is implemented as an interface to provide for testing fakes.
type StatefulPodControlInterface interface {
	// CreateStatefulPod create a Pod in a StatefulSet. Any PVCs necessary for the Pod are created prior to creating
	// the Pod. If the returned error is nil the Pod and its PVCs have been created.
	CreateStatefulPod(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error
	// UpdateStatefulPod Updates a Pod in a StatefulSet. If the Pod already has the correct identity and stable
	// storage this method is a no-op. If the Pod must be mutated to conform to the Set, it is mutated and updated.
	// pod is an in-out parameter, and any updates made to the pod are reflected as mutations to this parameter. If
	// the create is successful, the returned error is nil.
	UpdateStatefulPod(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error
	// DeleteStatefulPod deletes a Pod in a StatefulSet. The pods PVCs are not deleted. If the delete is successful,
	// the returned error is nil.
	DeleteStatefulPod(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error
}

func NewRealStatefulPodControl(
	kubeClient clientset.Interface,
	client keticlient.KetiV1Interface,
	setLister lister.StatefulSetLister,
	podLister lister.PodLister,
	pvcLister corelisters.PersistentVolumeClaimLister,
	recorder record.EventRecorder,
) StatefulPodControlInterface {
	return &realStatefulPodControl{kubeClient, client, setLister, podLister, pvcLister, recorder}
}

// realStatefulPodControl implements StatefulPodControlInterface using a clientset.Interface to communicate with the
// API server. The struct is package private as the internal details are irrelevant to importing packages.
type realStatefulPodControl struct {
	kubeClient clientset.Interface
	client   keticlient.KetiV1Interface
	setLister lister.StatefulSetLister
	podLister lister.PodLister
	pvcLister corelisters.PersistentVolumeClaimLister
	recorder  record.EventRecorder
}

func (spc *realStatefulPodControl) CreateStatefulPod(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error {
	// Create the Pod's PVCs prior to creating the Pod
	if err := spc.createPersistentVolumeClaims(set, pod); err != nil {
		spc.recordPodEvent("create", set, pod, err)
		return err
	}
	// If we created the PVCs attempt to create the Pod
	_, err := spc.client.Pods(set.Namespace).Create(pod)
	// sink already exists errors
	if apierrors.IsAlreadyExists(err) {
		return err
	}
	spc.recordPodEvent("create", set, pod, err)
	return err
}

func (spc *realStatefulPodControl) UpdateStatefulPod(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error {
	attemptedUpdate := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// assume the Pod is consistent
		consistent := true
		// if the Pod does not conform to its identity, update the identity and dirty the Pod
		if !identityMatches(set, pod) {
			updateIdentity(set, pod)
			consistent = false
		}
		// if the Pod does not conform to the StatefulSet's storage requirements, update the Pod's PVC's,
		// dirty the Pod, and create any missing PVCs
		if !storageMatches(set, pod) {
			updateStorage(set, pod)
			consistent = false
			if err := spc.createPersistentVolumeClaims(set, pod); err != nil {
				spc.recordPodEvent("update", set, pod, err)
				return err
			}
		}
		// if the Pod is not dirty, do nothing
		if consistent {
			return nil
		}

		attemptedUpdate = true
		// commit the update, retrying on conflicts
		_, updateErr := spc.client.Pods(set.Namespace).Update(pod)
		if updateErr == nil {
			return nil
		}

		if updated, err := spc.podLister.Pods(set.Namespace).Get(pod.Name); err == nil {
			// make a copy so we don't mutate the shared cache
			pod = updated.DeepCopy()
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated Pod %s/%s from lister: %v", set.Namespace, pod.Name, err))
		}

		return updateErr
	})
	if attemptedUpdate {
		spc.recordPodEvent("update", set, pod, err)
	}
	return err
}

func (spc *realStatefulPodControl) DeleteStatefulPod(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error {
	err := spc.client.Pods(set.Namespace).Delete(pod.Name, nil)
	spc.recordPodEvent("delete", set, pod, err)
	return err
}

// recordPodEvent records an event for verb applied to a Pod in a StatefulSet. If err is nil the generated event will
// have a reason of v1.EventTypeNormal. If err is not nil the generated event will have a reason of v1.EventTypeWarning.
func (spc *realStatefulPodControl) recordPodEvent(verb string, set *ketiv1.StatefulSet, pod *ketiv1.Pod, err error) {
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		message := fmt.Sprintf("%s Pod %s in StatefulSet %s successful",
			strings.ToLower(verb), pod.Name, set.Name)
		spc.recorder.Event(set, v1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		message := fmt.Sprintf("%s Pod %s in StatefulSet %s failed error: %s",
			strings.ToLower(verb), pod.Name, set.Name, err)
		spc.recorder.Event(set, v1.EventTypeWarning, reason, message)
	}
}

// recordClaimEvent records an event for verb applied to the PersistentVolumeClaim of a Pod in a StatefulSet. If err is
// nil the generated event will have a reason of v1.EventTypeNormal. If err is not nil the generated event will have a
// reason of v1.EventTypeWarning.
func (spc *realStatefulPodControl) recordClaimEvent(verb string, set *ketiv1.StatefulSet, pod *ketiv1.Pod, claim *v1.PersistentVolumeClaim, err error) {
	if err == nil {
		reason := fmt.Sprintf("Successful%s", strings.Title(verb))
		message := fmt.Sprintf("%s Claim %s Pod %s in StatefulSet %s success",
			strings.ToLower(verb), claim.Name, pod.Name, set.Name)
		spc.recorder.Event(set, v1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", strings.Title(verb))
		message := fmt.Sprintf("%s Claim %s for Pod %s in StatefulSet %s failed error: %s",
			strings.ToLower(verb), claim.Name, pod.Name, set.Name, err)
		spc.recorder.Event(set, v1.EventTypeWarning, reason, message)
	}
}

// createPersistentVolumeClaims creates all of the required PersistentVolumeClaims for pod, which must be a member of
// set. If all of the claims for Pod are successfully created, the returned error is nil. If creation fails, this method
// may be called again until no error is returned, indicating the PersistentVolumeClaims for pod are consistent with
// set's Spec.
func (spc *realStatefulPodControl) createPersistentVolumeClaims(set *ketiv1.StatefulSet, pod *ketiv1.Pod) error {
	var errs []error
	for _, claim := range getPersistentVolumeClaims(set, pod) {
		_, err := spc.kubeClient.CoreV1().PersistentVolumeClaims(claim.Namespace).Get(claim.Name, metav1.GetOptions{})
		switch {
		case apierrors.IsNotFound(err):
			klog.Infoln(claim)
			response, err := spc.kubeClient.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(&claim)
			klog.Infoln(response)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to create PVC %s: %s", claim.Name, err))
			}
			if err == nil || !apierrors.IsAlreadyExists(err) {
				spc.recordClaimEvent("create", set, pod, &claim, err)
			}
		case err != nil:
			errs = append(errs, fmt.Errorf("failed to retrieve PVC %s: %s", claim.Name, err))
			spc.recordClaimEvent("create", set, pod, &claim, err)
		}
		// TODO: Check resource requirements and accessmodes, update if necessary
	}
	return errorutils.NewAggregate(errs)
}

var _ StatefulPodControlInterface = &realStatefulPodControl{}
