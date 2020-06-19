package statefulset

import (
	"fmt"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	keticlient "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/util/retry"
)

// StatefulSetStatusUpdaterInterface is an interface used to update the StatefulSetStatus associated with a StatefulSet.
// For any use other than testing, clients should create an instance using NewRealStatefulSetStatusUpdater.
type StatefulSetStatusUpdaterInterface interface {
	// UpdateStatefulSetStatus sets the set's Status to status. Implementations are required to retry on conflicts,
	// but fail on other errors. If the returned error is nil set's Status has been successfully set to status.
	UpdateStatefulSetStatus(set *ketiv1.StatefulSet, status *ketiv1.StatefulSetStatus) error
}

// NewRealStatefulSetStatusUpdater returns a StatefulSetStatusUpdaterInterface that updates the Status of a StatefulSet,
// using the supplied client and setLister.
func NewRealStatefulSetStatusUpdater(
	client keticlient.KetiV1Interface,
	setLister lister.StatefulSetLister) StatefulSetStatusUpdaterInterface {
	return &realStatefulSetStatusUpdater{client, setLister}
}

type realStatefulSetStatusUpdater struct {
	client    keticlient.KetiV1Interface
	setLister lister.StatefulSetLister
}

func (ssu *realStatefulSetStatusUpdater) UpdateStatefulSetStatus(
	set *ketiv1.StatefulSet,
	status *ketiv1.StatefulSetStatus) error {
	// don't wait due to limited number of clients, but backoff after the default number of steps
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		set.Status = *status
		_, updateErr := ssu.client.StatefulSets(set.Namespace).UpdateStatus(set)
		if updateErr == nil {
			return nil
		}
		if updated, err := ssu.client.StatefulSets(set.Namespace).Get(set.Name, metav1.GetOptions{}); err == nil {
			// make a copy so we don't mutate the shared cache
			set = updated.DeepCopy()
		} else {
			utilruntime.HandleError(fmt.Errorf("error getting updated StatefulSet %s/%s from lister: %v", set.Namespace, set.Name, err))
		}

		return updateErr
	})
}

var _ StatefulSetStatusUpdaterInterface = &realStatefulSetStatusUpdater{}
