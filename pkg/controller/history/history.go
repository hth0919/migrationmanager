package history

import (
	"bytes"
	"encoding/json"
	"fmt"
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"
	keticlient "github.com/hth0919/migrationmanager/pkg/client/keti"
	"github.com/hth0919/migrationmanager/pkg/client/lister"
	"hash/fnv"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/util/retry"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
	"sort"
	"strconv"
)
const ControllerRevisionHashLabel = "controller.kubernetes.io/hash"
// EqualRevision returns true if lhs and rhs are either both nil, or both point to non-nil ControllerRevisions that
// contain semantically equivalent data. Otherwise this method returns false.
func EqualRevision(lhs *ketiv1.ControllerRevision, rhs *ketiv1.ControllerRevision) bool {
	var lhsHash, rhsHash *uint32
	if lhs == nil || rhs == nil {
		return lhs == rhs
	}
	if hs, found := lhs.Labels[ControllerRevisionHashLabel]; found {
		hash, err := strconv.ParseInt(hs, 10, 32)
		if err == nil {
			lhsHash = new(uint32)
			*lhsHash = uint32(hash)
		}
	}
	if hs, found := rhs.Labels[ControllerRevisionHashLabel]; found {
		hash, err := strconv.ParseInt(hs, 10, 32)
		if err == nil {
			rhsHash = new(uint32)
			*rhsHash = uint32(hash)
		}
	}
	if lhsHash != nil && rhsHash != nil && *lhsHash != *rhsHash {
		return false
	}
	return bytes.Equal(lhs.Data.Raw, rhs.Data.Raw) && apiequality.Semantic.DeepEqual(lhs.Data.Object, rhs.Data.Object)
}
// FindEqualRevisions returns all ControllerRevisions in revisions that are equal to needle using EqualRevision as the
// equality test. The returned slice preserves the order of revisions.
func FindEqualRevisions(revisions []*ketiv1.ControllerRevision, needle *ketiv1.ControllerRevision) []*ketiv1.ControllerRevision {
	var eq []*ketiv1.ControllerRevision
	for i := range revisions {
		if EqualRevision(revisions[i], needle) {
			eq = append(eq, revisions[i])
		}
	}
	return eq
}
// byRevision implements sort.Interface to allow ControllerRevisions to be sorted by Revision.
type byRevision []*ketiv1.ControllerRevision

func (br byRevision) Len() int {
	return len(br)
}

// Less breaks ties first by creation timestamp, then by name
func (br byRevision) Less(i, j int) bool {
	if br[i].Revision == br[j].Revision {
		if br[j].CreationTimestamp.Equal(&br[i].CreationTimestamp) {
			return br[i].Name < br[j].Name
		}
		return br[j].CreationTimestamp.After(br[i].CreationTimestamp.Time)
	}
	return br[i].Revision < br[j].Revision
}

func (br byRevision) Swap(i, j int) {
	br[i], br[j] = br[j], br[i]
}
// Interface provides an interface allowing for management of a Controller's history as realized by recorded
// ControllerRevisions. An instance of Interface can be retrieved from NewHistory. Implementations must treat all
// pointer parameters as "in" parameter, and they must not be mutated.
type Interface interface {
	// ListControllerRevisions lists all ControllerRevisions matching selector and owned by parent or no other
	// controller. If the returned error is nil the returned slice of ControllerRevisions is valid. If the
	// returned error is not nil, the returned slice is not valid.
	ListControllerRevisions(parent metav1.Object, selector labels.Selector) ([]*ketiv1.ControllerRevision, error)
	// CreateControllerRevision attempts to create the revision as owned by parent via a ControllerRef. If name
	// collision occurs, collisionCount (incremented each time collision occurs except for the first time) is
	// added to the hash of the revision and it is renamed using ControllerRevisionName. Implementations may
	// cease to attempt to retry creation after some number of attempts and return an error. If the returned
	// error is not nil, creation failed. If the returned error is nil, the returned ControllerRevision has been
	// created.
	// Callers must make sure that collisionCount is not nil. An error is returned if it is.
	CreateControllerRevision(parent metav1.Object, revision *ketiv1.ControllerRevision, collisionCount *int32) (*ketiv1.ControllerRevision, error)
	// DeleteControllerRevision attempts to delete revision. If the returned error is not nil, deletion has failed.
	DeleteControllerRevision(revision *ketiv1.ControllerRevision) error
	// UpdateControllerRevision updates revision such that its Revision is equal to newRevision. Implementations
	// may retry on conflict. If the returned error is nil, the update was successful and returned ControllerRevision
	// is valid. If the returned error is not nil, the update failed and the returned ControllerRevision is invalid.
	UpdateControllerRevision(revision *ketiv1.ControllerRevision, newRevision int64) (*ketiv1.ControllerRevision, error)
	// AdoptControllerRevision attempts to adopt revision by adding a ControllerRef indicating that the parent
	// Object of parentKind is the owner of revision. If revision is already owned, an error is returned. If the
	// resource patch fails, an error is returned. If no error is returned, the returned ControllerRevision is
	// valid.
	AdoptControllerRevision(parent metav1.Object, parentKind schema.GroupVersionKind, revision *ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error)
	// ReleaseControllerRevision attempts to release parent's ownership of revision by removing parent from the
	// OwnerReferences of revision. If an error is returned, parent remains the owner of revision. If no error is
	// returned, the returned ControllerRevision is valid.
	ReleaseControllerRevision(parent metav1.Object, revision *ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error)
}
func NewControllerRevision(parent metav1.Object,
	parentKind schema.GroupVersionKind,
	templateLabels map[string]string,
	data runtime.RawExtension,
	revision int64,
	collisionCount *int32) (*ketiv1.ControllerRevision, error) {
	labelMap := make(map[string]string)
	for k, v := range templateLabels {
		labelMap[k] = v
	}
	cr := &ketiv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Labels:          labelMap,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(parent, parentKind)},
		},
		Data:     data,
		Revision: revision,
	}
	hash := HashControllerRevision(cr, collisionCount)
	cr.Name = ControllerRevisionName(parent.GetName(), hash)
	cr.Labels[ControllerRevisionHashLabel] = hash
	return cr, nil
}
// NewHistory returns an instance of Interface that uses client to communicate with the API Server and lister to list
// ControllerRevisions. This method should be used to create an Interface for all scenarios other than testing.
func NewHistory(client keticlient.KetiV1Interface, lister lister.ControllerRevisionLister) Interface {
	return &realHistory{client, lister}
}
// SortControllerRevisions sorts revisions by their Revision.
func SortControllerRevisions(revisions []*ketiv1.ControllerRevision) {
	sort.Stable(byRevision(revisions))
}

type realHistory struct {
	client keticlient.KetiV1Interface
	lister lister.ControllerRevisionLister
}

func (rh *realHistory) ListControllerRevisions(parent metav1.Object, selector labels.Selector) ([]*ketiv1.ControllerRevision, error) {
	// List all revisions in the namespace that match the selector
	history, err := rh.lister.ControllerRevisions(parent.GetNamespace()).List(selector)
	if err != nil {
		return nil, err
	}
	var owned []*ketiv1.ControllerRevision
	for i := range history {
		ref := metav1.GetControllerOfNoCopy(history[i])
		if ref == nil || ref.UID == parent.GetUID() {
			owned = append(owned, history[i])
		}

	}
	return owned, err
}

func (rh *realHistory) CreateControllerRevision(parent metav1.Object, revision *ketiv1.ControllerRevision, collisionCount *int32) (*ketiv1.ControllerRevision, error) {
	if collisionCount == nil {
		return nil, fmt.Errorf("collisionCount should not be nil")
	}

	// Clone the input
	clone := revision.DeepCopy()

	// Continue to attempt to create the revision updating the name with a new hash on each iteration
	for {
		hash := HashControllerRevision(revision, collisionCount)
		// Update the revisions name
		clone.Name = ControllerRevisionName(parent.GetName(), hash)
		ns := parent.GetNamespace()
		created, err := rh.client.ControllerRevisions(ns).Create(clone)
		if errors.IsAlreadyExists(err) {
			exists, err := rh.client.ControllerRevisions(ns).Get(clone.Name, metav1.GetOptions{})
			if err != nil {
				return nil, err
			}
			if bytes.Equal(exists.Data.Raw, clone.Data.Raw) {
				return exists, nil
			}
			*collisionCount++
			continue
		}
		return created, err
	}
}
// ControllerRevisionName returns the Name for a ControllerRevision in the form prefix-hash. If the length
// of prefix is greater than 223 bytes, it is truncated to allow for a name that is no larger than 253 bytes.
func ControllerRevisionName(prefix string, hash string) string {
	if len(prefix) > 223 {
		prefix = prefix[:223]
	}

	return fmt.Sprintf("%s-%s", prefix, hash)
}
// HashControllerRevision hashes the contents of revision's Data using FNV hashing. If probe is not nil, the byte value
// of probe is added written to the hash as well. The returned hash will be a safe encoded string to avoid bad words.
func HashControllerRevision(revision *ketiv1.ControllerRevision, probe *int32) string {
	hf := fnv.New32()
	if len(revision.Data.Raw) > 0 {
		hf.Write(revision.Data.Raw)
	}
	if revision.Data.Object != nil {
		hashutil.DeepHashObject(hf, revision.Data.Object)
	}
	if probe != nil {
		hf.Write([]byte(strconv.FormatInt(int64(*probe), 10)))
	}
	return rand.SafeEncodeString(fmt.Sprint(hf.Sum32()))
}

func (rh *realHistory) UpdateControllerRevision(revision *ketiv1.ControllerRevision, newRevision int64) (*ketiv1.ControllerRevision, error) {
	clone := revision.DeepCopy()
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		if clone.Revision == newRevision {
			return nil
		}
		clone.Revision = newRevision
		updated, updateErr := rh.client.ControllerRevisions(clone.Namespace).Update(clone)
		if updateErr == nil {
			return nil
		}
		if updated != nil {
			clone = updated
		}
		if updated, err := rh.lister.ControllerRevisions(clone.Namespace).Get(clone.Name); err == nil {
			// make a copy so we don't mutate the shared cache
			clone = updated.DeepCopy()
		}
		return updateErr
	})
	return clone, err
}

func (rh *realHistory) DeleteControllerRevision(revision *ketiv1.ControllerRevision) error {
	return rh.client.ControllerRevisions(revision.Namespace).Delete(revision.Name, nil)
}

type objectForPatch struct {
	Metadata objectMetaForPatch `json:"metadata"`
}

// objectMetaForPatch define object meta struct for patch operation
type objectMetaForPatch struct {
	OwnerReferences []metav1.OwnerReference `json:"ownerReferences"`
	UID             types.UID               `json:"uid"`
}

func (rh *realHistory) AdoptControllerRevision(parent metav1.Object, parentKind schema.GroupVersionKind, revision *ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error) {
	blockOwnerDeletion := true
	isController := true
	// Return an error if the parent does not own the revision
	if owner := metav1.GetControllerOfNoCopy(revision); owner != nil {
		return nil, fmt.Errorf("attempt to adopt revision owned by %v", owner)
	}
	addControllerPatch := objectForPatch{
		Metadata: objectMetaForPatch{
			UID: revision.UID,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         parentKind.GroupVersion().String(),
				Kind:               parentKind.Kind,
				Name:               parent.GetName(),
				UID:                parent.GetUID(),
				Controller:         &isController,
				BlockOwnerDeletion: &blockOwnerDeletion,
			}},
		},
	}
	patchBytes, err := json.Marshal(&addControllerPatch)
	if err != nil {
		return nil, err
	}
	// Use strategic merge patch to add an owner reference indicating a controller ref
	return rh.client.ControllerRevisions(parent.GetNamespace()).Patch(revision.GetName(),
		types.StrategicMergePatchType, patchBytes)
}

func (rh *realHistory) ReleaseControllerRevision(parent metav1.Object, revision *ketiv1.ControllerRevision) (*ketiv1.ControllerRevision, error) {
	// Use strategic merge patch to add an owner reference indicating a controller ref
	released, err := rh.client.ControllerRevisions(revision.GetNamespace()).Patch(revision.GetName(),
		types.StrategicMergePatchType,
		[]byte(fmt.Sprintf(`{"metadata":{"ownerReferences":[{"$patch":"delete","uid":"%s"}],"uid":"%s"}}`, parent.GetUID(), revision.UID)))

	if err != nil {
		if errors.IsNotFound(err) {
			// We ignore deleted revisions
			return nil, nil
		}
		if errors.IsInvalid(err) {
			// We ignore cases where the parent no longer owns the revision or where the revision has no
			// owner.
			return nil, nil
		}
	}
	return released, err
}