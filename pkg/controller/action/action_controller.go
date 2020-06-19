package action

import (
	ketiv1 "github.com/hth0919/migrationmanager/pkg/apis/keti/v1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/source"
)

var log = logf.Log.WithName("controller_action")

/**
* USER ACTION REQUIRED: This is a scaffold file intended for the user to modify with their own Controller
* business logic.  Delete these comments after modifying this file.*
 */

// Add creates a new Action Controller and adds it to the Manager. The Manager will set fields on the Controller
// and Start it when the Manager is Started.
func Add(mgr manager.Manager) error {
	return add(mgr, newReconciler(mgr))
}

// newReconciler returns a new reconcile.Reconciler
func newReconciler(mgr manager.Manager) reconcile.Reconciler {
	return &ReconcileAction{client: mgr.GetClient(), scheme: mgr.GetScheme()}
}

// add adds a new Controller to mgr with r as the reconcile.Reconciler
func add(mgr manager.Manager, r reconcile.Reconciler) error {
	// Create a new controller
	c, err := controller.New("action-controller", mgr, controller.Options{Reconciler: r})
	if err != nil {
		return err
	}

	// Watch for changes to primary resource Action
	err = c.Watch(&source.Kind{Type: &ketiv1.Action{}}, &handler.EnqueueRequestForObject{})
	if err != nil {
		return err
	}

	// TODO(user): Modify this to be the types you create that are owned by the primary resource
	// Watch for changes to secondary resource Pods and requeue the owner Action
	err = c.Watch(&source.Kind{Type: &corev1.Pod{}}, &handler.EnqueueRequestForOwner{
		IsController: true,
		OwnerType:    &ketiv1.Action{},
	})
	if err != nil {
		return err
	}

	return nil
}

// blank assignment to verify that ReconcileAction implements reconcile.Reconciler
var _ reconcile.Reconciler = &ReconcileAction{}

// ReconcileAction reconciles a Action object
type ReconcileAction struct {
	// This client, initialized using mgr.Client() above, is a split client
	// that reads objects from the cache and writes to the apiserver
	client client.Client
	scheme *runtime.Scheme
}

// Reconcile reads that state of the cluster for a Action object and makes changes based on the state read
// and what is in the Action.Spec
// TODO(user): Modify this Reconcile function to implement your Controller logic.  This example creates
// a Pod as an example
// Note:
// The Controller will requeue the Request to be processed again if the returned error is non-nil or
// Result.Requeue is true, otherwise upon completion it will remove the work from the queue.
func (r *ReconcileAction) Reconcile(request reconcile.Request) (reconcile.Result, error) {
	/*reqLogger := log.WithValues("Request.Namespace", request.Namespace, "Request.Name", request.Name)
	reqLogger.Info("Reconciling Action")

	// Fetch the Action instance
	instance := &ketiv1.Action{}
	err := r.client.Get(context.TODO(), request.NamespacedName, instance)
	if err != nil {
		if errors.IsNotFound(err) {
			// Request object not found, could have been deleted after reconcile request.
			// Owned objects are automatically garbage collected. For additional cleanup logic use finalizers.
			// Return and don't requeue
			return reconcile.Result{}, nil
		}
		// Error reading the object - requeue the request.
		return reconcile.Result{}, err
	}

	reqLogger.Info("Reconciling Migration",instance)
	if strings.Contains(instance.Spec.Purpose ,"Convert") || strings.Contains(instance.Spec.Purpose, "convert") {
		instance.Spec.Purpose = "convert"
		result, err := convertHandler(instance,r)
		if err != nil {
			reqLogger.Error(err, err.Error())
			return result, err
		}
	} else if strings.Contains(instance.Spec.Purpose, "Period") || strings.Contains(instance.Spec.Purpose, "period") || strings.Contains(instance.Spec.Purpose, "Period") {
		instance.Spec.Purpose = "period"
		result, err := checkpointHandler(instance,r)
		if err != nil {
			reqLogger.Error(err, err.Error())
			return result, err
		}
	} else if strings.Contains(instance.Spec.Purpose, "Migration") || strings.Contains(instance.Spec.Purpose, "migration") {
		instance.Spec.Purpose = "migration"
		result, err := migrationHandler(instance,r)
		if err != nil {
			reqLogger.Error(err, err.Error())
			return result, err
		}
	} else {
		reqLogger.Error(err, "Unsupported Value" + instance.Spec.Purpose + " in keti.Migration")
		return reconcile.Result{}, err
	}*/
	return reconcile.Result{}, nil
}/*
func convertHandler(m *ketiv1.Action, r *ReconcileAction) (reconcile.Result, error) {
	m.Spec.DestinationNode = m.Spec.Node
	found := &corev1.Pod{}
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	found, err = clientset.CoreV1().Pods(m.Spec.Namespace).Get(m.Spec.Podname, metav1.GetOptions{})
	out, err := yaml.Marshal(found)
	if err != nil {
		return reconcile.Result{}, err
	}
	klog.Infoln("your pod show in yaml file")
	klog.Infoln(string(out))
	klog.Infoln("(1/2)pod Copy complete, Creating checkpoint ")
	yamlstore(m.Spec.DestinationNode, out)
	checkpointcreate(m.Spec.DestinationNode, m.Spec.Podname)
	klog.Infoln("(2/2)Creating checkpoint Complete")
	return reconcile.Result{Requeue: true}, nil
}

func checkpointHandler(m *ketiv1.Action, r *ReconcileAction) (reconcile.Result, error) {
	period(m.Spec.Period)

	return reconcile.Result{Requeue: true}, nil
}

func migrationHandler(m *ketiv1.Action, r *ReconcileAction) (reconcile.Result, error) {
	found := &corev1.Pod{}
	err := r.client.Get(context.TODO(), types.NamespacedName{Name: m.Spec.Podname, Namespace: m.Spec.Namespace}, found)
	out, err := yaml.Marshal(found)
	if err != nil {
		return reconcile.Result{}, err
	}
	klog.Infoln("your pod show in yaml file")
	klog.Infoln(string(out))
	klog.Infoln("(1/2)pod Copy complete, Creating checkpoint ")

	//체크포인트 생성 코드
	yamlstore(m.Spec.DestinationNode, out)

	klog.Infoln("(2/2)Creating checkpoint Complete")
	klog.Infoln("your pod will migrated from",m.Spec.Node,"to",m.Spec.DestinationNode)


	return reconcile.Result{Requeue: true}, nil
}

func yamlstore(destnode string, out []byte){
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	node := &corev1.Node{}
	nodeIP := ""
	node, err = clientset.CoreV1().Nodes().Get(destnode,metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	for informer := 0;informer<len(node.Status.Addresses);informer++ {
		if node.Status.Addresses[informer].Type == "InternalIP" {
			nodeIP = node.Status.Addresses[informer].Address
		}
	}
	Host := nodeIP + ":20550"
	conn, err := grpc.Dial(Host, grpc.WithInsecure())
	if err != nil {
		klog.Errorln("did not connect: ", err)
	}

	defer conn.Close()
	c := NewCheckpointPeriodClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	in := &StoreValue{
		Yaml: out,
	}
	_ , err = c.StoreYaml(ctx, in)
	if err != nil {
		klog.Errorln("did not connect: ", err)
	}
}
func period(checkpointperiod int64){
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	nodeIP := ""
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}
	for j := 0;j<len(nodes.Items);j++ {
		node := nodes.Items[j]
		for informer := 0; informer < len(node.Status.Addresses); informer++ {
			if node.Status.Addresses[informer].Type == "InternalIP" {
				nodeIP = node.Status.Addresses[informer].Address
			}
		}
		klog.Infoln(node.Name,"Checkpoint collector Host:",nodeIP + ":10350")
		Host := nodeIP + ":20550"
		conn, err := grpc.Dial(Host, grpc.WithInsecure())
		if err != nil {
			klog.Errorln("did not connect: ", err)
		}

		defer conn.Close()
		c := NewCheckpointPeriodClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		in := &InputValue{
			Period:               &checkpointperiod,
			PodName:              nil,
		}
		_ , err = c.SetCheckpointPeriod(ctx, in)
		if err != nil {
			klog.Errorln("did not connect: ", err)
		}
	}
}
func checkpointcreate(destnode string, podname string){
	config, err := rest.InClusterConfig()
	if err != nil {
		panic(err.Error())
	}
	// creates the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}
	node := &corev1.Node{}
	nodeIP := ""
	node, err = clientset.CoreV1().Nodes().Get(destnode,metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}
	for informer := 0;informer<len(node.Status.Addresses);informer++ {
		if node.Status.Addresses[informer].Type == "InternalIP" {
			nodeIP = node.Status.Addresses[informer].Address
		}
	}
	Host := nodeIP + ":20550"
	conn, err := grpc.Dial(Host, grpc.WithInsecure())
	if err != nil {
		klog.Errorln("did not connect: ", err)
	}

	defer conn.Close()
	c := NewCheckpointPeriodClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	createctruct := &CreateCheckpoint{
		PodName:              &podname,
	}
	_ , err = c.CheckpointCreate(ctx, createctruct)
	if err != nil {
		klog.Errorln("did not connect: ", err)
	}
}*/