package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1 "k8s.io/api/core/v1"
)

// +genclient
// +genclient:method=GetEphemeralContainers,verb=get,subresource=ephemeralcontainers,result=EphemeralContainers
// +genclient:method=UpdateEphemeralContainers,verb=update,subresource=ephemeralcontainers,input=EphemeralContainers,result=EphemeralContainers
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pod is the Schema for the pods API
// +kubebuilder:subresource:status
// +kubebuilder:resource:path=pods,scope=Namespaced
type Pod corev1.Pod


// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodList contains a list of Pod
type PodList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Pod `json:"items"`
}

type PodTemplateSpec corev1.PodTemplateSpec

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTemplate describes a template for creating copies of a predefined pod.
type PodTemplate corev1.PodTemplate

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTemplateList is a list of PodTemplates.
type PodTemplateList struct {
	metav1.TypeMeta `json:",inline"`
	// Standard list metadata.
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// List of pod templates
	Items []PodTemplate `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodLogOptions is the query options for a Pod's logs REST call.
type PodLogOptions struct {
	metav1.TypeMeta `json:",inline"`

	// The container for which to stream logs. Defaults to only container if there is one container in the pod.
	// +optional
	Container string `json:"container,omitempty" protobuf:"bytes,1,opt,name=container"`
	// Follow the log stream of the pod. Defaults to false.
	// +optional
	Follow bool `json:"follow,omitempty" protobuf:"varint,2,opt,name=follow"`
	// Return previous terminated container logs. Defaults to false.
	// +optional
	Previous bool `json:"previous,omitempty" protobuf:"varint,3,opt,name=previous"`
	// A relative time in seconds before the current time from which to show logs. If this value
	// precedes the time a pod was started, only logs since the pod start will be returned.
	// If this value is in the future, no logs will be returned.
	// Only one of sinceSeconds or sinceTime may be specified.
	// +optional
	SinceSeconds *int64 `json:"sinceSeconds,omitempty" protobuf:"varint,4,opt,name=sinceSeconds"`
	// An RFC3339 timestamp from which to show logs. If this value
	// precedes the time a pod was started, only logs since the pod start will be returned.
	// If this value is in the future, no logs will be returned.
	// Only one of sinceSeconds or sinceTime may be specified.
	// +optional
	SinceTime *metav1.Time `json:"sinceTime,omitempty" protobuf:"bytes,5,opt,name=sinceTime"`
	// If true, add an RFC3339 or RFC3339Nano timestamp at the beginning of every line
	// of log output. Defaults to false.
	// +optional
	Timestamps bool `json:"timestamps,omitempty" protobuf:"varint,6,opt,name=timestamps"`
	// If set, the number of lines from the end of the logs to show. If not specified,
	// logs are shown from the creation of the container or sinceSeconds or sinceTime
	// +optional
	TailLines *int64 `json:"tailLines,omitempty" protobuf:"varint,7,opt,name=tailLines"`
	// If set, the number of bytes to read from the server before terminating the
	// log output. This may not display a complete final line of logging, and may return
	// slightly more or slightly less than the specified limit.
	// +optional
	LimitBytes *int64 `json:"limitBytes,omitempty" protobuf:"varint,8,opt,name=limitBytes"`

	// insecureSkipTLSVerifyBackend indicates that the apiserver should not confirm the validity of the
	// serving certificate of the backend it is connecting to.  This will make the HTTPS connection between the apiserver
	// and the backend insecure. This means the apiserver cannot verify the log data it is receiving came from the real
	// kubelet.  If the kubelet is configured to verify the apiserver's TLS credentials, it does not mean the
	// connection to the real kubelet is vulnerable to a man in the middle attack (e.g. an attacker could not intercept
	// the actual log data coming from the real kubelet).
	// +optional
	InsecureSkipTLSVerifyBackend bool `json:"insecureSkipTLSVerifyBackend,omitempty" protobuf:"varint,9,opt,name=insecureSkipTLSVerifyBackend"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodExecOptions is the query options to a Pod's remote exec call.
// ---
// TODO: This is largely identical to PodAttachOptions above, make sure they stay in sync and see about merging
// and also when we cut V2, we should export a "StreamOptions" or somesuch that contains Stdin, Stdout, Stder and TTY
type PodExecOptions struct {
	metav1.TypeMeta `json:",inline"`

	// Redirect the standard input stream of the pod for this call.
	// Defaults to false.
	// +optional
	Stdin bool `json:"stdin,omitempty" protobuf:"varint,1,opt,name=stdin"`

	// Redirect the standard output stream of the pod for this call.
	// Defaults to true.
	// +optional
	Stdout bool `json:"stdout,omitempty" protobuf:"varint,2,opt,name=stdout"`

	// Redirect the standard error stream of the pod for this call.
	// Defaults to true.
	// +optional
	Stderr bool `json:"stderr,omitempty" protobuf:"varint,3,opt,name=stderr"`

	// TTY if true indicates that a tty will be allocated for the exec call.
	// Defaults to false.
	// +optional
	TTY bool `json:"tty,omitempty" protobuf:"varint,4,opt,name=tty"`

	// Container in which to execute the command.
	// Defaults to only container if there is only one container in the pod.
	// +optional
	Container string `json:"container,omitempty" protobuf:"bytes,5,opt,name=container"`

	// Command is the remote command to execute. argv array. Not executed within a shell.
	Command []string `json:"command" protobuf:"bytes,6,rep,name=command"`
}