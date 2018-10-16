/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"

	streamv1alpha1 "github.com/projectriff/stream-controller/pkg/apis/streamcontroller/v1alpha1"
	clientset "github.com/projectriff/stream-controller/pkg/client/clientset/versioned"
	informers "github.com/projectriff/stream-controller/pkg/client/informers/externalversions/streamcontroller/v1alpha1"
	listers "github.com/projectriff/stream-controller/pkg/client/listers/streamcontroller/v1alpha1"

	channelv1alpha1 "github.com/knative/eventing/pkg/apis/channels/v1alpha1"
	eventingclientset "github.com/knative/eventing/pkg/client/clientset/versioned"

	buildv1alpha1 "github.com/knative/build/pkg/apis/build/v1alpha1"

	servingv1alpha1 "github.com/knative/serving/pkg/apis/serving/v1alpha1"
	servingclientset "github.com/knative/serving/pkg/client/clientset/versioned"
	servinginformers "github.com/knative/serving/pkg/client/informers/externalversions/serving/v1alpha1"
	servinglisters "github.com/knative/serving/pkg/client/listers/serving/v1alpha1"
)

const controllerAgentName = "stream-controller"

const (
	// SuccessSynced is used as part of the Event 'reason' when a Stream is synced
	SuccessSynced = "Synced"
	// ErrResourceExists is used as part of the Event 'reason' when a Stream fails
	// to sync due to a Deployment of the same name already existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to a Deployment already existing
	MessageResourceExists = "Resource %q already exists and is not managed by Stream"
	// MessageResourceSynced is the message used for an Event fired when a Stream
	// is synced successfully
	MessageResourceSynced = "Stream synced successfully"
)

// StreamApp is a parsed app from the Stream definition
type StreamApp struct {
	Name string            `json:"name"`
	Args map[string]string `json:"args"`
}

// Controller is the controller implementation for Stream resources
type Controller struct {
	// kubeclientset is a standard kubernetes clientset
	kubeclientset kubernetes.Interface

	// clientset for Knative Serving
	servingclientset servingclientset.Interface

	// clientset for Knative Eventing
	eventingclientset eventingclientset.Interface

	// streamclientset is a clientset for our own API group
	streamclientset clientset.Interface

	servicesLister  servinglisters.ServiceLister
	functionsLister listers.FunctionLister
	streamsLister   listers.StreamLister
	streamsSynced   cache.InformerSynced

	// workqueue is a rate limited work queue. This is used to queue work to be
	// processed instead of performing it as soon as a change happens. This
	// means we can ensure we only process a fixed amount of resources at a
	// time, and makes it easy to ensure we are never processing the same item
	// simultaneously in two different workers.
	workqueue workqueue.RateLimitingInterface
	// recorder is an event recorder for recording Event resources to the
	// Kubernetes API.
	recorder record.EventRecorder
}

// NewController returns a new stream controller
func NewController(
	kubeclientset kubernetes.Interface,
	servingclientset servingclientset.Interface,
	eventingclientset eventingclientset.Interface,
	streamclientset clientset.Interface,
	streamInformer informers.StreamInformer,
	functionInformer informers.FunctionInformer,
	serviceInformer servinginformers.ServiceInformer) *Controller {

	// Create event broadcaster
	// Add stream-controller types to the default Kubernetes Scheme so Events can be
	// logged for stream-controller types.
	//? utilruntime.Must(streamscheme.AddToScheme(scheme.Scheme))
	glog.V(4).Info("Creating event broadcaster")
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(glog.Infof)
	eventBroadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: kubeclientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: controllerAgentName})

	controller := &Controller{
		kubeclientset:     kubeclientset,
		servingclientset:  servingclientset,
		eventingclientset: eventingclientset,
		streamclientset:   streamclientset,
		servicesLister:    serviceInformer.Lister(),
		functionsLister:   functionInformer.Lister(),
		streamsLister:     streamInformer.Lister(),
		streamsSynced:     streamInformer.Informer().HasSynced,
		workqueue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "Streams"),
		recorder:          recorder,
	}

	glog.Info("Setting up event handlers")
	// Set up an event handler for when Stream resources change
	streamInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: controller.enqueueStream,
		UpdateFunc: func(old, new interface{}) {
			controller.enqueueStream(new)
		},
	})

	return controller
}

// Run will set up the event handlers for types we are interested in, as well
// as syncing informer caches and starting workers. It will block until stopCh
// is closed, at which point it will shutdown the workqueue and wait for
// workers to finish processing their current work items.
func (c *Controller) Run(threadiness int, stopCh <-chan struct{}) error {
	defer runtime.HandleCrash()
	defer c.workqueue.ShutDown()

	// Start the informer factories to begin populating the informer caches
	glog.Info("Starting Stream controller")

	// Wait for the caches to be synced before starting workers
	glog.Info("Waiting for informer caches to sync")
	if ok := cache.WaitForCacheSync(stopCh, c.streamsSynced); !ok {
		return fmt.Errorf("failed to wait for caches to sync")
	}

	glog.Info("Starting workers")
	// Launch two workers to process Stream resources
	for i := 0; i < threadiness; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	glog.Info("Started workers")
	<-stopCh
	glog.Info("Shutting down workers")

	return nil
}

// runWorker is a long-running function that will continually call the
// processNextWorkItem function in order to read and process a message on the
// workqueue.
func (c *Controller) runWorker() {
	for c.processNextWorkItem() {
	}
}

// processNextWorkItem will read a single work item off the workqueue and
// attempt to process it, by calling the syncHandler.
func (c *Controller) processNextWorkItem() bool {
	obj, shutdown := c.workqueue.Get()

	if shutdown {
		return false
	}

	// We wrap this block in a func so we can defer c.workqueue.Done.
	err := func(obj interface{}) error {
		// We call Done here so the workqueue knows we have finished
		// processing this item. We also must remember to call Forget if we
		// do not want this work item being re-queued. For example, we do
		// not call Forget if a transient error occurs, instead the item is
		// put back on the workqueue and attempted again after a back-off
		// period.
		defer c.workqueue.Done(obj)
		var key string
		var ok bool
		// We expect strings to come off the workqueue. These are of the
		// form namespace/name. We do this as the delayed nature of the
		// workqueue means the items in the informer cache may actually be
		// more up to date that when the item was initially put onto the
		// workqueue.
		if key, ok = obj.(string); !ok {
			// As the item in the workqueue is actually invalid, we call
			// Forget here else we'd go into a loop of attempting to
			// process a work item that is invalid.
			c.workqueue.Forget(obj)
			runtime.HandleError(fmt.Errorf("expected string in workqueue but got %#v", obj))
			return nil
		}
		// Run the syncHandler, passing it the namespace/name string of the
		// Stream resource to be synced.
		if err := c.syncHandler(key); err != nil {
			return fmt.Errorf("error syncing '%s': %s", key, err.Error())
		}
		// Finally, if no error occurs we Forget this item so it does not
		// get queued again until another change happens.
		c.workqueue.Forget(obj)
		glog.Infof("Successfully synced '%s'", key)
		return nil
	}(obj)

	if err != nil {
		runtime.HandleError(err)
		return true
	}

	return true
}

// syncHandler compares the actual state with the desired, and attempts to
// converge the two. It then updates the Status block of the Stream resource
// with the current status of the resource.
func (c *Controller) syncHandler(key string) error {
	// Convert the namespace/name string into a distinct namespace and name
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		runtime.HandleError(fmt.Errorf("invalid resource key: %s", key))
		return nil
	}

	// Get the Stream resource with this namespace/name
	stream, err := c.streamsLister.Streams(namespace).Get(name)
	if err != nil {
		// The Stream resource may no longer exist, in which case we stop
		// processing.
		if errors.IsNotFound(err) {
			runtime.HandleError(fmt.Errorf("stream '%s' in work queue no longer exists", key))
			return nil
		}
		return err
	}

	definition := stream.Spec.Definition
	if definition == "" {
		return fmt.Errorf("%s: definition must be specified", key)
	}
	// call the Java stream-parser service
	req, err := http.NewRequest(http.MethodPost,
		"http://stream-parser.default.svc.cluster.local",
		bytes.NewBufferString(definition))
	if err != nil {
		return fmt.Errorf("unable to create request %v", err)
	}

	client := &http.Client{}
	res, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to invoke stream parser %v", err)
	}
	defer res.Body.Close()
	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("failed to read stream parser response %v", err)
	}
	apps := string(bytes)
	glog.Infof("parsed definition to apps: %s", apps)
	var streamApps []StreamApp
	err = json.Unmarshal(bytes, &streamApps)
	if err != nil {
		return err
	}

	var replyTo = "replies-channel"
	for i := len(streamApps) - 1; i >= 0; i-- {
		app := streamApps[i]
		functionName := app.Name
		glog.Infof("deploying function %s", functionName)
		function, err := c.functionsLister.Functions(namespace).Get(functionName)
		if err != nil {
			return err
		}
		image := function.Spec.Image
		repo := function.Spec.Repo
		artifact := function.Spec.Artifact
		ksvcName := generateResourceName(stream, functionName)

		existing, err := c.servingclientset.ServingV1alpha1().Services(namespace).Get(ksvcName, metav1.GetOptions{})
		if err != nil {
			if !errors.IsNotFound(err) {
				return err
			}
			// create the KService for the function
			kservice := newKnativeService(stream, functionName, artifact, app.Args, repo, image, namespace)
			_, err = c.servingclientset.ServingV1alpha1().Services(namespace).Create(kservice)
			if err != nil {
				return err
			}
			// create the input channel for the function
			channel := newChannel(stream, functionName, namespace)
			_, err = c.eventingclientset.ChannelsV1alpha1().Channels(namespace).Create(channel)
			if err != nil {
				if !errors.IsAlreadyExists(err) {
					return err
				}
				glog.Infof("channel %s already exists", channel.Name)
			}
			// create the Subscription for the function
			subscription := newSubscription(stream, channel.Name, replyTo, namespace)
			replyTo = fmt.Sprintf("%s-channel", channel.Name)
			_, err = c.eventingclientset.ChannelsV1alpha1().Subscriptions(namespace).Create(subscription)
			if err != nil {
				if !errors.IsAlreadyExists(err) {
					return err
				}
				glog.Infof("subscription %s already exists", channel.Name)
			}
			err = c.updateStreamStatus(stream)
			if err != nil {
				return err
			}
		} else {
			modified := existing.DeepCopy()
			modified.Spec.RunLatest.Configuration.RevisionTemplate.Spec.Container.Env = envFromArgs(app.Args)
			if !equality.Semantic.DeepEqual(existing, modified) {
				glog.Infof(fmt.Sprintf("updating Knative service: %s", ksvcName))
				serviceClient := c.servingclientset.ServingV1alpha1().Services(namespace)
				_, err = serviceClient.Update(modified)
				if err != nil {
					return err
				}
				glog.Infof(fmt.Sprintf("updated Knative service: %s", ksvcName))
			}
		}
	}

	// Finally, we update the status block of the Stream resource to reflect the
	// current state of the world
	err = c.updateStreamStatus(stream)
	if err != nil {
		return err
	}

	// TODO: figure out why this is not working
	//c.recorder.Event(stream, corev1.EventTypeNormal, SuccessSynced, MessageResourceSynced)
	return nil
}

func (c *Controller) updateStreamStatus(stream *streamv1alpha1.Stream) error {
	// NEVER modify objects from the store. It's a read-only, local cache.
	// You can use DeepCopy() to make a deep copy of original object and modify this copy
	// Or create a copy manually for better performance
	streamCopy := stream.DeepCopy()
	// TODO:
	streamCopy.Status.State = "Deployed"
	// If the CustomResourceSubresources feature gate is not enabled,
	// we must use Update instead of UpdateStatus to update the Status block of the Stream resource.
	// UpdateStatus will not allow changes to the Spec of the resource,
	// which is ideal for ensuring nothing other than resource status has been updated.
	_, err := c.streamclientset.ProjectriffV1alpha1().Streams(stream.Namespace).Update(streamCopy)
	return err
}

// enqueueStream takes a Stream resource and converts it into a namespace/name
// string which is then put onto the work queue. This method should *not* be
// passed resources of any type other than Stream.
func (c *Controller) enqueueStream(obj interface{}) {
	var key string
	var err error
	if key, err = cache.MetaNamespaceKeyFunc(obj); err != nil {
		runtime.HandleError(err)
		return
	}
	c.workqueue.AddRateLimited(key)
}

// handleObject will take any resource implementing metav1.Object and attempt
// to find the Stream resource that 'owns' it. It does this by looking at the
// objects metadata.ownerReferences field for an appropriate OwnerReference.
// It then enqueues that Stream resource to be processed. If the object does not
// have an appropriate OwnerReference, it will simply be skipped.
func (c *Controller) handleObject(obj interface{}) {
	var object metav1.Object
	var ok bool
	if object, ok = obj.(metav1.Object); !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object, invalid type"))
			return
		}
		object, ok = tombstone.Obj.(metav1.Object)
		if !ok {
			runtime.HandleError(fmt.Errorf("error decoding object tombstone, invalid type"))
			return
		}
		glog.V(4).Infof("Recovered deleted object '%s' from tombstone", object.GetName())
	}
	glog.V(4).Infof("Processing object: %s", object.GetName())
	if ownerRef := metav1.GetControllerOf(object); ownerRef != nil {
		// If this object is not owned by a Stream, we should not do anything more
		// with it.
		if ownerRef.Kind != "Stream" {
			return
		}

		stream, err := c.streamsLister.Streams(object.GetNamespace()).Get(ownerRef.Name)
		if err != nil {
			glog.V(4).Infof("ignoring orphaned object '%s' of stream '%s'", object.GetSelfLink(), ownerRef.Name)
			return
		}

		c.enqueueStream(stream)
		return
	}
}

// newKnativeService creates a new Knative Service for a Stream resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the Stream resource that 'owns' it.
func newKnativeService(stream *streamv1alpha1.Stream, name string, artifact string, args map[string]string, repo string, image string, namespace string) *servingv1alpha1.Service {
	// labels := map[string]string{
	// 	"app":        name,
	// 	"controller": stream.Name,
	// }
	resourceName := generateResourceName(stream, name)
	// var envVars []corev1.EnvVar
	// for k, v := range args {
	// 	envVars = append(envVars, corev1.EnvVar{
	// 		Name:  strings.ToUpper(k),
	// 		Value: v,
	// 	})
	// }
	return &servingv1alpha1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(stream, schema.GroupVersionKind{
					Group:   streamv1alpha1.SchemeGroupVersion.Group,
					Version: streamv1alpha1.SchemeGroupVersion.Version,
					Kind:    "Stream",
				}),
			},
		},
		Spec: servingv1alpha1.ServiceSpec{
			RunLatest: &servingv1alpha1.RunLatestType{
				Configuration: createConfigurationSpec(resourceName, artifact, args, repo, image, namespace),
			},
		},
	}
}

func createConfigurationSpec(resourceName string, artifact string, args map[string]string, repo string, image string, namespace string) servingv1alpha1.ConfigurationSpec {
	if repo == "" {
		return servingv1alpha1.ConfigurationSpec{
			RevisionTemplate: createRevisionTemplateSpec(resourceName, namespace, args, image),
		}
	}
	return servingv1alpha1.ConfigurationSpec{
		Build:            createBuild(resourceName, artifact, repo, image),
		RevisionTemplate: createRevisionTemplateSpec(resourceName, namespace, args, image),
	}
}

func createBuild(resourceName string, artifact string, repo string, image string) *buildv1alpha1.BuildSpec {
	return &buildv1alpha1.BuildSpec{
		ServiceAccountName: "riff-build",
		Source: &buildv1alpha1.SourceSpec{
			Git: &buildv1alpha1.GitSourceSpec{
				Revision: "master",
				Url:      repo,
			},
		},
		Template: &buildv1alpha1.TemplateInstantiationSpec{
			Name: "riff",
			Arguments: []buildv1alpha1.ArgumentSpec{
				buildv1alpha1.ArgumentSpec{
					Name:  "IMAGE",
					Value: image,
				},
				buildv1alpha1.ArgumentSpec{
					Name: "INVOKER_PATH",
					// TODO: make configurable
					Value: "https://github.com/projectriff/node-function-invoker/raw/v0.0.8/node-invoker.yaml",
				},
				buildv1alpha1.ArgumentSpec{
					Name:  "FUNCTION_ARTIFACT",
					Value: artifact,
				},
				buildv1alpha1.ArgumentSpec{
					Name:  "FUNCTION_HANDLER",
					Value: "",
				},
				buildv1alpha1.ArgumentSpec{
					Name:  "FUNCTION_NAME",
					Value: resourceName,
				},
			},
		},
	}
}

func createRevisionTemplateSpec(resourceName string, namespace string, args map[string]string, image string) servingv1alpha1.RevisionTemplateSpec {
	return servingv1alpha1.RevisionTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName,
			Namespace: namespace,
		},
		Spec: servingv1alpha1.RevisionSpec{
			Container: corev1.Container{
				Image: image,
				Env:   envFromArgs(args),
			},
		},
	}
}

func newChannel(stream *streamv1alpha1.Stream, functionName string, namespace string) *channelv1alpha1.Channel {
	//labels := map[string]string{
	//	"controller": stream.Name,
	//}
	return &channelv1alpha1.Channel{
		ObjectMeta: metav1.ObjectMeta{
			Name:      generateResourceName(stream, functionName),
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(stream, schema.GroupVersionKind{
					Group:   streamv1alpha1.SchemeGroupVersion.Group,
					Version: streamv1alpha1.SchemeGroupVersion.Version,
					Kind:    "Stream",
				}),
			},
		},
		Spec: channelv1alpha1.ChannelSpec{
			ClusterBus: "stub",
		},
	}
}

func newSubscription(stream *streamv1alpha1.Stream, channelName string, replyToChannelName string, namespace string) *channelv1alpha1.Subscription {
	return &channelv1alpha1.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name:      channelName,
			Namespace: namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(stream, schema.GroupVersionKind{
					Group:   streamv1alpha1.SchemeGroupVersion.Group,
					Version: streamv1alpha1.SchemeGroupVersion.Version,
					Kind:    "Stream",
				}),
			},
		},
		Spec: channelv1alpha1.SubscriptionSpec{
			Channel:    channelName,
			Subscriber: channelName,
			ReplyTo:    replyToChannelName,
		},
	}
}

func generateResourceName(stream *streamv1alpha1.Stream, functionName string) string {
	return fmt.Sprintf("%s-%s", stream.Name, functionName)
}

func envFromArgs(args map[string]string) []corev1.EnvVar {
	var envVars []corev1.EnvVar
	for k, v := range args {
		envVars = append(envVars, corev1.EnvVar{
			Name:  strings.ToUpper(k),
			Value: v,
		})
	}
	return envVars
}
