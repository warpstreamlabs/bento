package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"

	"github.com/warpstreamlabs/bento/public/service"
)

// Standard Kubernetes resources and their GVRs
var standardResources = map[string]schema.GroupVersionResource{
	"pods":                   {Group: "", Version: "v1", Resource: "pods"},
	"services":               {Group: "", Version: "v1", Resource: "services"},
	"configmaps":             {Group: "", Version: "v1", Resource: "configmaps"},
	"secrets":                {Group: "", Version: "v1", Resource: "secrets"},
	"namespaces":             {Group: "", Version: "v1", Resource: "namespaces"},
	"nodes":                  {Group: "", Version: "v1", Resource: "nodes"},
	"persistentvolumes":      {Group: "", Version: "v1", Resource: "persistentvolumes"},
	"persistentvolumeclaims": {Group: "", Version: "v1", Resource: "persistentvolumeclaims"},
	"deployments":            {Group: "apps", Version: "v1", Resource: "deployments"},
	"replicasets":            {Group: "apps", Version: "v1", Resource: "replicasets"},
	"statefulsets":           {Group: "apps", Version: "v1", Resource: "statefulsets"},
	"daemonsets":             {Group: "apps", Version: "v1", Resource: "daemonsets"},
	"jobs":                   {Group: "batch", Version: "v1", Resource: "jobs"},
	"cronjobs":               {Group: "batch", Version: "v1", Resource: "cronjobs"},
	"ingresses":              {Group: "networking.k8s.io", Version: "v1", Resource: "ingresses"},
	"networkpolicies":        {Group: "networking.k8s.io", Version: "v1", Resource: "networkpolicies"},
}

func kubernetesWatchInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services", "Kubernetes").
		Version("1.0.0").
		Summary("Watches Kubernetes resources for changes, similar to `kubectl get <resource> --watch`.").
		Description(`
This input watches Kubernetes resources and emits events when resources
are added, modified, or deleted. It supports both standard Kubernetes
resources and Custom Resource Definitions (CRDs).

### Standard Resources

For common resources, simply specify the resource name:
` + "```yaml" + `
input:
  kubernetes_watch:
    resource: pods
` + "```" + `

Supported standard resources: pods, services, configmaps, secrets,
deployments, replicasets, statefulsets, daemonsets, jobs, cronjobs,
ingresses, networkpolicies, nodes, namespaces, persistentvolumes,
persistentvolumeclaims.

### Custom Resources (CRDs)

For custom resources, specify the group, version, and resource:
` + "```yaml" + `
input:
  kubernetes_watch:
    custom_resource:
      group: stable.example.com
      version: v1
      resource: crontabs
` + "```" + `

### Watch Events

Each message includes the watch event type in metadata:
- **ADDED**: Resource was created
- **MODIFIED**: Resource was updated
- **DELETED**: Resource was removed
` + MetadataDescription([]string{
			"kubernetes_watch_event_type",
			"kubernetes_resource_kind",
			"kubernetes_resource_name",
			"kubernetes_resource_namespace",
			"kubernetes_resource_version",
			"kubernetes_resource_uid",
			"kubernetes_resource_creation_timestamp",
		})).
		Fields(AuthFields()...).
		Fields(CommonFields()...).
		Field(service.NewStringField("resource").
			Description("Standard Kubernetes resource type to watch.").
			Default("").
			Example("pods").
			Example("deployments").
			Example("configmaps")).
		Field(service.NewObjectField("custom_resource",
			service.NewStringField("group").
				Description("API group for the custom resource (e.g., 'stable.example.com').").
				Default(""),
			service.NewStringField("version").
				Description("API version for the custom resource (e.g., 'v1', 'v1beta1').").
				Default("v1"),
			service.NewStringField("resource").
				Description("Plural name of the custom resource (e.g., 'crontabs').").
				Default(""),
		).
			Description("Custom Resource Definition to watch. Use this for CRDs instead of 'resource'.").
			Optional().
			Advanced()).
		Field(service.NewStringListField("event_types").
			Description("Watch event types to include.").
			Default([]any{"ADDED", "MODIFIED", "DELETED"}).
			Example([]string{"ADDED", "DELETED"}).
			Example([]string{"MODIFIED"})).
		Field(service.NewBoolField("include_initial_list").
			Description("Emit ADDED events for all existing resources when starting.").
			Default(true)).
		LintRule(`
			let has_resource = this.resource.or("") != ""
			let has_custom = this.custom_resource.resource.or("") != ""
			root = if !$has_resource && !$has_custom {
				"either resource or custom_resource.resource must be specified"
			} else if $has_resource && $has_custom {
				"cannot specify both resource and custom_resource"
			}
		`)
}

func init() {
	err := service.RegisterInput(
		"kubernetes_watch", kubernetesWatchInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.Input, error) {
			return newKubernetesWatchInput(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

type watchEvent struct {
	eventType string
	object    *unstructured.Unstructured
}

type kubernetesWatchInput struct {
	clientSet *ClientSet
	log       *service.Logger

	// Configuration
	namespaces         []string
	labelSelector      string
	fieldSelector      string
	gvr                schema.GroupVersionResource
	eventTypes         map[string]struct{}
	includeInitialList bool
	requestTimeout     time.Duration

	// State
	mu           sync.Mutex
	eventChan    chan watchEvent
	shutSig      *shutdown.Signaller
	resourceVers map[string]string
}

func newKubernetesWatchInput(conf *service.ParsedConfig, mgr *service.Resources) (*kubernetesWatchInput, error) {
	k := &kubernetesWatchInput{
		log:          mgr.Logger(),
		eventChan:    make(chan watchEvent, 1000),
		shutSig:      shutdown.NewSignaller(),
		resourceVers: make(map[string]string),
	}

	var err error

	// Parse namespaces
	if k.namespaces, err = conf.FieldStringList("namespaces"); err != nil {
		return nil, err
	}

	// Parse selectors
	if k.labelSelector, err = conf.FieldString("label_selector"); err != nil {
		return nil, err
	}
	if k.fieldSelector, err = conf.FieldString("field_selector"); err != nil {
		return nil, err
	}

	// Determine GVR from resource or custom_resource
	resource, _ := conf.FieldString("resource")
	if resource != "" {
		gvr, ok := standardResources[resource]
		if !ok {
			return nil, fmt.Errorf("unknown resource type: %s", resource)
		}
		k.gvr = gvr
	} else {
		// Custom resource
		crConf := conf.Namespace("custom_resource")
		group, _ := crConf.FieldString("group")
		version, _ := crConf.FieldString("version")
		crResource, _ := crConf.FieldString("resource")

		if crResource == "" {
			return nil, errors.New("custom_resource.resource is required when using custom_resource")
		}

		k.gvr = schema.GroupVersionResource{
			Group:    group,
			Version:  version,
			Resource: crResource,
		}
	}

	// Parse event types filter
	eventTypesList, err := conf.FieldStringList("event_types")
	if err != nil {
		return nil, err
	}
	k.eventTypes = make(map[string]struct{})
	for _, et := range eventTypesList {
		k.eventTypes[et] = struct{}{}
	}

	// Parse behavior options
	if k.includeInitialList, err = conf.FieldBool("include_initial_list"); err != nil {
		return nil, err
	}
	requestTimeoutStr, err := conf.FieldString("request_timeout")
	if err != nil {
		return nil, err
	}
	if k.requestTimeout, err = time.ParseDuration(requestTimeoutStr); err != nil {
		return nil, fmt.Errorf("failed to parse request_timeout: %w", err)
	}

	// Get Kubernetes client
	if k.clientSet, err = GetClientSet(context.Background(), conf); err != nil {
		return nil, fmt.Errorf("failed to create kubernetes client: %w", err)
	}

	return k, nil
}

func (k *kubernetesWatchInput) getResourceVersion(namespace string) string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.resourceVers[namespace]
}

func (k *kubernetesWatchInput) setResourceVersion(namespace, version string) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if version == "" {
		delete(k.resourceVers, namespace)
		return
	}
	k.resourceVers[namespace] = version
}

func (k *kubernetesWatchInput) Connect(ctx context.Context) error {
	// Start watch loops for each namespace
	namespaces := k.namespaces
	if len(namespaces) == 0 {
		namespaces = []string{""} // Empty string = all namespaces
	}

	for _, ns := range namespaces {
		go k.watchNamespace(ns)
	}

	return nil
}

func (k *kubernetesWatchInput) watchNamespace(namespace string) {
	dynamicClient := k.clientSet.Dynamic
	retryAttempt := 0

	for {
		select {
		case <-k.shutSig.SoftStopChan():
			return
		default:
		}

		var resourceInterface dynamic.ResourceInterface
		if namespace == "" {
			resourceInterface = dynamicClient.Resource(k.gvr)
		} else {
			resourceInterface = dynamicClient.Resource(k.gvr).Namespace(namespace)
		}

		listOpts := metav1.ListOptions{
			LabelSelector: k.labelSelector,
			FieldSelector: k.fieldSelector,
		}

		// If include_initial_list is true on first run, list existing resources
		resourceVersion := k.getResourceVersion(namespace)
		if k.includeInitialList && resourceVersion == "" {
			k.listExistingResources(resourceInterface, namespace)
		}

		// Use stored resource version if available
		resourceVersion = k.getResourceVersion(namespace)
		if resourceVersion != "" {
			listOpts.ResourceVersion = resourceVersion
		}

		// Start watching
		watchCtx, watchCancel := requestContext(0, k.shutSig.SoftStopChan())
		watcher, err := resourceInterface.Watch(watchCtx, listOpts)
		if err != nil {
			watchCancel()
			// If the error is a "410 Gone" error, it means the resource version is too old.
			// Reset the resource version to force a fresh list on the next loop.
			if serr, ok := err.(*k8serrors.StatusError); ok && serr.ErrStatus.Code == http.StatusGone {
				k.log.Warnf("Watch for %s in namespace %s returned 410 Gone, resetting resource version", k.gvr.Resource, namespace)
				k.setResourceVersion(namespace, "")
			} else {
				k.log.Errorf("Failed to watch %s in namespace %s: %v", k.gvr.Resource, namespace, err)
			}
			backoff := calculateBackoff(retryAttempt)
			retryAttempt++
			select {
			case <-time.After(backoff):
			case <-k.shutSig.SoftStopChan():
				return
			}
			continue
		}

		// Reset retry counter on successful connection
		retryAttempt = 0
		k.processWatchEvents(watcher, namespace)
		watchCancel()
		watcher.Stop()
	}
}

func (k *kubernetesWatchInput) listExistingResources(resourceInterface dynamic.ResourceInterface, namespace string) {
	listOpts := metav1.ListOptions{
		LabelSelector: k.labelSelector,
		FieldSelector: k.fieldSelector,
		Limit:         500, // Process in chunks of 500
	}

	for {
		listCtx, cancel := requestContext(k.requestTimeout, k.shutSig.SoftStopChan())
		list, err := resourceInterface.List(listCtx, listOpts)
		cancel()
		if err != nil {
			k.log.Errorf("Failed to list %s: %v", k.gvr.Resource, err)
			return
		}

		// Store resource version for watch (update with latest)
		k.setResourceVersion(namespace, list.GetResourceVersion())

		// Check if ADDED events are filtered
		if _, ok := k.eventTypes["ADDED"]; ok {
			for i := range list.Items {
				item := &list.Items[i]
				select {
				case k.eventChan <- watchEvent{eventType: "ADDED", object: item}:
				case <-k.shutSig.SoftStopChan():
					return
				}
			}
		}

		// Check if there are more items
		continueToken := list.GetContinue()
		if continueToken == "" {
			break
		}
		listOpts.Continue = continueToken
	}
}

func (k *kubernetesWatchInput) processWatchEvents(watcher watch.Interface, namespace string) {
	for {
		select {
		case <-k.shutSig.SoftStopChan():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				// Watch channel closed
				return
			}

			switch event.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				obj, ok := event.Object.(*unstructured.Unstructured)
				if !ok {
					continue
				}

				// Store resource version
				k.setResourceVersion(namespace, obj.GetResourceVersion())

				// Check event type filter
				eventType := string(event.Type)
				if _, ok := k.eventTypes[eventType]; !ok {
					continue
				}

				select {
				case k.eventChan <- watchEvent{eventType: eventType, object: obj}:
				case <-k.shutSig.SoftStopChan():
					return
				}

			case watch.Bookmark:
				if obj, ok := event.Object.(*unstructured.Unstructured); ok {
					k.setResourceVersion(namespace, obj.GetResourceVersion())
				}

			case watch.Error:
				if status, ok := event.Object.(*metav1.Status); ok && status.Code == http.StatusGone {
					k.log.Warnf("Watch for %s in namespace %s returned 410 Gone, resetting resource version", k.gvr.Resource, namespace)
					k.setResourceVersion(namespace, "")
					return
				}
				k.log.Errorf("Watch error for %s in namespace %s: %v",
					k.gvr.Resource, namespace, event.Object)
				return
			}
		}
	}
}

func (k *kubernetesWatchInput) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	select {
	case event := <-k.eventChan:
		return k.eventToMessage(event)
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-k.shutSig.SoftStopChan():
		return nil, nil, service.ErrEndOfInput
	}
}

func (k *kubernetesWatchInput) eventToMessage(event watchEvent) (*service.Message, service.AckFunc, error) {
	obj := event.object

	// Serialize object to JSON
	objJSON, err := json.Marshal(obj.Object)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal object: %w", err)
	}

	msg := service.NewMessage(objJSON)

	// Add metadata
	msg.MetaSetMut("kubernetes_watch_event_type", event.eventType)
	msg.MetaSetMut("kubernetes_resource_kind", obj.GetKind())
	msg.MetaSetMut("kubernetes_resource_name", obj.GetName())
	msg.MetaSetMut("kubernetes_resource_namespace", obj.GetNamespace())
	msg.MetaSetMut("kubernetes_resource_version", obj.GetResourceVersion())
	msg.MetaSetMut("kubernetes_resource_uid", string(obj.GetUID()))

	creationTS := obj.GetCreationTimestamp()
	if !creationTS.Time.IsZero() {
		msg.MetaSetMut("kubernetes_resource_creation_timestamp",
			creationTS.Format(time.RFC3339))
	}

	// Add labels as metadata
	for key, value := range obj.GetLabels() {
		msg.MetaSetMut(getMetaKey("kubernetes_labels_", key), value)
	}

	// Add annotations as metadata (selected common ones)
	annotations := obj.GetAnnotations()
	for key, value := range annotations {
		msg.MetaSetMut(getMetaKey("kubernetes_annotations_", key), value)
	}

	return msg, func(ctx context.Context, err error) error {
		return nil // Watch events don't require acknowledgment
	}, nil
}

func (k *kubernetesWatchInput) Close(ctx context.Context) error {
	k.shutSig.TriggerSoftStop()
	k.shutSig.TriggerHasStopped()
	return nil
}
