package observer

import (
	"context"
	"fmt"
	"strings"
	"time"

	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apiextensionsv1beta1informer "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions/apiextensions/v1beta1"
	apiextensionsv1beta1lister "k8s.io/apiextensions-apiserver/pkg/client/listers/apiextensions/v1beta1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog"

	"github.com/openshift/library-go/pkg/operator/events"
)

const (
	configSuffix           = ".config.openshift.io"
	controllerWorkQueueKey = "key"
)

var defaultResyncDuration = 5 * time.Minute

type ConfigObserverController struct {
	cachesToSync []cache.InformerSynced
	queue        workqueue.RateLimitingInterface
	recorder     events.Recorder

	crdLister       apiextensionsv1beta1lister.CustomResourceDefinitionLister
	crdInformer     cache.SharedIndexInformer
	dynamicClient   dynamic.Interface
	cachedDiscovery discovery.CachedDiscoveryInterface

	openshiftConfigObservers []*openshiftConfigObserver
	storage                  *GitStorage
}

func NewConfigObserverController(config *rest.Config) (*ConfigObserverController, error) {
	client, err := dynamic.NewForConfig(config)
	if err != nil {
		return nil, err
	}
	kubeClient, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	dc, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, err
	}

	storage, err := NewGitStorage()
	if err != nil {
		return nil, err
	}

	c := &ConfigObserverController{
		dynamicClient: client,
		queue:         workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "ConfigObserverController"),
		crdInformer:   apiextensionsv1beta1informer.NewCustomResourceDefinitionInformer(kubeClient, defaultResyncDuration, cache.Indexers{}),
		storage:       storage,
	}

	c.cachedDiscovery = memory.NewMemCacheClient(dc)
	c.crdLister = apiextensionsv1beta1lister.NewCustomResourceDefinitionLister(c.crdInformer.GetIndexer())
	c.crdInformer.AddEventHandler(c.eventHandler())
	c.cachesToSync = append(c.cachesToSync, c.crdInformer.HasSynced)

	return c, nil
}

func (c *ConfigObserverController) currentOpenShiftConfigResourceKinds() ([]schema.GroupVersionKind, error) {
	observedCrds, err := c.crdLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	currentConfigResources := []schema.GroupVersionKind{}
	for _, crd := range observedCrds {
		if !strings.HasSuffix(crd.GetName(), configSuffix) {
			continue
		}
		for _, version := range crd.Spec.Versions {
			if !version.Served {
				continue
			}
			currentConfigResources = append(currentConfigResources, schema.GroupVersionKind{
				Group:   crd.Name,
				Version: crd.Spec.Versions[0].Name,
				Kind:    crd.Spec.Names.Kind,
			})
		}
	}
	return currentConfigResources, nil
}

func (c *ConfigObserverController) sync() error {
	current, err := c.currentOpenShiftConfigResourceKinds()
	if err != nil {
		return err
	}

	// TODO: Handle CRD delete case
	kindNeedObserver := []schema.GroupVersionKind{}
	for _, configKind := range current {
		hasObserver := false
		for _, o := range c.openshiftConfigObservers {
			if o.isKind(configKind) {
				hasObserver = true
				break
			}
		}
		if !hasObserver {
			kindNeedObserver = append(kindNeedObserver, configKind)
		}
	}

	waitForCacheSyncFn := []cache.InformerSynced{}
	if len(kindNeedObserver) > 0 {
		c.cachedDiscovery.Invalidate()
		gr, err := restmapper.GetAPIGroupResources(c.cachedDiscovery)
		if err != nil {
			return err
		}
		mapper := restmapper.NewDiscoveryRESTMapper(gr)
		for _, kind := range kindNeedObserver {
			mapping, err := mapper.RESTMapping(kind.GroupKind(), kind.Version)
			if err != nil {
				// better luck next time
				continue
			}
			// we got mapping, lets run the observer
			observer := newOpenShiftConfigObserver(kind.Kind, mapping.Resource, c.dynamicClient, c.storage.eventHandler())
			waitForCacheSyncFn = append(waitForCacheSyncFn, observer.hasInformerCacheSynced)
			go observer.run(context.TODO())
			c.openshiftConfigObservers = append(c.openshiftConfigObservers, observer)
		}
	}

	if !cache.WaitForCacheSync(context.TODO().Done(), waitForCacheSyncFn...) {
		return fmt.Errorf("failed to wait for cache sync, will requeue")
	}

	return nil
}

// eventHandler queues the operator to check spec and status
func (c *ConfigObserverController) eventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.queue.Add(controllerWorkQueueKey) },
		UpdateFunc: func(old, new interface{}) { c.queue.Add(controllerWorkQueueKey) },
		DeleteFunc: func(obj interface{}) { c.queue.Add(controllerWorkQueueKey) },
	}
}

// Run starts the kube-apiserver and blocks until stopCh is closed.
func (c *ConfigObserverController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	klog.Infof("Starting ConfigObserver")
	defer klog.Infof("Shutting down ConfigObserver")

	if !cache.WaitForCacheSync(stopCh, c.cachesToSync...) {
		return
	}

	// doesn't matter what workers say, only start one.
	go wait.Until(c.runWorker, time.Second, stopCh)

	<-stopCh
}

func (c *ConfigObserverController) runWorker() {
	for c.processNextWorkItem() {
	}
}

func (c *ConfigObserverController) processNextWorkItem() bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.sync()
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}
