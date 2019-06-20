package observer

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/tools/cache"
)

type openshiftConfigObserver struct {
	groupVersionResource schema.GroupVersionResource
	kind                 string
	informer             cache.SharedIndexInformer
	hasSynced            cache.InformerSynced
}

func newOpenShiftConfigObserver(kind string, configResource schema.GroupVersionResource, client dynamic.Interface, resourceHandlers ...cache.ResourceEventHandler) *openshiftConfigObserver {
	observer := &openshiftConfigObserver{
		informer:             dynamicinformer.NewDynamicSharedInformerFactory(client, defaultResyncDuration).ForResource(configResource).Informer(),
		kind:                 kind,
		groupVersionResource: configResource,
	}
	observer.hasSynced = observer.informer.HasSynced
	for _, handler := range resourceHandlers {
		observer.informer.AddEventHandler(handler)
	}
	return observer
}

func (c openshiftConfigObserver) isKind(kind schema.GroupVersionKind) bool {
	return schema.GroupVersionKind{
		Group:   c.groupVersionResource.Group,
		Version: c.groupVersionResource.Version,
		Kind:    c.kind,
	} == kind
}

func (c openshiftConfigObserver) hasInformerCacheSynced() bool {
	return c.hasSynced()
}

func (c openshiftConfigObserver) run(ctx context.Context) {
	c.informer.Run(ctx.Done())
}
