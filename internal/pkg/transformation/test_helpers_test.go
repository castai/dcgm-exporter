package transformation

import (
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

// testInformerForDRA is a minimal cache.SharedIndexInformer implementation
// that wraps an in-memory cache.Indexer for unit tests. It avoids the need
// for a real Kubernetes API server.
type testInformerForDRA struct {
	cache.SharedIndexInformer
	indexer cache.Indexer
}

func newTestInformerForDRA() *testInformerForDRA {
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		"poolName": func(obj interface{}) ([]string, error) {
			switch rs := obj.(type) {
			case *resourcev1.ResourceSlice:
				return []string{rs.Spec.Pool.Name}, nil
			case *resourcev1beta1.ResourceSlice:
				return []string{rs.Spec.Pool.Name}, nil
			}
			return nil, nil
		},
	})
	return &testInformerForDRA{indexer: indexer}
}

func (t *testInformerForDRA) GetIndexer() cache.Indexer {
	return t.indexer
}

func (t *testInformerForDRA) HasSynced() bool {
	return true
}

func (t *testInformerForDRA) AddEventHandler(_ cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (t *testInformerForDRA) AddEventHandlerWithResyncPeriod(_ cache.ResourceEventHandler, _ time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return nil, nil
}

func (t *testInformerForDRA) AddIndexers(_ cache.Indexers) error {
	return nil
}

func (t *testInformerForDRA) Run(_ <-chan struct{}) {}

func (t *testInformerForDRA) Add(objs ...runtime.Object) error {
	for _, obj := range objs {
		if err := t.indexer.Add(obj); err != nil {
			return err
		}
	}
	return nil
}
