package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	resourcev1 "k8s.io/api/resource/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	resourcelistersv1 "k8s.io/client-go/listers/resource/v1"
	resourcelistersv1beta1 "k8s.io/client-go/listers/resource/v1beta1"
	"k8s.io/client-go/tools/cache"
)

func strPtr(s string) *string { return &s }

func TestDetectResourceSliceAPIVersion(t *testing.T) {
	tests := map[string]struct {
		apiResources []*metav1.APIResourceList
		expected     resourceSliceAPIVersion
	}{
		"v1 available": {
			apiResources: []*metav1.APIResourceList{
				{
					GroupVersion: resourcev1.SchemeGroupVersion.String(),
					APIResources: []metav1.APIResource{
						{Name: "resourceslices"},
					},
				},
			},
			expected: resourceSliceAPIV1,
		},
		"v1beta1 available": {
			apiResources: []*metav1.APIResourceList{
				{
					GroupVersion: resourcev1beta1.SchemeGroupVersion.String(),
					APIResources: []metav1.APIResource{
						{Name: "resourceslices"},
					},
				},
			},
			expected: resourceSliceAPIV1beta1,
		},
		"both available prefers v1": {
			apiResources: []*metav1.APIResourceList{
				{
					GroupVersion: resourcev1.SchemeGroupVersion.String(),
					APIResources: []metav1.APIResource{
						{Name: "resourceslices"},
					},
				},
				{
					GroupVersion: resourcev1beta1.SchemeGroupVersion.String(),
					APIResources: []metav1.APIResource{
						{Name: "resourceslices"},
					},
				},
			},
			expected: resourceSliceAPIV1,
		},
		"neither available": {
			apiResources: []*metav1.APIResourceList{},
			expected:     resourceSliceAPIUnknown,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.Fake.Resources = tc.apiResources
			got := detectResourceSliceAPIVersion(client)
			assert.Equal(t, tc.expected, got)
		})
	}
}

func TestBuildDeviceMapping(t *testing.T) {
	tests := map[string]struct {
		deviceType string
		uuid       string
		parentUUID string
		profile    string
		wantKey    string
		wantMIG    *DRAMigDeviceInfo
	}{
		"gpu": {
			deviceType: "gpu",
			uuid:       "GPU-aaaa-bbbb",
			wantKey:    "GPU-aaaa-bbbb",
			wantMIG:    nil,
		},
		"mig": {
			deviceType: "mig",
			uuid:       "MIG-1111",
			parentUUID: "GPU-parent-1",
			profile:    "1g.12gb",
			wantKey:    "GPU-parent-1",
			wantMIG: &DRAMigDeviceInfo{
				MIGDeviceUUID: "MIG-1111",
				Profile:       "1g.12gb",
				ParentUUID:    "GPU-parent-1",
			},
		},
		"mig missing parent": {
			deviceType: "mig",
			uuid:       "MIG-2222",
			parentUUID: "",
			profile:    "2g.24gb",
			wantKey:    "",
			wantMIG:    nil,
		},
		"unknown type": {
			deviceType: "tpu",
			uuid:       "TPU-xxxx",
			wantKey:    "",
			wantMIG:    nil,
		},
		"empty type": {
			deviceType: "",
			uuid:       "GPU-yyyy",
			wantKey:    "",
			wantMIG:    nil,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			key, mig := buildDeviceMapping(tc.deviceType, tc.uuid, tc.parentUUID, tc.profile)
			assert.Equal(t, tc.wantKey, key)
			assert.Equal(t, tc.wantMIG, mig)
		})
	}
}

func newV1Indexer() cache.Indexer {
	return cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
}

func newV1beta1Indexer() cache.Indexer {
	return cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
}

func TestMakeV1Lookup(t *testing.T) {
	tests := map[string]struct {
		slices  []runtime.Object
		pool    string
		device  string
		wantKey string
		wantMIG *DRAMigDeviceInfo
	}{
		"gpu found": {
			slices: []runtime.Object{&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-1"},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1.ResourcePool{Name: "poolA"},
					Devices: []resourcev1.Device{{
						Name: "gpu-0",
						Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
							"type": {StringValue: strPtr("gpu")},
							"uuid": {StringValue: strPtr("GPU-aaaa-bbbb")},
						},
					}},
				},
			}},
			pool: "poolA", device: "gpu-0",
			wantKey: "GPU-aaaa-bbbb",
			wantMIG: nil,
		},
		"mig found": {
			slices: []runtime.Object{&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-mig"},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1.ResourcePool{Name: "poolB"},
					Devices: []resourcev1.Device{{
						Name: "mig-0",
						Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
							"type":       {StringValue: strPtr("mig")},
							"uuid":       {StringValue: strPtr("MIG-1111")},
							"profile":    {StringValue: strPtr("1g.12gb")},
							"parentUUID": {StringValue: strPtr("GPU-parent-1")},
						},
					}},
				},
			}},
			pool: "poolB", device: "mig-0",
			wantKey: "GPU-parent-1",
			wantMIG: &DRAMigDeviceInfo{
				MIGDeviceUUID: "MIG-1111",
				Profile:       "1g.12gb",
				ParentUUID:    "GPU-parent-1",
			},
		},
		"wrong pool": {
			slices: []runtime.Object{&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-2"},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1.ResourcePool{Name: "poolA"},
					Devices: []resourcev1.Device{{
						Name: "gpu-0",
						Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
							"type": {StringValue: strPtr("gpu")},
							"uuid": {StringValue: strPtr("GPU-xxxx")},
						},
					}},
				},
			}},
			pool: "poolZ", device: "gpu-0",
			wantKey: "",
		},
		"wrong driver": {
			slices: []runtime.Object{&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-3"},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: "other.driver.io",
					Pool:   resourcev1.ResourcePool{Name: "poolA"},
					Devices: []resourcev1.Device{{
						Name: "gpu-0",
						Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
							"type": {StringValue: strPtr("gpu")},
							"uuid": {StringValue: strPtr("GPU-wrong")},
						},
					}},
				},
			}},
			pool: "poolA", device: "gpu-0",
			wantKey: "",
		},
		"device not found": {
			slices: []runtime.Object{&resourcev1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-4"},
				Spec: resourcev1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1.ResourcePool{Name: "poolA"},
					Devices: []resourcev1.Device{{
						Name: "gpu-0",
						Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
							"type": {StringValue: strPtr("gpu")},
							"uuid": {StringValue: strPtr("GPU-yyyy")},
						},
					}},
				},
			}},
			pool: "poolA", device: "gpu-99",
			wantKey: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			indexer := newV1Indexer()
			for _, obj := range tc.slices {
				require.NoError(t, indexer.Add(obj))
			}
			lister := resourcelistersv1.NewResourceSliceLister(indexer)
			lookup := makeV1Lookup(lister)

			key, mig := lookup(tc.pool, tc.device)
			assert.Equal(t, tc.wantKey, key)
			assert.Equal(t, tc.wantMIG, mig)
		})
	}
}

func TestMakeV1beta1Lookup(t *testing.T) {
	tests := map[string]struct {
		slices  []runtime.Object
		pool    string
		device  string
		wantKey string
		wantMIG *DRAMigDeviceInfo
	}{
		"gpu found": {
			slices: []runtime.Object{&resourcev1beta1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-beta-1"},
				Spec: resourcev1beta1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1beta1.ResourcePool{Name: "poolC"},
					Devices: []resourcev1beta1.Device{{
						Name: "gpu-1",
						Basic: &resourcev1beta1.BasicDevice{
							Attributes: map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceAttribute{
								"type": {StringValue: strPtr("gpu")},
								"uuid": {StringValue: strPtr("GPU-cccc-dddd")},
							},
						},
					}},
				},
			}},
			pool: "poolC", device: "gpu-1",
			wantKey: "GPU-cccc-dddd",
			wantMIG: nil,
		},
		"mig found": {
			slices: []runtime.Object{&resourcev1beta1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-beta-mig"},
				Spec: resourcev1beta1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1beta1.ResourcePool{Name: "poolD"},
					Devices: []resourcev1beta1.Device{{
						Name: "mig-1",
						Basic: &resourcev1beta1.BasicDevice{
							Attributes: map[resourcev1beta1.QualifiedName]resourcev1beta1.DeviceAttribute{
								"type":       {StringValue: strPtr("mig")},
								"uuid":       {StringValue: strPtr("MIG-2222")},
								"profile":    {StringValue: strPtr("2g.24gb")},
								"parentUUID": {StringValue: strPtr("GPU-parent-2")},
							},
						},
					}},
				},
			}},
			pool: "poolD", device: "mig-1",
			wantKey: "GPU-parent-2",
			wantMIG: &DRAMigDeviceInfo{
				MIGDeviceUUID: "MIG-2222",
				Profile:       "2g.24gb",
				ParentUUID:    "GPU-parent-2",
			},
		},
		"nil basic": {
			slices: []runtime.Object{&resourcev1beta1.ResourceSlice{
				ObjectMeta: metav1.ObjectMeta{Name: "slice-nil-basic"},
				Spec: resourcev1beta1.ResourceSliceSpec{
					Driver: DRAGPUDriverName,
					Pool:   resourcev1beta1.ResourcePool{Name: "poolE"},
					Devices: []resourcev1beta1.Device{{
						Name:  "gpu-2",
						Basic: nil,
					}},
				},
			}},
			pool: "poolE", device: "gpu-2",
			wantKey: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			indexer := newV1beta1Indexer()
			for _, obj := range tc.slices {
				require.NoError(t, indexer.Add(obj))
			}
			lister := resourcelistersv1beta1.NewResourceSliceLister(indexer)
			lookup := makeV1beta1Lookup(lister)

			key, mig := lookup(tc.pool, tc.device)
			assert.Equal(t, tc.wantKey, key)
			assert.Equal(t, tc.wantMIG, mig)
		})
	}
}

func TestDRAResourceSliceManager_GetDeviceInfo_NilLookup(t *testing.T) {
	mgr := &DRAResourceSliceManager{lookup: nil}
	key, mig := mgr.GetDeviceInfo("poolA", "gpu-0")
	assert.Empty(t, key)
	assert.Nil(t, mig)
}

func TestResourceSliceAPIVersion_String(t *testing.T) {
	assert.Equal(t, resourcev1.SchemeGroupVersion.String(), resourceSliceAPIV1.String())
	assert.Equal(t, resourcev1beta1.SchemeGroupVersion.String(), resourceSliceAPIV1beta1.String())
	assert.Equal(t, "unknown", resourceSliceAPIUnknown.String())
}

// Suppress unused import warnings — labels is used by Listers internally
var _ = labels.Everything
