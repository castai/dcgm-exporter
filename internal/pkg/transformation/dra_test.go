package transformation

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	resourcev1 "k8s.io/api/resource/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"
)

func strPtr(s string) *string { return &s }

func TestGetDeviceInfo_V1_GPU(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1.ResourceSlice{
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
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}
	uuid, migInfo := mgr.GetDeviceInfo("poolA", "gpu-0")
	assert.Equal(t, "GPU-aaaa-bbbb", uuid)
	assert.Nil(t, migInfo)
}

func TestGetDeviceInfo_V1_MIG(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1.ResourceSlice{
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
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}
	uuid, migInfo := mgr.GetDeviceInfo("poolB", "mig-0")
	assert.Equal(t, "GPU-parent-1", uuid)
	require.NotNil(t, migInfo)
	assert.Equal(t, "MIG-1111", migInfo.MIGDeviceUUID)
	assert.Equal(t, "1g.12gb", migInfo.Profile)
	assert.Equal(t, "GPU-parent-1", migInfo.ParentUUID)
}

func TestGetDeviceInfo_V1beta1_GPU(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1beta1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-beta"},
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
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1beta1"}
	uuid, migInfo := mgr.GetDeviceInfo("poolC", "gpu-1")
	assert.Equal(t, "GPU-cccc-dddd", uuid)
	assert.Nil(t, migInfo)
}

func TestGetDeviceInfo_V1beta1_MIG(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1beta1.ResourceSlice{
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
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1beta1"}
	uuid, migInfo := mgr.GetDeviceInfo("poolD", "mig-1")
	assert.Equal(t, "GPU-parent-2", uuid)
	require.NotNil(t, migInfo)
	assert.Equal(t, "MIG-2222", migInfo.MIGDeviceUUID)
	assert.Equal(t, "2g.24gb", migInfo.Profile)
	assert.Equal(t, "GPU-parent-2", migInfo.ParentUUID)
}

func TestGetDeviceInfo_NoMatch(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-empty"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: DRAGPUDriverName,
			Pool:   resourcev1.ResourcePool{Name: "poolA"},
			Devices: []resourcev1.Device{{
				Name: "gpu-99",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type": {StringValue: strPtr("gpu")},
					"uuid": {StringValue: strPtr("GPU-xxxx")},
				},
			}},
		},
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}
	uuid, migInfo := mgr.GetDeviceInfo("poolA", "gpu-does-not-exist")
	assert.Empty(t, uuid)
	assert.Nil(t, migInfo)
}

func TestGetDeviceInfo_WrongDriver(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-other-driver"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: "other.driver.io",
			Pool:   resourcev1.ResourcePool{Name: "poolA"},
			Devices: []resourcev1.Device{{
				Name: "gpu-0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type": {StringValue: strPtr("gpu")},
					"uuid": {StringValue: strPtr("GPU-wrong-driver")},
				},
			}},
		},
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}
	uuid, migInfo := mgr.GetDeviceInfo("poolA", "gpu-0")
	assert.Empty(t, uuid)
	assert.Nil(t, migInfo)
}

func TestGetDeviceInfo_NilInformer(t *testing.T) {
	mgr := &DRAResourceSliceManager{informer: nil}
	uuid, migInfo := mgr.GetDeviceInfo("poolA", "gpu-0")
	assert.Empty(t, uuid)
	assert.Nil(t, migInfo)
}

func TestGetDynamicResourceMappings_GPU(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-map"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: DRAGPUDriverName,
			Pool:   resourcev1.ResourcePool{Name: "poolA"},
			Devices: []resourcev1.Device{{
				Name: "gpu-0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type": {StringValue: strPtr("gpu")},
					"uuid": {StringValue: strPtr("GPU-mapped")},
				},
			}},
		},
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}

	dr := &podresourcesapi.DynamicResource{
		ClaimName:      "my-claim",
		ClaimNamespace: "my-ns",
		ClaimResources: []*podresourcesapi.ClaimResource{{
			DriverName: DRAGPUDriverName,
			PoolName:   "poolA",
			DeviceName: "gpu-0",
		}},
	}

	mappings := mgr.GetDynamicResourceMappings(dr)
	require.Len(t, mappings, 1)
	assert.Equal(t, "GPU-mapped", mappings[0].MappingKey)
	assert.Equal(t, "my-claim", mappings[0].Info.ClaimName)
	assert.Equal(t, "my-ns", mappings[0].Info.ClaimNamespace)
	assert.Equal(t, DRAGPUDriverName, mappings[0].Info.DriverName)
	assert.Equal(t, "poolA", mappings[0].Info.PoolName)
	assert.Equal(t, "gpu-0", mappings[0].Info.DeviceName)
	assert.Nil(t, mappings[0].Info.MIGInfo)
}

func TestGetDynamicResourceMappings_MIG(t *testing.T) {
	inf := newTestInformerForDRA()
	err := inf.Add(&resourcev1.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: "slice-mig-map"},
		Spec: resourcev1.ResourceSliceSpec{
			Driver: DRAGPUDriverName,
			Pool:   resourcev1.ResourcePool{Name: "poolB"},
			Devices: []resourcev1.Device{{
				Name: "mig-0",
				Attributes: map[resourcev1.QualifiedName]resourcev1.DeviceAttribute{
					"type":       {StringValue: strPtr("mig")},
					"uuid":       {StringValue: strPtr("MIG-5555")},
					"profile":    {StringValue: strPtr("3g.40gb")},
					"parentUUID": {StringValue: strPtr("GPU-parent-5")},
				},
			}},
		},
	})
	require.NoError(t, err)

	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}

	dr := &podresourcesapi.DynamicResource{
		ClaimName:      "mig-claim",
		ClaimNamespace: "mig-ns",
		ClaimResources: []*podresourcesapi.ClaimResource{{
			DriverName: DRAGPUDriverName,
			PoolName:   "poolB",
			DeviceName: "mig-0",
		}},
	}

	mappings := mgr.GetDynamicResourceMappings(dr)
	require.Len(t, mappings, 1)
	assert.Equal(t, "GPU-parent-5", mappings[0].MappingKey)
	require.NotNil(t, mappings[0].Info.MIGInfo)
	assert.Equal(t, "MIG-5555", mappings[0].Info.MIGInfo.MIGDeviceUUID)
	assert.Equal(t, "3g.40gb", mappings[0].Info.MIGInfo.Profile)
	assert.Equal(t, "GPU-parent-5", mappings[0].Info.MIGInfo.ParentUUID)
}

func TestGetDynamicResourceMappings_NilResource(t *testing.T) {
	mgr := &DRAResourceSliceManager{}
	mappings := mgr.GetDynamicResourceMappings(nil)
	assert.Nil(t, mappings)
}

func TestGetDynamicResourceMappings_NonGPUDriver(t *testing.T) {
	inf := newTestInformerForDRA()
	mgr := &DRAResourceSliceManager{informer: inf, sliceAPIVersion: "v1"}

	dr := &podresourcesapi.DynamicResource{
		ClaimName:      "other-claim",
		ClaimNamespace: "other-ns",
		ClaimResources: []*podresourcesapi.ClaimResource{{
			DriverName: "other.driver.io",
			PoolName:   "poolA",
			DeviceName: "dev-0",
		}},
	}

	mappings := mgr.GetDynamicResourceMappings(dr)
	assert.Empty(t, mappings)
}
