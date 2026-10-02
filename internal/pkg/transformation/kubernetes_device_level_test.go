/*
 * Copyright (c) 2024, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package transformation

import (
	stdos "os"
	"path/filepath"
	"testing"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	mockdeviceinfo "github.com/NVIDIA/dcgm-exporter/internal/mocks/pkg/deviceinfo"
	mocknvmlprovider "github.com/NVIDIA/dcgm-exporter/internal/mocks/pkg/nvmlprovider"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/appconfig"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/collector"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/counters"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/deviceinfo"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/nvmlprovider"
	"github.com/NVIDIA/dcgm-exporter/internal/pkg/testutils"
)

func TestNeedsPerProcessAttribution(t *testing.T) {
	t.Parallel()

	draPod := func(namespace, name, uid string) PodInfo {
		return PodInfo{
			Name: name, Namespace: namespace, UID: uid, Container: "ctr",
			DynamicResources: &DynamicResourceInfo{ClaimName: "claim"},
		}
	}

	tests := []struct {
		name string
		pods []PodInfo
		want bool
	}{
		{name: "no pods", pods: nil, want: false},
		{name: "single DRA pod", pods: []PodInfo{draPod("default", "pod-a", "uid-1")}, want: false},
		{
			name: "multiple containers of one DRA pod count as a single claimant",
			pods: []PodInfo{draPod("default", "pod-a", "uid-1"), draPod("default", "pod-a", "uid-1")},
			want: false,
		},
		{
			name: "two DRA pods with unpopulated UIDs are distinct owners",
			pods: []PodInfo{draPod("default", "pod-a", ""), draPod("default", "pod-b", "")},
			want: true,
		},
		{
			name: "same pod name with different UIDs is not one owner",
			pods: []PodInfo{draPod("default", "pod-a", "uid-1"), draPod("default", "pod-a", "uid-2")},
			want: true,
		},
		{
			name: "one entry with an unpopulated UID matches by name",
			pods: []PodInfo{draPod("default", "pod-a", "uid-1"), draPod("default", "pod-a", "")},
			want: false,
		},
		{
			name: "same pod name in different namespaces is not one owner",
			pods: []PodInfo{draPod("ns-1", "pod-a", ""), draPod("ns-2", "pod-a", "")},
			want: true,
		},
		{name: "single device-plugin pod", pods: []PodInfo{{Name: "pod-a", Namespace: "default", UID: "uid-1"}}, want: true},
		{
			name: "device-plugin pod next to a DRA pod",
			pods: []PodInfo{draPod("default", "pod-a", "uid-1"), {Name: "pod-b", Namespace: "default", UID: "uid-2"}},
			want: true,
		},
		{
			name: "two DRA pods share the device",
			pods: []PodInfo{draPod("default", "pod-a", "uid-1"), draPod("default", "pod-b", "uid-2")},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, needsPerProcessAttribution(tc.pods))
		})
	}
}

func TestFilterPerProcessCandidates(t *testing.T) {
	t.Parallel()

	draPod := func(name string) PodInfo {
		return PodInfo{Name: name, Namespace: "default", DynamicResources: &DynamicResourceInfo{ClaimName: "claim"}}
	}

	filtered := filterPerProcessCandidates(map[string][]PodInfo{
		"exclusive-dra": {draPod("pod-a")},
		"shared-dra":    {draPod("pod-a"), draPod("pod-b")},
		"device-plugin": {{Name: "pod-c", Namespace: "default"}},
	})

	assert.NotContains(t, filtered, "exclusive-dra")
	assert.Contains(t, filtered, "shared-dra")
	assert.Contains(t, filtered, "device-plugin")
}

const deviceLevelTestGPUUUID = "GPU-uuid-0"

type deviceLevelTestPod struct {
	name string
	dra  bool
}

// runDeviceLevelScenario runs Process for one metric on a single GPU and
// returns the device-level and pod-labelled series.
func runDeviceLevelScenario(
	t *testing.T,
	virtualGPUs bool,
	pods []deviceLevelTestPod,
	counter counters.Counter,
	deviceValue string,
	setupNVML func(*mocknvmlprovider.MockNVML),
) (deviceMetrics, podMetrics []collector.Metric) {
	t.Helper()
	ctrl := gomock.NewController(t)

	realNVML := nvmlprovider.Client()
	t.Cleanup(func() { nvmlprovider.SetClient(realNVML) })
	mockNVML := mocknvmlprovider.NewMockNVML(ctrl)
	setupNVML(mockNVML)
	nvmlprovider.SetClient(mockNVML)

	var podResources []*podresourcesapi.PodResources
	var podObjects []runtime.Object
	for _, p := range pods {
		container := &podresourcesapi.ContainerResources{Name: "ctr"}
		if p.dra {
			container.DynamicResources = []*podresourcesapi.DynamicResource{{
				ClaimName:      "shared-claim",
				ClaimNamespace: "default",
				ClaimResources: []*podresourcesapi.ClaimResource{{
					DriverName: DRAGPUDriverName,
					PoolName:   "poolA",
					DeviceName: "gpu-x",
				}},
			}}
		} else {
			container.Devices = []*podresourcesapi.ContainerDevices{{
				ResourceName: appconfig.NvidiaResourceName,
				DeviceIds:    []string{deviceLevelTestGPUUUID},
			}}
		}
		podResources = append(podResources, &podresourcesapi.PodResources{
			Name: p.name, Namespace: "default", Containers: []*podresourcesapi.ContainerResources{container},
		})
		podObjects = append(podObjects, &v1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: p.name, Namespace: "default", UID: types.UID("uid-" + p.name),
		}})
	}

	tmpDir, cleanup := testutils.CreateTmpDir(t)
	t.Cleanup(cleanup)
	socketPath := filepath.Join(tmpDir, "kubelet.sock")
	server := grpc.NewServer()
	podresourcesapi.RegisterPodResourcesListerServer(server, &dynamicResourcePodResourcesServer{
		response: &podresourcesapi.ListPodResourcesResponse{PodResources: podResources},
	})
	t.Cleanup(testutils.StartMockServer(t, server, socketPath))

	clientset := fake.NewClientset(podObjects...)
	pm := &PodMapper{
		Config: &appconfig.Config{
			KubernetesEnableDRA:       true,
			KubernetesVirtualGPUs:     virtualGPUs,
			KubernetesEnablePodUID:    true,
			KubernetesGPUIdType:       appconfig.GPUUID,
			PodResourcesKubeletSocket: socketPath,
			NvidiaResourceNames:       []string{appconfig.NvidiaResourceName},
		},
		ResourceSliceManager: newTestDRAManagerWithDevices(map[string]testDRADeviceMapping{
			"poolA/gpu-x": {uuid: deviceLevelTestGPUUUID},
		}),
		Client:           clientset,
		labelFilterCache: newLabelFilterCache(nil, 1000),
	}
	setupMockInformer(t, pm, clientset)

	mockSystemInfo := mockdeviceinfo.NewMockProvider(ctrl)
	mockSystemInfo.EXPECT().InfoType().Return(dcgm.FE_GPU).AnyTimes()
	mockSystemInfo.EXPECT().GPUCount().Return(toUint(1)).AnyTimes()
	mockSystemInfo.EXPECT().GPU(toUint(0)).Return(deviceinfo.GPUInfo{
		DeviceInfo: dcgm.Device{UUID: deviceLevelTestGPUUUID, GPU: 0},
	}).AnyTimes()

	metrics := collector.MetricsByCounter{
		counter: {{
			GPU: "0", GPUUUID: deviceLevelTestGPUUUID, GPUDevice: "nvidia0", Value: deviceValue,
			Counter: counter, Attributes: map[string]string{}, Labels: map[string]string{},
		}},
	}
	require.NoError(t, pm.Process(metrics, mockSystemInfo))

	for _, m := range metrics[counter] {
		if _, ok := m.Attributes[podAttribute]; ok {
			podMetrics = append(podMetrics, m)
		} else {
			deviceMetrics = append(deviceMetrics, m)
		}
	}
	return deviceMetrics, podMetrics
}

func assertDeviceLevelScenario(
	t *testing.T,
	pods []deviceLevelTestPod,
	deviceMetrics, podMetrics []collector.Metric,
	wantDevice int, deviceValue, wantPodValue string,
) {
	t.Helper()
	require.Len(t, deviceMetrics, wantDevice)
	for _, m := range deviceMetrics {
		assert.Equal(t, deviceValue, m.Value)
	}
	require.Len(t, podMetrics, len(pods))
	podNames := make(map[string]bool, len(podMetrics))
	for _, m := range podMetrics {
		assert.Equal(t, wantPodValue, m.Value)
		podNames[m.Attributes[podAttribute]] = true
	}
	for _, p := range pods {
		assert.True(t, podNames[p.name], "missing series for pod %s", p.name)
	}
}

func noNVMLProcessQueries(*mocknvmlprovider.MockNVML) {}

// TestPodMapperProcessDeviceLevelMetric verifies that the device-level value
// stays visible when a GPU is used by a single pod.
func TestPodMapperProcessDeviceLevelMetric(t *testing.T) {
	testutils.RequireLinux(t)

	gpuUtil := counters.Counter{FieldID: 203, FieldName: metricGPUUtil, PromType: "gauge"}
	power := counters.Counter{FieldID: 155, FieldName: "DCGM_FI_DEV_POWER_USAGE", PromType: "gauge"}
	noProcesses := func(mockNVML *mocknvmlprovider.MockNVML) {
		mockNVML.EXPECT().GetDeviceProcessMemory(deviceLevelTestGPUUUID).Return(map[uint32]uint64{}, nil).AnyTimes()
		mockNVML.EXPECT().GetDeviceProcessUtilization(deviceLevelTestGPUUUID).Return(map[uint32]uint32{}, nil).AnyTimes()
	}

	tests := []struct {
		name         string
		virtualGPUs  bool
		pods         []deviceLevelTestPod
		counter      counters.Counter
		setupNVML    func(*mocknvmlprovider.MockNVML)
		wantDevice   int
		wantPodValue string
	}{
		{
			name:         "single DRA pod gets the device value, virtual gpus on",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}},
			counter:      gpuUtil,
			setupNVML:    noNVMLProcessQueries,
			wantPodValue: "42",
		},
		{
			name:         "single DRA pod gets the device value, virtual gpus off",
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}},
			counter:      gpuUtil,
			setupNVML:    noNVMLProcessQueries,
			wantPodValue: "42",
		},
		{
			name:         "single DRA pod, non-per-process metric",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}},
			counter:      power,
			setupNVML:    noNVMLProcessQueries,
			wantPodValue: "42",
		},
		{
			name:         "single device-plugin pod keeps the device-level series",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a"}},
			counter:      gpuUtil,
			setupNVML:    noProcesses,
			wantDevice:   1,
			wantPodValue: "0",
		},
		{
			name:         "shared DRA claim keeps the device-level series, virtual gpus on",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}, {name: "pod-b", dra: true}},
			counter:      gpuUtil,
			setupNVML:    noProcesses,
			wantDevice:   1,
			wantPodValue: "0",
		},
		{
			name:         "shared DRA claim keeps the device-level series, virtual gpus off",
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}, {name: "pod-b", dra: true}},
			counter:      gpuUtil,
			setupNVML:    noProcesses,
			wantDevice:   1,
			wantPodValue: "0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deviceMetrics, podMetrics := runDeviceLevelScenario(t, tc.virtualGPUs, tc.pods, tc.counter, "42", tc.setupNVML)
			assertDeviceLevelScenario(t, tc.pods, deviceMetrics, podMetrics, tc.wantDevice, "42", tc.wantPodValue)
		})
	}
}

// TestPodMapperProcessMPSDeviceLevelMetric covers MPS, where NVML attributes
// utilization to the MPS server process rather than to any workload pod.
func TestPodMapperProcessMPSDeviceLevelMetric(t *testing.T) {
	testutils.RequireLinux(t)

	// A live PID that belongs to none of the pods, like the MPS server.
	mpsServerPID := uint32(stdos.Getpid()) //nolint:gosec // G115: PIDs are non-negative
	mpsServer := func(mockNVML *mocknvmlprovider.MockNVML) {
		mockNVML.EXPECT().GetDeviceProcessMemory(deviceLevelTestGPUUUID).
			Return(map[uint32]uint64{mpsServerPID: 28 * 1024 * 1024}, nil).AnyTimes()
		mockNVML.EXPECT().GetDeviceProcessUtilization(deviceLevelTestGPUUUID).
			Return(map[uint32]uint32{mpsServerPID: 94}, nil).AnyTimes()
	}

	gpuUtil := counters.Counter{FieldID: 203, FieldName: metricGPUUtil, PromType: "gauge"}
	fbUsed := counters.Counter{FieldID: 252, FieldName: metricFBUsed, PromType: "gauge"}

	tests := []struct {
		name         string
		virtualGPUs  bool
		pods         []deviceLevelTestPod
		counter      counters.Counter
		deviceValue  string
		setupNVML    func(*mocknvmlprovider.MockNVML)
		wantDevice   int
		wantPodValue string
	}{
		{
			name:         "device-plugin MPS, single pod: utilization stays on the device-level series",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a"}},
			counter:      gpuUtil,
			deviceValue:  "94",
			setupNVML:    mpsServer,
			wantDevice:   1,
			wantPodValue: "0",
		},
		{
			name:         "device-plugin MPS, single pod: memory stays on the device-level series",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a"}},
			counter:      fbUsed,
			deviceValue:  "72",
			setupNVML:    mpsServer,
			wantDevice:   1,
			wantPodValue: "0",
		},
		{
			name:         "DRA MPS, single pod: pod series carries the device utilization",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}},
			counter:      gpuUtil,
			deviceValue:  "94",
			setupNVML:    noNVMLProcessQueries,
			wantPodValue: "94",
		},
		{
			name:         "DRA MPS, shared claim: utilization stays on the device-level series",
			virtualGPUs:  true,
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}, {name: "pod-b", dra: true}},
			counter:      gpuUtil,
			deviceValue:  "94",
			setupNVML:    mpsServer,
			wantDevice:   1,
			wantPodValue: "0",
		},
		{
			name:         "DRA MPS, shared claim, virtual gpus off: utilization stays on the device-level series",
			pods:         []deviceLevelTestPod{{name: "pod-a", dra: true}, {name: "pod-b", dra: true}},
			counter:      gpuUtil,
			deviceValue:  "94",
			setupNVML:    mpsServer,
			wantDevice:   1,
			wantPodValue: "0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deviceMetrics, podMetrics := runDeviceLevelScenario(t, tc.virtualGPUs, tc.pods, tc.counter, tc.deviceValue, tc.setupNVML)
			assertDeviceLevelScenario(t, tc.pods, deviceMetrics, podMetrics, tc.wantDevice, tc.deviceValue, tc.wantPodValue)
		})
	}
}
