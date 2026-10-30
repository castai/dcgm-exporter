/*
 * Copyright (c) 2025, NVIDIA CORPORATION.  All rights reserved.
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
	"fmt"
	"path/filepath"
	"sort"
	"testing"

	"github.com/NVIDIA/go-dcgm/pkg/dcgm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

const scenarioGPUUUID = "GPU-scenario-0"

var (
	smActiveCounter    = counters.Counter{FieldID: 1002, FieldName: "DCGM_FI_PROF_SM_ACTIVE", PromType: "gauge"}
	smOccupancyCounter = counters.Counter{FieldID: 1003, FieldName: "DCGM_FI_PROF_SM_OCCUPANCY", PromType: "gauge"}
	powerCounter       = counters.Counter{FieldID: 155, FieldName: "DCGM_FI_DEV_POWER_USAGE", PromType: "gauge"}
	gpuUtilCounter     = counters.Counter{FieldID: 203, FieldName: metricGPUUtil, PromType: "gauge"}
	fbUsedCounter      = counters.Counter{FieldID: 252, FieldName: metricFBUsed, PromType: "gauge"}
)

// scenarioContainer describes one GPU-holding container of a scenario pod.
// Exactly one of dra or deviceIDs describes how the container holds the GPU.
type scenarioContainer struct {
	name      string
	dra       bool
	deviceIDs []string
}

type scenarioPod struct {
	name       string
	containers []scenarioContainer
}

func (p scenarioPod) podUID() string {
	return "uid-" + p.name
}

// scenarioOptions configures runDeviceLevelScenario.
type scenarioOptions struct {
	virtualGPUs bool
	idType      appconfig.KubernetesGPUIDType
	pods        []scenarioPod
	counter     counters.Counter
	// deviceValue is the exporter's device-level reading for the single GPU.
	deviceValue string
	setupNVML   func(*mocknvmlprovider.MockNVML)
}

func noNVMLProcessQueries(*mocknvmlprovider.MockNVML) {}

func emptyProcessQueries(mockNVML *mocknvmlprovider.MockNVML) {
	mockNVML.EXPECT().GetDeviceProcessMemory(scenarioGPUUUID).Return(map[uint32]uint64{}, nil).AnyTimes()
	mockNVML.EXPECT().GetDeviceProcessUtilization(scenarioGPUUUID).Return(map[uint32]uint32{}, nil).AnyTimes()
}

// runDeviceLevelScenario runs Process once for one device-level metric on a
// single GPU and returns the unattributed (device) rows and the pod-attributed
// rows. Pod resources are served from an in-memory fixture over a real Unix
// socket, mirroring the production retrieval path.
func runDeviceLevelScenario(t *testing.T, opts scenarioOptions) (deviceMetrics, podMetrics []collector.Metric) {
	t.Helper()
	ctrl := gomock.NewController(t)

	realNVML := nvmlprovider.Client()
	t.Cleanup(func() { nvmlprovider.SetClient(realNVML) })
	mockNVML := mocknvmlprovider.NewMockNVML(ctrl)
	if opts.setupNVML == nil {
		opts.setupNVML = noNVMLProcessQueries
	}
	opts.setupNVML(mockNVML)
	nvmlprovider.SetClient(mockNVML)

	podResources := make([]*podresourcesapi.PodResources, 0, len(opts.pods))
	kubePods := make([]*v1.Pod, 0, len(opts.pods))
	for _, pod := range opts.pods {
		containers := make([]*podresourcesapi.ContainerResources, 0, len(pod.containers))
		for _, ctr := range pod.containers {
			container := &podresourcesapi.ContainerResources{Name: ctr.name}
			if ctr.dra {
				container.DynamicResources = []*podresourcesapi.DynamicResource{{
					ClaimName:      "claim-" + pod.name + "-" + ctr.name,
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
					DeviceIds:    ctr.deviceIDs,
				}}
			}
			containers = append(containers, container)
		}
		podResources = append(podResources, &podresourcesapi.PodResources{
			Name: pod.name, Namespace: "default", Containers: containers,
		})
		kubePods = append(kubePods, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: pod.name, Namespace: "default", UID: types.UID(pod.podUID()),
			},
		})
	}

	tmpDir, cleanup := testutils.CreateTmpDir(t)
	t.Cleanup(cleanup)
	socketPath := filepath.Join(tmpDir, "kubelet.sock")
	server := grpc.NewServer()
	podresourcesapi.RegisterPodResourcesListerServer(server, &dynamicResourcePodResourcesServer{
		response: &podresourcesapi.ListPodResourcesResponse{PodResources: podResources},
	})
	t.Cleanup(testutils.StartMockServer(t, server, socketPath))

	clientset := fake.NewClientset(toRuntimeObjects(kubePods)...)
	pm := &PodMapper{
		Config: &appconfig.Config{
			KubernetesEnableDRA:       true,
			KubernetesVirtualGPUs:     opts.virtualGPUs,
			KubernetesEnablePodUID:    true,
			KubernetesGPUIdType:       opts.idType,
			PodResourcesKubeletSocket: socketPath,
			NvidiaResourceNames:       []string{appconfig.NvidiaResourceName},
		},
		ResourceSliceManager: newTestDRAManagerWithDevices(map[string]testDRADeviceMapping{
			draDeviceKey("poolA", "gpu-x"): {uuid: scenarioGPUUUID},
		}),
		Client:           clientset,
		labelFilterCache: newLabelFilterCache(nil, 1000),
		stopChan:         make(chan struct{}),
	}
	t.Cleanup(pm.Stop)
	setupMockInformer(t, pm, clientset)

	mockSystemInfo := mockdeviceinfo.NewMockProvider(ctrl)
	mockSystemInfo.EXPECT().InfoType().Return(dcgm.FE_GPU).AnyTimes()
	mockSystemInfo.EXPECT().GPUCount().Return(toUint(1)).AnyTimes()
	mockSystemInfo.EXPECT().GPU(toUint(0)).Return(deviceinfo.GPUInfo{
		DeviceInfo: dcgm.Device{UUID: scenarioGPUUUID, GPU: 0},
	}).AnyTimes()

	metrics := collector.MetricsByCounter{
		opts.counter: {{
			GPU:        "0",
			GPUUUID:    scenarioGPUUUID,
			GPUDevice:  "nvidia0",
			Value:      opts.deviceValue,
			Counter:    opts.counter,
			Attributes: map[string]string{},
			Labels:     map[string]string{},
		}},
	}
	require.NoError(t, pm.Process(metrics, mockSystemInfo))

	for _, m := range metrics[opts.counter] {
		if _, hasPod := m.Attributes[podAttribute]; hasPod {
			podMetrics = append(podMetrics, m)
		} else {
			deviceMetrics = append(deviceMetrics, m)
		}
	}
	return deviceMetrics, podMetrics
}

// metricSeriesKey renders the identifying label set of a metric for duplicate
// series detection: two metrics with the same key cannot both be exposed. It
// includes the device identity the renderer adds as labels (GPU index, UUID,
// device name, MIG instance) so per-instance series never collide.
func metricSeriesKey(m collector.Metric) string {
	pairs := make([]string, 0, len(m.Attributes)+len(m.Labels)+6)
	pairs = append(pairs,
		"gpu="+m.GPU,
		"UUID="+m.GPUUUID,
		"device="+m.GPUDevice,
		"instance="+m.GPUInstanceID,
		"profile="+m.MigProfile,
	)
	for k, v := range m.Attributes {
		pairs = append(pairs, k+"="+v)
	}
	for k, v := range m.Labels {
		pairs = append(pairs, k+"="+v)
	}
	sort.Strings(pairs)
	return fmt.Sprintf("%s{%s}", m.Counter.FieldName, pairs)
}

func assertNoDuplicateSeries(t *testing.T, metrics ...[]collector.Metric) {
	t.Helper()
	seen := make(map[string]bool)
	for _, group := range metrics {
		for _, m := range group {
			key := metricSeriesKey(m)
			require.Falsef(t, seen[key], "duplicate series emitted: %s", key)
			seen[key] = true
		}
	}
}

// assertDeviceMetadataPreserved checks that an unattributed device row keeps
// the device identity downstream joins rely on.
func assertDeviceMetadataPreserved(t *testing.T, m collector.Metric) {
	t.Helper()
	assert.Equal(t, scenarioGPUUUID, m.GPUUUID)
	assert.Equal(t, "nvidia0", m.GPUDevice)
	assert.Equal(t, "0", m.GPU)
	// No workload identity may be attached to a row representing several
	// consumers.
	assert.NotContains(t, m.Attributes, podAttribute)
	assert.NotContains(t, m.Attributes, namespaceAttribute)
	assert.NotContains(t, m.Attributes, containerAttribute)
	assert.NotContains(t, m.Attributes, uidAttribute)
	assert.NotContains(t, m.Attributes, vgpuAttribute)
	assert.NotContains(t, m.Attributes, draClaimName)
	assert.NotContains(t, m.Attributes, draDriverName)
}

func draPod(pod string, containers ...string) scenarioPod {
	ctrs := make([]scenarioContainer, 0, len(containers))
	for _, name := range containers {
		ctr := scenarioContainer{name: name, dra: true}
		ctr.deviceIDs = nil
		ctr.dra = true
		ctrs = append(ctrs, ctr)
	}
	return scenarioPod{name: pod, containers: ctrs}
}

func devicePluginPod(pod, container string, deviceIDs ...string) scenarioPod {
	return scenarioPod{name: pod, containers: []scenarioContainer{{name: container, deviceIDs: deviceIDs}}}
}

// TestSharedDeviceAttributionScenarios covers the attribution contract across
// sharing modes, identifier types, virtual-GPU settings, and container
// topologies:
//
//   - Multiple distinct pod containers sharing a device emit device-level
//     measurements once, unattributed, with device metadata preserved.
//   - Per-process measurements (GPU_UTIL, FB_USED) survive with per-container
//     attribution and DRA claim labels.
//   - A single consumer keeps device-level attribution of the device value.
//   - Zero consumers keep the original device reading.
//
// NVML returns no processes, so per-process rows report the idle zero; the
// nonzero-value attribution is covered separately through
// createPerProcessMetrics fixtures.
func TestSharedDeviceAttributionScenarios(t *testing.T) {
	testutils.RequireLinux(t)

	twoDRAPods := []scenarioPod{draPod("pod-a", "ctr"), draPod("pod-b", "ctr")}
	oneDRAPodTwoContainers := []scenarioPod{draPod("pod-a", "ctr-a", "ctr-b")}
	sharedDevicePlugin := []scenarioPod{
		devicePluginPod("pod-a", "ctr", scenarioGPUUUID),
		devicePluginPod("pod-b", "ctr", scenarioGPUUUID),
	}

	tests := []struct {
		name            string
		opts            scenarioOptions
		wantDevice      int
		wantDeviceValue string
		wantPods        int
		wantPodValue    string
	}{
		{
			name: "two DRA pods share the GPU; profiling counter emitted once, virtual gpus on, uuid id",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: twoDRAPods, counter: smActiveCounter, deviceValue: "0.98", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "0.98",
		},
		{
			name: "two DRA pods share the GPU; occupancy emitted once, virtual gpus off, uuid id",
			opts: scenarioOptions{
				virtualGPUs: false, idType: appconfig.GPUUID,
				pods: twoDRAPods, counter: smOccupancyCounter, deviceValue: "0.16", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "0.16",
		},
		{
			name: "two DRA pods share the GPU; power emitted once, virtual gpus on, device-name id",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.DeviceName,
				pods: twoDRAPods, counter: powerCounter, deviceValue: "42", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "42",
		},
		{
			name: "two DRA pods share the GPU; sm active emitted once, virtual gpus off, device-name id",
			opts: scenarioOptions{
				virtualGPUs: false, idType: appconfig.DeviceName,
				pods: twoDRAPods, counter: smActiveCounter, deviceValue: "0.97", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "0.97",
		},
		{
			name: "two DRA pods share the GPU; per-process memory stays per pod with claim labels",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: twoDRAPods, counter: fbUsedCounter, deviceValue: "9186", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "9186",
			wantPods: 2, wantPodValue: "0",
		},
		{
			name: "two DRA pods share the GPU; per-process util stays per pod, device-name id",
			opts: scenarioOptions{
				virtualGPUs: false, idType: appconfig.DeviceName,
				pods: twoDRAPods, counter: gpuUtilCounter, deviceValue: "100", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "100",
			wantPods: 2, wantPodValue: "0",
		},
		{
			name: "one DRA pod with two GPU containers keeps per-container memory attribution",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: oneDRAPodTwoContainers, counter: fbUsedCounter, deviceValue: "8192", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "8192",
			wantPods: 2, wantPodValue: "0",
		},
		{
			name: "one DRA pod with two GPU containers; device-level value emitted once unattributed",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: oneDRAPodTwoContainers, counter: smActiveCounter, deviceValue: "0.7", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "0.7",
		},
		{
			name: "one DRA pod with two GPU containers; power unattributed, virtual gpus off",
			opts: scenarioOptions{
				virtualGPUs: false, idType: appconfig.GPUUID,
				pods: oneDRAPodTwoContainers, counter: powerCounter, deviceValue: "70", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "70",
		},
		{
			name: "single DRA consumer keeps device-level attribution",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: []scenarioPod{draPod("pod-a", "ctr")}, counter: smActiveCounter, deviceValue: "0.69",
				setupNVML: noNVMLProcessQueries,
			},
			wantDevice: 0,
			wantPods:   1, wantPodValue: "0.69",
		},
		{
			name: "shared device-plugin GPU gets the same suppression policy",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: sharedDevicePlugin, counter: smActiveCounter, deviceValue: "0.81", setupNVML: emptyProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "0.81",
		},
		{
			name: "repeated device ids of one consumer emit a single attributed series",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods:    []scenarioPod{devicePluginPod("pod-a", "ctr", scenarioGPUUUID, scenarioGPUUUID)},
				counter: powerCounter, deviceValue: "55", setupNVML: emptyProcessQueries,
			},
			wantDevice: 0,
			wantPods:   1, wantPodValue: "55",
		},
		{
			name: "zero consumers keep the original device reading",
			opts: scenarioOptions{
				virtualGPUs: true, idType: appconfig.GPUUID,
				pods: nil, counter: powerCounter, deviceValue: "37", setupNVML: noNVMLProcessQueries,
			},
			wantDevice: 1, wantDeviceValue: "37",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			deviceMetrics, podMetrics := runDeviceLevelScenario(t, tc.opts)

			require.Len(t, deviceMetrics, tc.wantDevice)
			for _, m := range deviceMetrics {
				assert.Equal(t, tc.wantDeviceValue, m.Value)
				assertDeviceMetadataPreserved(t, m)
			}
			require.Len(t, podMetrics, tc.wantPods)

			for _, m := range podMetrics {
				assert.Equal(t, tc.wantPodValue, m.Value)
			}
			assertNoDuplicateSeries(t, deviceMetrics, podMetrics)

			// DRA-attributed per-process rows must carry claim identity for
			// downstream joins.
			for _, pod := range tc.opts.pods {
				for _, ctr := range pod.containers {
					if !ctr.dra || !isPerProcessMetric(tc.opts.counter.FieldName) {
						continue
					}
					claimName := "claim-" + pod.name + "-" + ctr.name
					found := false
					for _, m := range podMetrics {
						if m.Attributes[podAttribute] == pod.name &&
							m.Attributes[containerAttribute] == ctr.name &&
							m.Attributes[draClaimName] == claimName {
							found = true
						}
					}
					assert.Truef(t, found, "missing attributed series for %s/%s with claim %s", pod.name, ctr.name, claimName)
				}
			}
		})
	}
}

// TestSharedDeviceAttributionTransitions verifies that attribution follows the
// live consumer set across scrapes: a sole consumer is attributed, a second
// consumer switches the device to a single unattributed series, and removing
// it restores attribution without stale workload labels.
func TestSharedDeviceAttributionTransitions(t *testing.T) {
	testutils.RequireLinux(t)

	solo := scenarioOptions{
		virtualGPUs: true, idType: appconfig.GPUUID,
		pods: []scenarioPod{draPod("pod-a", "ctr")}, counter: smActiveCounter, deviceValue: "0.5",
		setupNVML: noNVMLProcessQueries,
	}
	shared := scenarioOptions{
		virtualGPUs: true, idType: appconfig.GPUUID,
		pods: []scenarioPod{draPod("pod-a", "ctr"), draPod("pod-b", "ctr")}, counter: smActiveCounter, deviceValue: "0.9",
		setupNVML: emptyProcessQueries,
	}

	// 1 consumer: the device value is attributed to the pod.
	deviceMetrics, podMetrics := runDeviceLevelScenario(t, solo)
	require.Len(t, deviceMetrics, 0)
	require.Len(t, podMetrics, 1)
	assert.Equal(t, "pod-a", podMetrics[0].Attributes[podAttribute])
	assert.Equal(t, "0.5", podMetrics[0].Value)

	// 2 consumers: one unattributed device series, no per-pod rows.
	deviceMetrics, podMetrics = runDeviceLevelScenario(t, shared)
	require.Len(t, deviceMetrics, 1)
	assertDeviceMetadataPreserved(t, deviceMetrics[0])
	require.Len(t, podMetrics, 0)

	// Back to 1 consumer: attribution returns to the same pod with fresh
	// identity and no residue from the shared phase.
	deviceMetrics, podMetrics = runDeviceLevelScenario(t, solo)
	require.Len(t, deviceMetrics, 0)
	require.Len(t, podMetrics, 1)
	assert.Equal(t, "pod-a", podMetrics[0].Attributes[podAttribute])
	assert.Equal(t, "uid-pod-a", podMetrics[0].Attributes[uidAttribute])
	assert.Equal(t, "default", podMetrics[0].Attributes[namespaceAttribute])
	assert.Equal(t, "ctr", podMetrics[0].Attributes[containerAttribute])
	assert.Equal(t, "0.5", podMetrics[0].Value)
}

// TestCreatePerProcessMetricsPerContainer covers nonzero per-process values
// attributed per (pod, container) with DRA claim labels, using hand-built
// per-process fixtures that stand in for resolved PID mappings.
func TestCreatePerProcessMetricsPerContainer(t *testing.T) {
	t.Parallel()

	gpuUUID := "GPU-00000000-0000-0000-0000-000000000000"
	claim := func(name string) *DynamicResourceInfo {
		return &DynamicResourceInfo{
			ClaimName: name, ClaimNamespace: "ns1",
			DriverName: DRAGPUDriverName, PoolName: "poolA", DeviceName: "gpu-x",
		}
	}

	newPodMapper := func() *PodMapper {
		return &PodMapper{Config: &appconfig.Config{}}
	}

	fbMetric := func() collector.Metric {
		return collector.Metric{GPUUUID: gpuUUID, Value: "0", Attributes: map[string]string{}, Labels: map[string]string{}}
	}

	t.Run("two pods get their own nonzero memory values with claim labels", func(t *testing.T) {
		t.Parallel()
		podA := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr", DynamicResources: claim("claim-a")}
		podB := PodInfo{Name: "pod-b", Namespace: "default", UID: "uid-b", Container: "ctr", DynamicResources: claim("claim-b")}

		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{gpuUUID: {
				pidToMemory: map[uint32]uint64{101: 2 << 30, 102: 3 << 30},
			}},
			pidToPod:     map[uint32]*PodInfo{101: &podA, 102: &podB},
			deviceToPods: map[string][]PodInfo{gpuUUID: {podA, podB}},
		}

		result, err := newPodMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpuUUID}, fbUsedCounter, fbMetric(), dataMap, func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 2)

		byPod := map[string]collector.Metric{}
		for _, m := range result {
			byPod[m.Attributes[podAttribute]] = m
		}
		require.Len(t, byPod, 2)
		assert.Equal(t, "2048", byPod["pod-a"].Value)
		assert.Equal(t, "3072", byPod["pod-b"].Value)
		assert.Equal(t, "claim-a", byPod["pod-a"].Attributes[draClaimName])
		assert.Equal(t, "claim-b", byPod["pod-b"].Attributes[draClaimName])
		assert.Equal(t, "uid-a", byPod["pod-a"].Attributes[uidAttribute])
		assertNoDuplicateSeries(t, nil, result)
	})

	t.Run("containers of one pod get separate values, not a repeated pod total", func(t *testing.T) {
		t.Parallel()
		ctrA := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-a", DynamicResources: claim("claim-1")}
		ctrB := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-b", DynamicResources: claim("claim-2")}

		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{gpuUUID: {
				pidToMemory: map[uint32]uint64{101: 2 << 30, 102: 1 << 30},
			}},
			pidToPod:     map[uint32]*PodInfo{101: &ctrA, 102: &ctrB},
			deviceToPods: map[string][]PodInfo{gpuUUID: {ctrA, ctrB}},
		}

		result, err := newPodMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpuUUID}, fbUsedCounter, fbMetric(), dataMap, func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 2)

		byContainer := map[string]string{}
		for _, m := range result {
			byContainer[m.Attributes[containerAttribute]] = m.Value
		}
		assert.Equal(t, map[string]string{"ctr-a": "2048", "ctr-b": "1024"}, byContainer)
		assertNoDuplicateSeries(t, nil, result)
	})

	t.Run("container without processes reports zero, not absence", func(t *testing.T) {
		t.Parallel()
		ctrA := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-a"}
		ctrB := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-b"}

		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{gpuUUID: {
				pidToMemory: map[uint32]uint64{101: 2 << 30},
			}},
			pidToPod:     map[uint32]*PodInfo{101: &ctrA},
			deviceToPods: map[string][]PodInfo{gpuUUID: {ctrA, ctrB}},
		}

		result, err := newPodMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpuUUID}, fbUsedCounter, fbMetric(), dataMap, func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 2)

		byContainer := map[string]string{}
		for _, m := range result {
			byContainer[m.Attributes[containerAttribute]] = m.Value
		}
		assert.Equal(t, map[string]string{"ctr-a": "2048", "ctr-b": "0"}, byContainer)
	})

	t.Run("gpu util attributes per container", func(t *testing.T) {
		t.Parallel()
		podA := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-a"}
		podB := PodInfo{Name: "pod-b", Namespace: "default", UID: "uid-b", Container: "ctr-a"}

		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{gpuUUID: {
				pidToSMUtil: map[uint32]uint32{101: 40, 102: 60},
			}},
			pidToPod:     map[uint32]*PodInfo{101: &podA, 102: &podB},
			deviceToPods: map[string][]PodInfo{gpuUUID: {podA, podB}},
		}

		result, err := newPodMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpuUUID}, gpuUtilCounter,
			collector.Metric{GPUUUID: gpuUUID, Value: "0", Attributes: map[string]string{}, Labels: map[string]string{}},
			dataMap, func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 2)

		byPod := map[string]string{}
		for _, m := range result {
			byPod[m.Attributes[podAttribute]] = m.Value
		}
		assert.Equal(t, map[string]string{"pod-a": "40", "pod-b": "60"}, byPod)
	})
}

// TestSameConsumer covers the attribution identity comparison, including the
// UID fallback semantics.
// TestCreatePerProcessMetricsAcrossDevices covers the collector's global
// PID-to-pod map being shared across devices: with the attribution identity
// being (pod, container) — invariant per process — the entry resolved for a
// later device must not corrupt an earlier device's attribution, even though
// the devices describe the consumer with different vGPU allocations.
func TestCreatePerProcessMetricsAcrossDevices(t *testing.T) {
	t.Parallel()

	gpu0, gpu1 := "GPU-dev-0", "GPU-dev-1"
	claim := func(name string) *DynamicResourceInfo {
		return &DynamicResourceInfo{
			ClaimName: name, ClaimNamespace: "ns1",
			DriverName: DRAGPUDriverName, PoolName: "poolA", DeviceName: "gpu-x",
		}
	}
	fbMetric := func() collector.Metric {
		return collector.Metric{Value: "0", Attributes: map[string]string{}, Labels: map[string]string{}}
	}
	newMapper := func() *PodMapper { return &PodMapper{Config: &appconfig.Config{}} }

	// One pod container with one process using both GPUs. The sharing map
	// describes it with VGPU=1 on GPU 0 and VGPU=2 on GPU 1; the global
	// pidToPod holds whichever entry the last device's collection resolved.
	consumer := func(vgpu string) PodInfo {
		return PodInfo{
			Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr", VGPU: vgpu,
			DynamicResources: claim("claim-a"),
		}
	}

	t.Run("later device resolution does not zero the earlier device", func(t *testing.T) {
		t.Parallel()
		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{
				gpu0: {pidToMemory: map[uint32]uint64{101: 1024 << 20}},
				gpu1: {pidToMemory: map[uint32]uint64{101: 2048 << 20}},
			},
			pidToPod: map[uint32]*PodInfo{101: ptr(consumer("2"))}, // GPU 1 won the overwrite
			deviceToPods: map[string][]PodInfo{
				gpu0: {consumer("1")},
				gpu1: {consumer("2")},
			},
		}

		for _, tc := range []struct{ gpuUUID, vgpu, want string }{
			{gpu0, "1", "1024"},
			{gpu1, "2", "2048"},
		} {
			result, err := newMapper().createPerProcessMetrics(
				collector.Metric{GPUUUID: tc.gpuUUID}, fbUsedCounter, fbMetric(), dataMap,
				func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
			)
			require.NoError(t, err)
			require.Len(t, result, 1)
			assert.Equal(t, tc.want, result[0].Value)
			assert.Equal(t, "pod-a", result[0].Attributes[podAttribute])
			assert.Equal(t, "ctr", result[0].Attributes[containerAttribute])
			assert.Equal(t, tc.vgpu, result[0].Attributes[vgpuAttribute], "allocation label follows the device's own description")
		}
	})

	t.Run("reversed overwrite order is equally correct", func(t *testing.T) {
		t.Parallel()
		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{
				gpu0: {pidToMemory: map[uint32]uint64{101: 1024 << 20}},
				gpu1: {pidToMemory: map[uint32]uint64{101: 2048 << 20}},
			},
			pidToPod: map[uint32]*PodInfo{101: ptr(consumer("1"))}, // GPU 0 won the overwrite
			deviceToPods: map[string][]PodInfo{
				gpu0: {consumer("1")},
				gpu1: {consumer("2")},
			},
		}

		for _, tc := range []struct{ gpuUUID, want string }{
			{gpu0, "1024"},
			{gpu1, "2048"},
		} {
			result, err := newMapper().createPerProcessMetrics(
				collector.Metric{GPUUUID: tc.gpuUUID}, fbUsedCounter, fbMetric(), dataMap,
				func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
			)
			require.NoError(t, err)
			require.Len(t, result, 1)
			assert.Equal(t, tc.want, result[0].Value)
		}
	})

	t.Run("multiple processes of one container accumulate per device", func(t *testing.T) {
		t.Parallel()
		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{
				gpu0: {pidToMemory: map[uint32]uint64{101: 100 << 20, 102: 24 << 20}},
			},
			pidToPod: map[uint32]*PodInfo{
				101: ptr(consumer("")),
				102: ptr(consumer("")),
			},
			deviceToPods: map[string][]PodInfo{gpu0: {consumer("")}},
		}

		result, err := newMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpu0}, fbUsedCounter, fbMetric(), dataMap,
			func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 1)
		assert.Equal(t, "124", result[0].Value)
	})

	t.Run("replica allocations of one container emit a single device measurement", func(t *testing.T) {
		t.Parallel()
		// The sharing map lists the container once per vGPU replica of the
		// device; cgroups cannot distinguish which replica a process uses.
		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{
				gpu0: {pidToMemory: map[uint32]uint64{101: 100 << 20, 102: 50 << 20}},
			},
			pidToPod: map[uint32]*PodInfo{
				101: ptr(consumer("1")),
				102: ptr(consumer("2")),
			},
			deviceToPods: map[string][]PodInfo{gpu0: {consumer("1"), consumer("2")}},
		}

		result, err := newMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpu0}, fbUsedCounter, fbMetric(), dataMap,
			func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 1, "one series per container, not per replica")
		assert.Equal(t, "150", result[0].Value)
		assert.NotContains(t, result[0].Attributes, vgpuAttribute, "ambiguous replica allocation is not retained")
		assertNoDuplicateSeries(t, result)
	})

	t.Run("unresolved container attribution emits no misleading zero", func(t *testing.T) {
		t.Parallel()
		// The pod has two GPU containers; one process resolves to ctr-a,
		// another cannot be resolved. ctr-b must not appear as idle-zero.
		ctrA := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-a", DynamicResources: claim("claim-a")}
		ctrB := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "ctr-b", DynamicResources: claim("claim-b")}
		unknown := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-a", Container: "", containerUnknown: true}

		dataMap := &perProcessDataMap{
			metrics: map[string]*perProcessMetrics{
				gpu0: {pidToMemory: map[uint32]uint64{101: 100 << 20, 102: 900 << 20}},
			},
			pidToPod:     map[uint32]*PodInfo{101: &ctrA, 102: &unknown},
			deviceToPods: map[string][]PodInfo{gpu0: {ctrA, ctrB}},
		}

		result, err := newMapper().createPerProcessMetrics(
			collector.Metric{GPUUUID: gpu0}, fbUsedCounter, fbMetric(), dataMap,
			func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
		)
		require.NoError(t, err)
		require.Len(t, result, 1, "only the resolved container is attributed")
		assert.Equal(t, "ctr-a", result[0].Attributes[containerAttribute])
		assert.Equal(t, "100", result[0].Value)
		assertNoDuplicateSeries(t, result)
	})
}

func ptr(p PodInfo) *PodInfo { return &p }

func TestSameConsumer(t *testing.T) {
	t.Parallel()

	base := PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr"}
	tests := []struct {
		name string
		a, b PodInfo
		want bool
	}{
		{name: "identical", a: base, b: base, want: true},
		{
			name: "same pod different container",
			a:    base,
			b:    PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "other"},
			want: false,
		},
		{
			name: "same pod and container across different vgpu allocations",
			a:    base,
			b:    PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: "2"},
			want: true,
		},
		{
			name: "unpopulated UIDs fall back to namespace and name",
			a:    PodInfo{Name: "pod-a", Namespace: "default", Container: "ctr"},
			b:    PodInfo{Name: "pod-a", Namespace: "default", Container: "ctr"},
			want: true,
		},
		{
			name: "unpopulated UID next to populated UID matches by name",
			a:    base,
			b:    PodInfo{Name: "pod-a", Namespace: "default", Container: "ctr"},
			want: true,
		},
		{
			name: "different namespaces",
			a:    base,
			b:    PodInfo{Name: "pod-a", Namespace: "other", UID: "uid-2", Container: "ctr"},
			want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, sameConsumer(tc.a, tc.b))
			assert.Equal(t, tc.want, sameConsumer(tc.b, tc.a))
		})
	}
}

// TestDistinctPodConsumers covers deduplication of repeated consumer mappings.
func TestDistinctPodConsumers(t *testing.T) {
	t.Parallel()

	pod := func(container string) PodInfo {
		return PodInfo{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: container}
	}

	tests := []struct {
		name string
		pods []PodInfo
		want []PodInfo // retained consumers with their (possibly cleared) VGPU
	}{
		{name: "nil", pods: nil, want: []PodInfo{}},
		{name: "single", pods: []PodInfo{pod("ctr")}, want: []PodInfo{pod("ctr")}},
		{
			name: "repeated identical entries collapse",
			pods: []PodInfo{pod("ctr"), pod("ctr"), pod("ctr")},
			want: []PodInfo{pod("ctr")},
		},
		{
			name: "containers of one pod stay distinct",
			pods: []PodInfo{pod("ctr-a"), pod("ctr-b")},
			want: []PodInfo{pod("ctr-a"), pod("ctr-b")},
		},
		{
			name: "vgpu replicas of one container merge to one consumer with cleared allocation",
			pods: []PodInfo{
				{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: "1"},
				{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: "2"},
			},
			want: []PodInfo{{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: ""}},
		},
		{
			name: "agreeing vgpu allocations are retained",
			pods: []PodInfo{
				{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: "1"},
				{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: "1"},
			},
			want: []PodInfo{{Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr", VGPU: "1"}},
		},
		{
			name: "different pods stay distinct",
			pods: []PodInfo{pod("ctr"), {Name: "pod-b", Namespace: "default", UID: "uid-2", Container: "ctr"}},
			want: []PodInfo{pod("ctr"), {Name: "pod-b", Namespace: "default", UID: "uid-2", Container: "ctr"}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := distinctPodConsumers(tc.pods)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestNeedsPerProcessAttributionContainers extends the pod-level table with
// the container boundary: several GPU containers of one pod require per-process
// collection so values can be split per container.
func TestNeedsPerProcessAttributionContainers(t *testing.T) {
	t.Parallel()

	draPod := func(container string) PodInfo {
		return PodInfo{
			Name: "pod-a", Namespace: "default", UID: "uid-1", Container: container,
			DynamicResources: &DynamicResourceInfo{ClaimName: "claim"},
		}
	}

	assert.False(t, needsPerProcessAttribution(nil))
	assert.False(t, needsPerProcessAttribution([]PodInfo{draPod("ctr")}))
	assert.True(t, needsPerProcessAttribution([]PodInfo{draPod("ctr-a"), draPod("ctr-b")}),
		"one pod with two GPU containers needs per-process collection to split values per container")
}

// TestExtractContainerID covers runtime container ID extraction from cgroup
// paths across the layouts seen with containerd.
func TestExtractContainerID(t *testing.T) {
	t.Parallel()

	const id = "0f240e9ecb3ac2b6b1f0e4b6f7b3a2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a1"

	tests := []struct {
		name string
		path string
		want string
	}{
		{
			name: "cgroup v2 containerd scope",
			path: "/kubepods.slice/kubepods-burstable.slice/kubepods-burstable-pod1234.slice/cri-containerd-" + id + ".scope",
			want: id,
		},
		{
			name: "cgroup v1 plain component",
			path: "/kubepods/burstable/pod1234/" + id,
			want: id,
		},
		{name: "no container component", path: "/kubepods.slice/kubepods-burstable.slice", want: ""},
		{name: "empty path", path: "", want: ""},
		{
			name: "pod uid only",
			path: "/kubepods.slice/kubepods-besteffort-pod096fd1c5_8b0a_4a58_a8c1_72e3ad77b872.slice",
			want: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, extractContainerID(tc.path))
		})
	}
}

// TestResolveContainerEntry covers PID-to-container resolution against the
// pod status mapping: resolution through the runtime ID, the unambiguous
// single-candidate fallback, and unknown attribution when several candidates
// cannot be distinguished.
func TestResolveContainerEntry(t *testing.T) {
	t.Parallel()

	ctrA := PodInfo{
		Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr-a",
		containerIDs: map[string]string{"id-a": "ctr-a"},
	}
	ctrB := PodInfo{
		Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr-b",
		containerIDs: map[string]string{"id-b": "ctr-b"},
	}

	t.Run("resolves the container owning the runtime id", func(t *testing.T) {
		t.Parallel()
		got := resolveContainerEntry([]*PodInfo{&ctrA, &ctrB}, "id-b")
		require.NotNil(t, got)
		assert.Equal(t, "ctr-b", got.Container)
	})

	t.Run("resolution is independent of candidate order", func(t *testing.T) {
		t.Parallel()
		forward := resolveContainerEntry([]*PodInfo{&ctrA, &ctrB}, "id-a")
		reverse := resolveContainerEntry([]*PodInfo{&ctrB, &ctrA}, "id-a")
		require.NotNil(t, forward)
		require.NotNil(t, reverse)
		assert.Equal(t, "ctr-a", forward.Container)
		assert.Equal(t, "ctr-a", reverse.Container)
	})

	t.Run("unresolved container with several candidates is unknown", func(t *testing.T) {
		t.Parallel()
		// No runtime ID available (missing container status / cgroup layout).
		assert.Nil(t, resolveContainerEntry([]*PodInfo{&ctrA, &ctrB}, ""))
		// Runtime ID not present in the observed pod status.
		assert.Nil(t, resolveContainerEntry([]*PodInfo{&ctrA, &ctrB}, "id-zz"))
	})

	t.Run("resolved container holds no allocation on this device", func(t *testing.T) {
		t.Parallel()
		// The pod status knows container ctr-c, but only ctr-a and ctr-b hold
		// this device: the process belongs to neither of them.
		other := PodInfo{
			Name: "pod-a", Namespace: "default", UID: "uid-1", Container: "ctr-a",
			containerIDs: map[string]string{"id-c": "ctr-c"},
		}
		assert.Nil(t, resolveContainerEntry([]*PodInfo{&other, &ctrB}, "id-c"))
	})

	t.Run("unambiguous single candidate owns every process of the pod", func(t *testing.T) {
		t.Parallel()
		got := resolveContainerEntry([]*PodInfo{&ctrA}, "id-zz")
		require.NotNil(t, got)
		assert.Equal(t, "ctr-a", got.Container)
	})
}

// TestSharedMIGInstanceAttribution covers MIG topology: a GPU instance shared
// by two pods is emitted once without attribution, while a separate instance
// on the same GPU keeps its sole consumer's attribution. Consumers of
// different instances never merge. Metrics carry the production MIG
// representation — parent GPU UUID plus GPUInstanceID — which is what the
// collector assigns and the per-process lookup keys on (parent UUID +
// instance in getMIGMetricsKey).
func TestSharedMIGInstanceAttribution(t *testing.T) {
	testutils.RequireLinux(t)

	const (
		parentUUID = "GPU-mig-parent-0"
		migShared  = "MIG-shared-instance"
		migSolo    = "MIG-solo-instance"
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	realNVML := nvmlprovider.Client()
	t.Cleanup(func() { nvmlprovider.SetClient(realNVML) })
	mockNVML := mocknvmlprovider.NewMockNVML(ctrl)
	mockNVML.EXPECT().GetMIGDeviceInfoByID(migShared).Return(&nvmlprovider.MIGDeviceInfo{
		ParentUUID: parentUUID, GPUInstanceID: 3,
	}, nil).AnyTimes()
	mockNVML.EXPECT().GetMIGDeviceInfoByID(migSolo).Return(&nvmlprovider.MIGDeviceInfo{
		ParentUUID: parentUUID, GPUInstanceID: 4,
	}, nil).AnyTimes()
	mockNVML.EXPECT().GetAllMIGDevicesProcessMemory(parentUUID).Return(map[uint]map[uint32]uint64{}, nil).AnyTimes()
	nvmlprovider.SetClient(mockNVML)

	claimFor := func(device string) *podresourcesapi.DynamicResource {
		return &podresourcesapi.DynamicResource{
			ClaimName:      "claim-" + device,
			ClaimNamespace: "default",
			ClaimResources: []*podresourcesapi.ClaimResource{{
				DriverName: DRAGPUDriverName,
				PoolName:   "poolA",
				DeviceName: device,
			}},
		}
	}

	podResources := []*podresourcesapi.PodResources{
		{Name: "pod-a", Namespace: "default", Containers: []*podresourcesapi.ContainerResources{{
			Name: "ctr", DynamicResources: []*podresourcesapi.DynamicResource{claimFor("gpu-mig-shared")},
		}}},
		{Name: "pod-b", Namespace: "default", Containers: []*podresourcesapi.ContainerResources{{
			Name: "ctr", DynamicResources: []*podresourcesapi.DynamicResource{claimFor("gpu-mig-shared")},
		}}},
		{Name: "pod-c", Namespace: "default", Containers: []*podresourcesapi.ContainerResources{{
			Name: "ctr", DynamicResources: []*podresourcesapi.DynamicResource{claimFor("gpu-mig-solo")},
		}}},
	}
	kubePods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-a", Namespace: "default", UID: types.UID("uid-pod-a")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-b", Namespace: "default", UID: types.UID("uid-pod-b")}},
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-c", Namespace: "default", UID: types.UID("uid-pod-c")}},
	}

	tmpDir, cleanup := testutils.CreateTmpDir(t)
	defer cleanup()
	socketPath := filepath.Join(tmpDir, "kubelet.sock")
	server := grpc.NewServer()
	podresourcesapi.RegisterPodResourcesListerServer(server, &dynamicResourcePodResourcesServer{
		response: &podresourcesapi.ListPodResourcesResponse{PodResources: podResources},
	})
	cleanupServer := testutils.StartMockServer(t, server, socketPath)
	defer cleanupServer()

	clientset := fake.NewClientset(toRuntimeObjects(kubePods)...)
	pm := &PodMapper{
		Config: &appconfig.Config{
			KubernetesEnableDRA:       true,
			KubernetesVirtualGPUs:     true,
			KubernetesEnablePodUID:    true,
			KubernetesGPUIdType:       appconfig.GPUUID,
			PodResourcesKubeletSocket: socketPath,
			NvidiaResourceNames:       []string{appconfig.NvidiaResourceName},
		},
		ResourceSliceManager: newTestDRAManagerWithDevices(map[string]testDRADeviceMapping{
			draDeviceKey("poolA", "gpu-mig-shared"): {
				uuid: migShared,
				mig:  &DRAMigDeviceInfo{MIGDeviceUUID: migShared, Profile: "1g.10gb", ParentUUID: parentUUID},
			},
			draDeviceKey("poolA", "gpu-mig-solo"): {
				uuid: migSolo,
				mig:  &DRAMigDeviceInfo{MIGDeviceUUID: migSolo, Profile: "1g.10gb", ParentUUID: parentUUID},
			},
		}),
		Client:           clientset,
		labelFilterCache: newLabelFilterCache(nil, 1000),
		stopChan:         make(chan struct{}),
	}
	defer pm.Stop()
	setupMockInformer(t, pm, clientset)

	mockSystemInfo := mockdeviceinfo.NewMockProvider(ctrl)
	mockSystemInfo.EXPECT().InfoType().Return(dcgm.FE_GPU).AnyTimes()
	mockSystemInfo.EXPECT().GPUCount().Return(toUint(1)).AnyTimes()
	mockSystemInfo.EXPECT().GPU(toUint(0)).Return(deviceinfo.GPUInfo{
		DeviceInfo: dcgm.Device{UUID: parentUUID, GPU: 0},
		MigEnabled: true,
		GPUInstances: []deviceinfo.GPUInstanceInfo{
			{Info: dcgm.MigEntityInfo{NvmlInstanceId: 3}},
			{Info: dcgm.MigEntityInfo{NvmlInstanceId: 4}},
		},
	}).AnyTimes()

	metrics := collector.MetricsByCounter{
		smActiveCounter: {
			{
				GPU: "0", GPUUUID: parentUUID, GPUInstanceID: "3", MigProfile: "1g.10gb",
				Value: "0.77", Counter: smActiveCounter, Attributes: map[string]string{}, Labels: map[string]string{},
			},
			{
				GPU: "0", GPUUUID: parentUUID, GPUInstanceID: "4", MigProfile: "1g.10gb",
				Value: "0.31", Counter: smActiveCounter, Attributes: map[string]string{}, Labels: map[string]string{},
			},
		},
	}

	require.NoError(t, pm.Process(metrics, mockSystemInfo))
	result := metrics[smActiveCounter]
	require.Len(t, result, 2)
	assertNoDuplicateSeries(t, result)

	var sharedRow, soloRow *collector.Metric
	for i := range result {
		switch result[i].GPUInstanceID {
		case "3":
			sharedRow = &result[i]
		case "4":
			soloRow = &result[i]
		}
	}
	require.NotNil(t, sharedRow, "shared MIG instance row missing")
	require.NotNil(t, soloRow, "solo MIG instance row missing")

	// The shared instance is emitted once, unattributed, with device and MIG
	// identity kept.
	assert.Equal(t, "0.77", sharedRow.Value)
	assert.NotContains(t, sharedRow.Attributes, podAttribute)
	assert.NotContains(t, sharedRow.Attributes, namespaceAttribute)
	assert.NotContains(t, sharedRow.Attributes, containerAttribute)
	assert.NotContains(t, sharedRow.Attributes, uidAttribute)
	assert.NotContains(t, sharedRow.Attributes, draClaimName)
	assert.Equal(t, parentUUID, sharedRow.GPUUUID)
	assert.Equal(t, "3", sharedRow.GPUInstanceID)

	// The separate instance keeps its sole consumer's attribution.
	assert.Equal(t, "0.31", soloRow.Value)
	assert.Equal(t, "pod-c", soloRow.Attributes[podAttribute])
	assert.Equal(t, "uid-pod-c", soloRow.Attributes[uidAttribute])
	assert.Equal(t, "claim-gpu-mig-solo", soloRow.Attributes[draClaimName])
	assert.Equal(t, "4", soloRow.GPUInstanceID)
}

// TestMIGInstancePerProcessAttribution covers nonzero per-process memory on
// MIG instances using the production collector representation: per-device data
// is keyed by parent GPU UUID plus GPU instance, and the shared instance's
// two consumers each receive their own measured value with DRA metadata while
// the sibling instance stays isolated.
func TestMIGInstancePerProcessAttribution(t *testing.T) {
	t.Parallel()

	parentUUID := "GPU-mig-parent-0"
	sharedKey := getMIGMetricsKey(parentUUID, "3")
	soloKey := getMIGMetricsKey(parentUUID, "4")

	claim := func(name string) *DynamicResourceInfo {
		return &DynamicResourceInfo{
			ClaimName: name, ClaimNamespace: "gpu-test",
			DriverName: DRAGPUDriverName, PoolName: "poolA", DeviceName: name,
		}
	}
	podA := PodInfo{Name: "pod-a", Namespace: "gpu-test", UID: "uid-a", Container: "ctr", DynamicResources: claim("gpu-mig-shared")}
	podB := PodInfo{Name: "pod-b", Namespace: "gpu-test", UID: "uid-b", Container: "ctr", DynamicResources: claim("gpu-mig-shared")}
	podC := PodInfo{Name: "pod-c", Namespace: "gpu-test", UID: "uid-c", Container: "ctr", DynamicResources: claim("gpu-mig-solo")}

	dataMap := &perProcessDataMap{
		metrics: map[string]*perProcessMetrics{
			sharedKey: {pidToMemory: map[uint32]uint64{101: 2048 << 20, 102: 3072 << 20}},
			soloKey:   {pidToMemory: map[uint32]uint64{201: 1536 << 20}},
		},
		pidToPod: map[uint32]*PodInfo{101: &podA, 102: &podB, 201: &podC},
		deviceToPods: map[string][]PodInfo{
			sharedKey: {podA, podB},
			soloKey:   {podC},
		},
	}

	newMapper := func() *PodMapper { return &PodMapper{Config: &appconfig.Config{}} }
	fbMetric := func(instance string) collector.Metric {
		return collector.Metric{
			GPU: "0", GPUUUID: parentUUID, GPUInstanceID: instance, MigProfile: "1g.10gb",
			Value: "0", Attributes: map[string]string{}, Labels: map[string]string{},
		}
	}
	migMetric := func(instance string) collector.Metric {
		return collector.Metric{GPU: "0", GPUUUID: parentUUID, GPUInstanceID: instance, MigProfile: "1g.10gb"}
	}

	// Shared instance: each consumer receives its own measured value.
	result, err := newMapper().createPerProcessMetrics(
		migMetric("3"), fbUsedCounter, fbMetric("3"), dataMap,
		func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
	)
	require.NoError(t, err)
	require.Len(t, result, 2)

	byPod := map[string]string{}
	claims := map[string]string{}
	for _, m := range result {
		byPod[m.Attributes[podAttribute]] = m.Value
		claims[m.Attributes[podAttribute]] = m.Attributes[draClaimName]
	}
	assert.Equal(t, map[string]string{"pod-a": "2048", "pod-b": "3072"}, byPod)
	assert.Equal(t, map[string]string{"pod-a": "gpu-mig-shared", "pod-b": "gpu-mig-shared"}, claims)
	assertNoDuplicateSeries(t, result)

	// Sibling instance: isolated, its own consumer and value.
	result, err = newMapper().createPerProcessMetrics(
		migMetric("4"), fbUsedCounter, fbMetric("4"), dataMap,
		func() dcgm.Field_Entity_Group { return dcgm.FE_GPU },
	)
	require.NoError(t, err)
	require.Len(t, result, 1)
	assert.Equal(t, "pod-c", result[0].Attributes[podAttribute])
	assert.Equal(t, "1536", result[0].Value)
	assert.Equal(t, "gpu-mig-solo", result[0].Attributes[draClaimName])
	assert.Equal(t, "4", result[0].GPUInstanceID)
}

// TestContainerIDNameMap covers runtime container ID references from the pod
// status, including init and ephemeral containers.
func TestContainerIDNameMap(t *testing.T) {
	t.Parallel()

	pod := &v1.Pod{
		Status: v1.PodStatus{
			ContainerStatuses: []v1.ContainerStatus{
				{Name: "app", ContainerID: "containerd://aaa"},
			},
			InitContainerStatuses: []v1.ContainerStatus{
				{Name: "init", ContainerID: "containerd://bbb"},
			},
			EphemeralContainerStatuses: []v1.ContainerStatus{
				{Name: "debug", ContainerID: "containerd://ccc"},
			},
		},
	}

	got := containerIDNameMap(pod)
	assert.Equal(t, map[string]string{"aaa": "app", "bbb": "init", "ccc": "debug"}, got)

	assert.Nil(t, containerIDNameMap(&v1.Pod{}))
}
