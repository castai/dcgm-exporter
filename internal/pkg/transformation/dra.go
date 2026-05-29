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
	"log/slog"
	"time"

	resourcev1 "k8s.io/api/resource/v1"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	podresourcesapi "k8s.io/kubelet/pkg/apis/podresources/v1"

	"github.com/NVIDIA/dcgm-exporter/internal/pkg/kubeclient"
)

const (
	informerResyncPeriod = 10 * time.Minute
)

type resourceSliceAdapter interface {
	GetDevices() []deviceAdapter
}

type deviceAdapter interface {
	GetName() string
	GetAttribute(key string) string
	HasAttributes() bool
}

type v1ResourceSliceAdapter struct {
	slice *resourcev1.ResourceSlice
}

func (a *v1ResourceSliceAdapter) GetDevices() []deviceAdapter {
	devices := make([]deviceAdapter, len(a.slice.Spec.Devices))
	for i := range a.slice.Spec.Devices {
		devices[i] = &v1DeviceAdapter{device: &a.slice.Spec.Devices[i]}
	}
	return devices
}

type v1DeviceAdapter struct {
	device *resourcev1.Device
}

func (a *v1DeviceAdapter) GetName() string {
	return a.device.Name
}

func (a *v1DeviceAdapter) HasAttributes() bool {
	return a.device.Attributes != nil
}

func (a *v1DeviceAdapter) GetAttribute(key string) string {
	if a.device.Attributes == nil {
		return ""
	}
	attrKey := resourcev1.QualifiedName(key)
	if attr, ok := a.device.Attributes[attrKey]; ok && attr.StringValue != nil {
		return *attr.StringValue
	}
	return ""
}

type v1beta1ResourceSliceAdapter struct {
	slice *resourcev1beta1.ResourceSlice
}

func (a *v1beta1ResourceSliceAdapter) GetDevices() []deviceAdapter {
	devices := make([]deviceAdapter, len(a.slice.Spec.Devices))
	for i := range a.slice.Spec.Devices {
		devices[i] = &v1beta1DeviceAdapter{device: &a.slice.Spec.Devices[i]}
	}
	return devices
}

type v1beta1DeviceAdapter struct {
	device *resourcev1beta1.Device
}

func (a *v1beta1DeviceAdapter) GetName() string {
	return a.device.Name
}

func (a *v1beta1DeviceAdapter) HasAttributes() bool {
	return a.device.Basic != nil && a.device.Basic.Attributes != nil
}

func (a *v1beta1DeviceAdapter) GetAttribute(key string) string {
	if a.device.Basic == nil || a.device.Basic.Attributes == nil {
		return ""
	}
	attrKey := resourcev1beta1.QualifiedName(key)
	if attr, ok := a.device.Basic.Attributes[attrKey]; ok && attr.StringValue != nil {
		return *attr.StringValue
	}
	return ""
}

func supportsResourceSliceGV(client kubernetes.Interface, groupVersion string) bool {
	resources, err := client.Discovery().ServerResourcesForGroupVersion(groupVersion)
	if err != nil {
		slog.Warn("Discovery failed for groupVersion", "groupVersion", groupVersion, "error", err)
		return false
	}
	for _, r := range resources.APIResources {
		if r.Name == "resourceslices" {
			return true
		}
	}
	return false
}

func NewDRAResourceSliceManager() (*DRAResourceSliceManager, error) {
	client, err := kubeclient.GetKubeClient()
	if err != nil {
		return nil, fmt.Errorf("error getting kube client: %w", err)
	}

	const (
		resourceGVV1      = "resource.k8s.io/v1"
		resourceGVV1beta1 = "resource.k8s.io/v1beta1"
	)

	v1Served := supportsResourceSliceGV(client, resourceGVV1)
	v1beta1Served := supportsResourceSliceGV(client, resourceGVV1beta1)
	if !v1Served && !v1beta1Served {
		slog.Warn("Neither resource.k8s.io/v1 nor v1beta1 ResourceSlice API is served; DRA labels will not be available")
		return nil, nil
	}

	// Prefer v1 when served; fall back to v1beta1.
	var selected string
	switch {
	case v1Served:
		selected = "v1"
	case v1beta1Served:
		selected = "v1beta1"
	}

	factory := informers.NewSharedInformerFactory(client, informerResyncPeriod)

	var informer cache.SharedIndexInformer
	switch selected {
	case "v1":
		informer = factory.Resource().V1().ResourceSlices().Informer()
		err = informer.AddIndexers(cache.Indexers{
			"poolName": func(obj interface{}) ([]string, error) {
				rs, ok := obj.(*resourcev1.ResourceSlice)
				if !ok {
					return nil, nil
				}
				return []string{rs.Spec.Pool.Name}, nil
			},
		})
		if err != nil {
			return nil, fmt.Errorf("error adding pool indexer to v1 ResourceSlice informer: %w", err)
		}
	case "v1beta1":
		informer = factory.Resource().V1beta1().ResourceSlices().Informer()
		err = informer.AddIndexers(cache.Indexers{
			"poolName": func(obj interface{}) ([]string, error) {
				rs, ok := obj.(*resourcev1beta1.ResourceSlice)
				if !ok {
					return nil, nil
				}
				return []string{rs.Spec.Pool.Name}, nil
			},
		})
		if err != nil {
			return nil, fmt.Errorf("error adding pool indexer to v1beta1 ResourceSlice informer: %w", err)
		}
	}

	m := &DRAResourceSliceManager{
		informer:        informer,
		sliceAPIVersion: selected,
	}

	factory.Start(wait.NeverStop)

	synced := cache.WaitForCacheSync(wait.NeverStop, informer.HasSynced)
	if !synced {
		factory.Shutdown()
		return nil, fmt.Errorf("ResourceSlice informer cache sync failed")
	}

	slog.Info("ResourceSlice API informer synced successfully", "apiVersion", selected)
	return m, nil
}

func lookupDRADeviceInAdapter(pool, device string, adapter resourceSliceAdapter) (string, *DRAMigDeviceInfo) {
	for _, dev := range adapter.GetDevices() {
		if !dev.HasAttributes() {
			continue
		}
		if dev.GetName() != device {
			continue
		}

		deviceType := dev.GetAttribute("type")
		switch deviceType {
		case "mig":
			parentUUID := dev.GetAttribute("parentUUID")
			profile := dev.GetAttribute("profile")
			migUUID := dev.GetAttribute("uuid")
			if parentUUID != "" {
				migInfo := &DRAMigDeviceInfo{
					MIGDeviceUUID: migUUID,
					Profile:       profile,
					ParentUUID:    parentUUID,
				}
				slog.Debug("Found MIG device", "pool", pool, "device", device, "parentUUID", parentUUID)
				return parentUUID, migInfo
			}
		case "gpu":
			uuid := dev.GetAttribute("uuid")
			if uuid != "" {
				slog.Debug("Found GPU device", "pool", pool, "device", device, "uuid", uuid)
				return uuid, nil
			}
		default:
			slog.Warn("Device has unknown type", "pool", pool, "device", device, "type", deviceType)
		}
	}
	return "", nil
}

// GetDeviceInfo queries the informer cache directly using a type-switch over
// the stored objects, avoiding version-specific helper methods.
func (m *DRAResourceSliceManager) GetDeviceInfo(pool, device string) (string, *DRAMigDeviceInfo) {
	if m.informer == nil {
		return "", nil
	}

	items, err := m.informer.GetIndexer().ByIndex("poolName", pool)
	if err != nil {
		slog.Error("Error listing ResourceSlices by pool index", "pool", pool, "error", err)
		return "", nil
	}

	for _, item := range items {
		var adapter resourceSliceAdapter
		var driver string
		switch rs := item.(type) {
		case *resourcev1.ResourceSlice:
			driver, adapter = rs.Spec.Driver, &v1ResourceSliceAdapter{slice: rs}
		case *resourcev1beta1.ResourceSlice:
			driver, adapter = rs.Spec.Driver, &v1beta1ResourceSliceAdapter{slice: rs}
		default:
			continue
		}
		if driver != DRAGPUDriverName {
			continue
		}
		if mappingKey, migInfo := lookupDRADeviceInAdapter(pool, device, adapter); mappingKey != "" {
			return mappingKey, migInfo
		}
	}

	slog.Debug("No UUID found for DRA device", "pool", pool, "device", device)
	return "", nil
}

type DynamicResourceMapping struct {
	MappingKey string
	Info       *DynamicResourceInfo
}

func (m *DRAResourceSliceManager) GetDynamicResourceMappings(resource *podresourcesapi.DynamicResource) []DynamicResourceMapping {
	if resource == nil {
		return nil
	}

	mappings := make([]DynamicResourceMapping, 0, len(resource.GetClaimResources()))
	for _, claimResource := range resource.GetClaimResources() {
		draDriverName := claimResource.GetDriverName()
		if draDriverName != DRAGPUDriverName {
			continue
		}

		draPoolName := claimResource.GetPoolName()
		draDeviceName := claimResource.GetDeviceName()

		mappingKey, migInfo := m.GetDeviceInfo(draPoolName, draDeviceName)
		if mappingKey == "" {
			slog.Debug("No UUID for DRA claim resource", "pool", draPoolName, "device", draDeviceName)
			continue
		}

		drInfo := &DynamicResourceInfo{
			ClaimName:      resource.GetClaimName(),
			ClaimNamespace: resource.GetClaimNamespace(),
			DriverName:     draDriverName,
			PoolName:       draPoolName,
			DeviceName:     draDeviceName,
		}
		if migInfo != nil {
			drInfo.MIGInfo = migInfo
		}

		mappings = append(mappings, DynamicResourceMapping{
			MappingKey: mappingKey,
			Info:       drInfo,
		})
	}

	return mappings
}
