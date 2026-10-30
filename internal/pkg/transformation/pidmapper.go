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
	"regexp"
	"strings"
	"sync"

	"github.com/containerd/cgroups/v3"
)

var podUIDRegex = regexp.MustCompile(`pod([a-f0-9_-]+)`)

// containerIDRegex matches a runtime container ID as the last cgroup path
// component. Containerd's CRI integration places the 64-hex container ID in
// a scope named "cri-containerd-<id>.scope" under cgroup v2, or as a plain
// path component under cgroup v1.
var containerIDRegex = regexp.MustCompile(`(?:cri-containerd-)?([0-9a-f]{64})(?:\.scope)?$`)

type pidToPodMapper struct {
	pidToUID         map[uint32]string
	pidToContainerID map[uint32]string
	cgroupWarnOnce   sync.Once
}

func newPIDToPodMapper() *pidToPodMapper {
	return &pidToPodMapper{
		pidToUID:         make(map[uint32]string),
		pidToContainerID: make(map[uint32]string),
	}
}

// getPodCgroupIdentity returns the pod UID and runtime container ID encoded
// in the process's cgroup path. Both values are cached per PID.
func (m *pidToPodMapper) getPodCgroupIdentity(pid uint32) (uid, containerID string, err error) {
	uid, uidCached := m.pidToUID[pid]
	containerID, idCached := m.pidToContainerID[pid]
	if uidCached && idCached {
		return uid, containerID, nil
	}

	cgroupPath := fmt.Sprintf("/proc/%d/cgroup", pid)
	subsystems, unified, err := cgroups.ParseCgroupFileUnified(cgroupPath)
	if err != nil {
		return "", "", fmt.Errorf("failed to parse cgroup file for PID %d: %w", pid, err)
	}

	uid = extractPodUIDFromPaths(subsystems, unified)
	if uid != "" {
		m.pidToUID[pid] = uid
	}
	containerID = extractContainerIDFromPaths(subsystems, unified)
	if containerID != "" {
		m.pidToContainerID[pid] = containerID
	}
	return uid, containerID, nil
}

func extractPodUIDFromPaths(subsystems map[string]string, unified string) string {
	for _, path := range subsystems {
		if uid := extractPodUID(path); uid != "" {
			return uid
		}
	}
	if uid := extractPodUID(unified); uid != "" {
		return uid
	}
	return ""
}

func extractPodUID(path string) string {
	matches := podUIDRegex.FindStringSubmatch(path)
	if len(matches) < 2 {
		return ""
	}
	uid := strings.ReplaceAll(matches[1], "_", "-")
	if len(uid) < 32 {
		return ""
	}
	return uid
}

func extractContainerIDFromPaths(subsystems map[string]string, unified string) string {
	for _, path := range subsystems {
		if id := extractContainerID(path); id != "" {
			return id
		}
	}
	return extractContainerID(unified)
}

// extractContainerID returns the runtime container ID from the last
// component of a cgroup path, or "" when the path carries none.
func extractContainerID(path string) string {
	if path == "" {
		return ""
	}
	segments := strings.Split(path, "/")
	matches := containerIDRegex.FindStringSubmatch(segments[len(segments)-1])
	if len(matches) < 2 {
		return ""
	}
	return matches[1]
}

func (m *pidToPodMapper) buildPIDToPodMap(pids []uint32, pods []PodInfo) map[uint32]*PodInfo {
	uidToPods := make(map[string][]*PodInfo)
	for i := range pods {
		if pods[i].UID != "" {
			uidToPods[pods[i].UID] = append(uidToPods[pods[i].UID], &pods[i])
		}
	}
	result := make(map[uint32]*PodInfo)
	for _, pid := range pids {
		uid, containerID, err := m.getPodCgroupIdentity(pid)
		if err != nil {
			slog.Debug("Failed to map PID to pod", "pid", pid, "error", err)
			m.cgroupWarnOnce.Do(func() {
				slog.Warn("Failed to map PID to pod, per-process metrics may be incomplete", "pid", pid, "error", err)
			})
			continue
		}
		if uid == "" {
			continue
		}
		candidates, ok := uidToPods[uid]
		if !ok || len(candidates) == 0 {
			continue
		}
		if entry := resolveContainerEntry(candidates, containerID); entry != nil {
			result[pid] = entry
		} else {
			// The owning pod is known but the container is not. Mark the
			// attribution unknown instead of guessing: assigning the value to
			// an arbitrary container would both fabricate that container's
			// usage and make the real owner look idle.
			unknown := *candidates[0]
			unknown.Container = ""
			unknown.VGPU = ""
			unknown.Labels = nil
			unknown.DynamicResources = nil
			unknown.containerUnknown = true
			result[pid] = &unknown
			slog.Debug("Could not resolve process container; attribution unknown for this pod",
				"pid", pid,
				"containerID", containerID,
				"pod", candidates[0].Name)
		}
	}

	return result
}

// resolveContainerEntry selects the PodInfo for the container that owns the
// process. The runtime container ID from the cgroup path is translated to a
// container name through the pod status recorded on the entries. The single
// candidate is an unambiguous fallback: with one GPU-holding container, any
// process of the pod belongs to it. With several candidates and no successful
// resolution (unrecognized cgroup layout, missing container status, or a
// container name that holds no allocation on this device), nil is returned
// and the caller must treat attribution as unknown rather than guess.
func resolveContainerEntry(candidates []*PodInfo, containerID string) *PodInfo {
	if containerID != "" {
		for _, candidate := range candidates {
			if name, ok := candidate.containerIDs[containerID]; ok {
				for _, entry := range candidates {
					if entry.Container == name {
						return entry
					}
				}
			}
		}
	}
	// Unambiguous fallback: a single candidate owns every process of the pod
	// on this device.
	if len(candidates) == 1 {
		return candidates[0]
	}
	return nil
}
