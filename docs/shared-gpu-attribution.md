# Shared GPU metric attribution contract

This document describes how dcgm-exporter attributes GPU metrics when a GPU
(or MIG instance) is shared between consumers, and why.

## Background

The exporter enriches DCGM metrics with Kubernetes pod attribution. With DRA
sharing (`KUBERNETES_ENABLE_DRA=true`, `KUBERNETES_VIRTUAL_GPUS=true`,
`DCGM_EXPORTER_KUBERNETES_GPU_ID_TYPE=device-name`), GPUs are shared between
pods through ResourceClaims with `sharing.strategy: MPS` or `TimeSlicing`.

DCGM and NVML expose **no per-client compute breakdown** for a shared GPU:

- Under MPS, client warps interleave concurrently on the same SMs through the
  MPS server; even per-process `GPU_UTIL` is attributed to the MPS server
  process, not to tenant pods.
- Under time-slicing, per-process `GPU_UTIL` attributes cleanly (execution is
  sequential), but profiling counters (`SM_ACTIVE`, `SM_OCCUPANCY`, pipes,
  tensor, DRAM) remain device-level only.
- Per-client **memory** is the exception: the driver tracks allocations, so
  `FB_USED` is genuinely measurable per process.

Before this contract, the exporter attributed every device-level metric to
*every* co-located pod: each pod's series carried the full device value
(for example, `SM_ACTIVE 0.98` on both tenants of a shared T4). Downstream
consumers (kvisor into ClickHouse, workload-optimization) read those as
genuine per-pod measurements, and the recommendation engine flipped shared
workloads back to exclusive GPUs.

## The attribution boundary

The attribution identity is the **pod container**: one (pod, container) pair.
This is the finest identity the available telemetry supports:

- Process identity is resolvable to a pod UID and a runtime container ID via
  the process cgroup path; the container ID maps to a container name through
  the pod status. Per-process values are aggregated per container per device.
- The vGPU allocation is **not** part of the identity: cgroups identify the
  container, not which virtual replica of a physical GPU its processes use.
  A container holding several replicas emits one measurement per device; the
  vgpu label is retained only when every mapping agrees on it.
- When a process's owning pod is known but its container is not (unrecognized
  cgroup layout, missing container status, or the resolved container holds no
  allocation on this device), the attribution is **unknown**: the value is
  counted toward no container, and the pod's containers on that device are
  not reported as idle — one of them may own the unresolved process.
- The downstream consumers key records by (pod, container): the kvisor GPU
  mapper builds records keyed by device, pod, namespace, container, and the
  workload-optimization reader queries `gpu_metrics` with an exact
  `container = @container` filter. Rows without a container label are
  invisible to those queries, so a pod-level aggregate cannot substitute for
  per-container rows.

## Contract

For a device held by **multiple distinct consumers** (two pods, or two
containers, or two vGPU allocations):

1. Device-level metrics (everything except `DCGM_FI_DEV_GPU_UTIL` and
   `DCGM_FI_DEV_FB_USED`) are emitted **once, without workload attribution**.
   Device metadata (UUID, device name, GPU index, MIG identifiers) is
   preserved on the unattributed row.
2. Per-process metrics keep per-consumer attribution with the existing DRA
   claim labels, each carrying that consumer's own process-derived value.
   Per-process handling always runs before shared-device suppression.
3. A pod UID, pod labels, container, vGPU allocation, or claim identity is
   never attached to a series representing several consumers.
4. The device reading is never divided by the consumer count and presented
   as a measured per-pod value.

For a **single consumer**: the device value is attributed to that consumer
and NVML process queries are skipped (the device value equals the consumer's
usage). For **zero consumers**: the original device reading is kept, never
forced to zero.

Repeated mappings of the same consumer (via both the device plugin and a DRA
claim, or multiple claim slices) collapse to one series; the merged entry
keeps the DRA claim metadata. Consumers of separate MIG instances never merge;
instance-specific suppression and attribution apply per instance.

## Known limitations

- Under MPS, per-pod `GPU_UTIL` reads zero: the MPS server owns the compute
  attribution. Per-client memory remains the useful per-pod signal.
- When a process's container cannot be resolved (unrecognized cgroup layout
  or the informer has not yet observed the container status), the value is
  attributed to no container and the pod's containers on that device emit no
  rows rather than fabricated zeros.
- Per-process attribution for MIG instances is limited: DRA MIG metrics carry
  MIG UUIDs while per-process data is keyed by parent UUID and GPU instance.
- Deployment of this contract does not repair historical duplicated rows in
  ClickHouse; consumers must handle or age out existing data.
