package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	v1 "kubevirt.io/api/core/v1"
	cdiv1beta1 "kubevirt.io/containerized-data-importer-api/pkg/apis/core/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	restorev1alpha1 "kubevirt.io/vm-file-restore-operator/api/v1alpha1"
)

// NewSubresourceRESTClient creates a REST client for the KubeVirt subresource API
// (subresources.kubevirt.io/v1). Used for addvolume/removevolume calls that work
// with both HotplugVolumes and DeclarativeHotplugVolumes feature gates.
func NewSubresourceRESTClient(cfg *rest.Config) (rest.Interface, error) {
	restCfg := rest.CopyConfig(cfg)
	restCfg.GroupVersion = &schema.GroupVersion{Group: "subresources.kubevirt.io", Version: "v1"}
	restCfg.APIPath = "/apis"
	restCfg.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	return rest.RESTClientFor(restCfg)
}

// GetVolumeName returns the volume name for a given restore CR name.
// The volume name is used as the volume name, disk name, and serial number for guest OS detection.
// Panics if crName is empty (caller error - should never happen with valid K8s objects).
func GetVolumeName(crName string) string {
	if crName == "" {
		panic("GetVolumeName called with empty crName")
	}
	return crName + "-restore"
}

// HotplugVolume hotplugs a restore volume to the target VM using the KubeVirt subresource API.
// This works with both HotplugVolumes and DeclarativeHotplugVolumes feature gates.
// apiReader is a non-cached reader used to read DataVolume status immediately after creation.
func HotplugVolume(ctx context.Context, c client.Client, apiReader client.Reader, subresourceClient rest.Interface, vmfr *restorev1alpha1.VirtualMachineFileRestore, vm *v1.VirtualMachine) error {
	logger := log.FromContext(ctx)
	volumeName := GetVolumeName(vmfr.Name)

	// Issue #16: Check for other restore operations on this VM
	for _, vol := range vm.Spec.Template.Spec.Volumes {
		if strings.HasSuffix(vol.Name, "-restore") && vol.Name != volumeName {
			return fmt.Errorf("another restore is in progress (volume %s exists), cannot hotplug", vol.Name)
		}
	}

	// Issue #15: Check both volume and disk for idempotency
	volumeExists := false
	diskExists := false
	for _, vol := range vm.Spec.Template.Spec.Volumes {
		if vol.Name == volumeName {
			volumeExists = true
			break
		}
	}
	for _, disk := range vm.Spec.Template.Spec.Domain.Devices.Disks {
		if disk.Name == volumeName {
			diskExists = true
			break
		}
	}

	if volumeExists && diskExists {
		// Both exist, already hotplugged
		return nil
	}
	if volumeExists || diskExists {
		// Partial state - this shouldn't happen but handle it
		logger.Error(fmt.Errorf("partial hotplug detected"), "Inconsistent state",
			"volumeExists", volumeExists, "diskExists", diskExists)
		return fmt.Errorf("partial hotplug detected (volume=%v, disk=%v), needs cleanup",
			volumeExists, diskExists)
	}

	// Build volume source based on the restore source type
	var volumeSource v1.VolumeSource

	if vmfr.Spec.Source.PVC != nil {
		// Validate namespace (KubeVirt doesn't support cross-namespace PVC refs)
		pvcNamespace := vmfr.Spec.Source.PVC.Namespace
		if pvcNamespace == "" {
			pvcNamespace = vmfr.Namespace
		}
		if pvcNamespace != vmfr.Namespace {
			return fmt.Errorf("cross-namespace PVC restore not supported: PVC is in %s, VM is in %s", pvcNamespace, vmfr.Namespace)
		}

		// PVC source: use directly with hotplug
		volumeSource = v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: vmfr.Spec.Source.PVC.Name,
				},
				Hotpluggable: true,
			},
		}
	} else if vmfr.Spec.Source.Snapshot != nil {
		// Snapshot source: create DataVolume with empty storage spec
		// DataVolume will automatically inherit access mode, volume mode, and size from snapshot
		snapshotNamespace := vmfr.Spec.Source.Snapshot.Namespace
		if snapshotNamespace == "" {
			snapshotNamespace = vmfr.Namespace
		}

		dataVolume := &cdiv1beta1.DataVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: vmfr.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by": "vm-file-restore-operator",
					"filerestore.kubevirt.io/name": vmfr.Name,
				},
			},
			Spec: cdiv1beta1.DataVolumeSpec{
				Source: &cdiv1beta1.DataVolumeSource{
					Snapshot: &cdiv1beta1.DataVolumeSourceSnapshot{
						Namespace: snapshotNamespace,
						Name:      vmfr.Spec.Source.Snapshot.Name,
					},
				},
				Storage: &cdiv1beta1.StorageSpec{
					AccessModes: []corev1.PersistentVolumeAccessMode{
						corev1.ReadWriteOnce,
					},
				},
			},
		}

		// Create DataVolume, verify if already exists
		if err := c.Create(ctx, dataVolume); err != nil {
			if !errors.IsAlreadyExists(err) {
				return fmt.Errorf("failed to create DataVolume from snapshot: %w", err)
			}
		}

		// Wait for DataVolume to be Succeeded (use direct API reader to avoid cache lag)
		existing := &cdiv1beta1.DataVolume{}
		if err := apiReader.Get(ctx, client.ObjectKey{Name: volumeName, Namespace: vmfr.Namespace}, existing); err != nil {
			return fmt.Errorf("failed to get DataVolume: %w", err)
		}
		if existing.Status.Phase != cdiv1beta1.Succeeded {
			// This is a transient condition - caller will retry
			return NewTransientError(fmt.Sprintf("DataVolume is provisioning (phase: %s), will retry", existing.Status.Phase))
		}

		volumeSource = v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				PersistentVolumeClaimVolumeSource: corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: volumeName,
				},
				Hotpluggable: true,
			},
		}
	} else if vmfr.Spec.Source.Remote != nil {
		return fmt.Errorf("remote sources not yet supported")
	} else {
		return fmt.Errorf("no valid source specified")
	}

	addVolumeOpts := &v1.AddVolumeOptions{
		Name: volumeName,
		Disk: &v1.Disk{
			Name: volumeName,
			DiskDevice: v1.DiskDevice{
				Disk: &v1.DiskTarget{
					Bus: v1.DiskBusSCSI,
				},
			},
			Serial: volumeName,
		},
		VolumeSource: &v1.HotplugVolumeSource{
			PersistentVolumeClaim: volumeSource.PersistentVolumeClaim,
		},
	}

	body, err := json.Marshal(addVolumeOpts)
	if err != nil {
		return fmt.Errorf("marshal AddVolumeOptions: %w", err)
	}

	if err := subresourceClient.Put().
		Namespace(vmfr.Namespace).
		Resource("virtualmachines").
		Name(vm.Name).
		SubResource("addvolume").
		Body(body).
		Do(ctx).
		Error(); err != nil {
		if errors.IsConflict(err) {
			return nil
		}
		return fmt.Errorf("failed to hotplug volume via subresource API: %w", err)
	}

	return nil
}

// UnplugVolume removes the restore volume from the target VM using the KubeVirt subresource API.
// It also cleans up temporary DataVolumes created for snapshot sources.
func UnplugVolume(ctx context.Context, c client.Client, subresourceClient rest.Interface, vmfr *restorev1alpha1.VirtualMachineFileRestore, vm *v1.VirtualMachine) error {
	volumeName := GetVolumeName(vmfr.Name)

	removeVolumeOpts := &v1.RemoveVolumeOptions{
		Name: volumeName,
	}

	body, err := json.Marshal(removeVolumeOpts)
	if err != nil {
		return fmt.Errorf("marshal RemoveVolumeOptions: %w", err)
	}

	if err := subresourceClient.Put().
		Namespace(vmfr.Namespace).
		Resource("virtualmachines").
		Name(vm.Name).
		SubResource("removevolume").
		Body(body).
		Do(ctx).
		Error(); err != nil {
		if !errors.IsNotFound(err) {
			return fmt.Errorf("failed to unplug volume via subresource API: %w", err)
		}
	}

	// If snapshot source, delete DataVolume (which will delete the PVC)
	if vmfr.Spec.Source.Snapshot != nil {
		dataVolume := &cdiv1beta1.DataVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name:      volumeName,
				Namespace: vmfr.Namespace,
			},
		}
		if err := c.Delete(ctx, dataVolume); err != nil && !errors.IsNotFound(err) {
			return fmt.Errorf("delete DataVolume: %w", err)
		}
	}

	return nil
}
