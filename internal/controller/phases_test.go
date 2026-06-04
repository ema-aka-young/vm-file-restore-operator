//nolint:goconst // Test constants are acceptable
package controller

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	restorev1alpha1 "kubevirt.io/vm-file-restore-operator/api/v1alpha1"
)

func TestGetSourceName_FromPVC(t *testing.T) {
	vmfr := &restorev1alpha1.VirtualMachineFileRestore{
		Spec: restorev1alpha1.VirtualMachineFileRestoreSpec{
			Source: restorev1alpha1.RestoreSource{
				PVC: &restorev1alpha1.PVCSource{
					Name: "my-backup-pvc",
				},
			},
		},
	}

	sourceName := getSourceName(vmfr)

	expected := "my-backup-pvc"
	if sourceName != expected {
		t.Errorf("expected sourceName '%s', got '%s'", expected, sourceName)
	}
}

func TestGetSourceName_FromSnapshot(t *testing.T) {
	vmfr := &restorev1alpha1.VirtualMachineFileRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-restore",
		},
		Spec: restorev1alpha1.VirtualMachineFileRestoreSpec{
			Source: restorev1alpha1.RestoreSource{
				Snapshot: &restorev1alpha1.VolumeSnapshotSource{
					Name: "win11-pvc-snapshot-1",
				},
			},
		},
	}

	sourceName := getSourceName(vmfr)

	expected := "win11-pvc-snapshot-1"
	if sourceName != expected {
		t.Errorf("expected sourceName '%s', got '%s'", expected, sourceName)
	}
}

func TestGetSourceName_Fallback(t *testing.T) {
	vmfr := &restorev1alpha1.VirtualMachineFileRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name: "my-restore",
		},
		Spec: restorev1alpha1.VirtualMachineFileRestoreSpec{
			Source: restorev1alpha1.RestoreSource{
				// No PVC or Snapshot specified
			},
		},
	}

	sourceName := getSourceName(vmfr)

	expected := "my-restore"
	if sourceName != expected {
		t.Errorf("expected sourceName '%s' (fallback to VMFR name), got '%s'", expected, sourceName)
	}
}
