/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cinder

import (
	"os"
	"path"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/types"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util/exec"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util/mount"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/volume"
	"github.com/golang/glog"
)

// This is the primary entrypoint for volume plugins.
func ProbeVolumePlugins() []volume.VolumePlugin {
	return []volume.VolumePlugin{&cinderPlugin{nil}}
}

type cinderPlugin struct {
	host volume.VolumeHost
}

var _ volume.VolumePlugin = &cinderPlugin{}

const (
	cinderPersistentDiskPluginName = "kubernetes.io/cinder"
)

func (plugin *cinderPlugin) Init(host volume.VolumeHost) {
	plugin.host = host
}

func (plugin *cinderPlugin) Name() string {
	return cinderPersistentDiskPluginName
}

func (plugin *cinderPlugin) CanSupport(spec *volume.Spec) bool {
	return spec.PersistentVolumeSource.CinderVolume != nil || spec.VolumeSource.CinderVolume != nil
}

func (plugin *cinderPlugin) NewBuilder(spec *volume.Spec, podRef *api.ObjectReference, _ volume.VolumeOptions) (volume.Builder, error) {
	// Inject real implementations here, test through the internal function.
	return plugin.newBuilderInternal(spec, podRef.UID, &CinderDiskUtil{}, mount.New())
}

func (plugin *cinderPlugin) newBuilderInternal(spec *volume.Spec, podUID types.UID, manager cdManager, mounter mount.Interface) (volume.Builder, error) {

	var cinder *api.CinderVolumeSource
	if spec.VolumeSource.CinderVolume != nil {
		cinder = spec.VolumeSource.CinderVolume
	} else {
		cinder = spec.PersistentVolumeSource.CinderVolume
	}

	pdName := cinder.VolID
	fsType := cinder.FSType
	readOnly := cinder.ReadOnly
	return &cinderVolume{
		podUID:  podUID,
		volName: spec.Name,
		pdName:  pdName,
		fsType:  fsType,
		//partition:   partition,
		readOnly:    readOnly,
		manager:     manager,
		mounter:     mounter,
		diskMounter: &cinderSafeFormatAndMount{mounter, exec.New()},
		plugin:      plugin,
	}, nil
}

func (plugin *cinderPlugin) NewCleaner(volName string, podUID types.UID) (volume.Cleaner, error) {
	// Inject real implementations here, test through the internal function.
	return plugin.newCleanerInternal(volName, podUID, &CinderDiskUtil{}, mount.New())
}

func (plugin *cinderPlugin) newCleanerInternal(volName string, podUID types.UID, manager cdManager, mounter mount.Interface) (volume.Cleaner, error) {

	return &cinderVolume{
		podUID:      podUID,
		volName:     volName,
		manager:     manager,
		mounter:     mounter,
		diskMounter: &cinderSafeFormatAndMount{mounter, exec.New()},
		plugin:      plugin,
	}, nil
}

// Abstract interface to PD operations.
type cdManager interface {
	// Attaches the disk to the kubelet's host machine.
	AttachDisk(cd *cinderVolume, globalPDPath string) error
	// Detaches the disk from the kubelet's host machine.
	DetachDisk(cd *cinderVolume) error
}

// cinderPersistentDisk volumes are disk resources provided by C3
// that are attached to the kubelet's host machine and exposed to the pod.
type cinderVolume struct {
	volName string

	podUID types.UID
	// Unique identifier of the PD, used to find the disk resource in the provider.
	pdName string
	// Filesystem type, optional.
	fsType string
	// Specifies the partition to mount
	//partition string
	// Specifies whether the disk will be attached as read-only.
	readOnly bool
	// Utility interface that provides API calls to the provider to attach/detach disks.
	manager cdManager
	// Mounter interface that provides system calls to mount the global path to the pod local path.
	mounter mount.Interface
	// diskMounter provides the interface that is used to mount the actual block device.
	diskMounter mount.Interface
	plugin      *cinderPlugin
}

func detachDiskLogError(cd *cinderVolume) {
	err := cd.manager.DetachDisk(cd)
	if err != nil {
		glog.Warningf("Failed to detach disk: %v (%v)", cd, err)
	}
}

func (cd *cinderVolume) SetUp() error {
	return cd.SetUpAt(cd.GetPath())
}

// SetUp attaches the disk and bind mounts to the volume path.
func (cd *cinderVolume) SetUpAt(dir string) error {

	// TODO: handle failed mounts here.
	mountpoint, err := cd.mounter.IsMountPoint(dir)
	glog.V(4).Infof("PersistentDisk set up: %s %v %v", dir, mountpoint, err)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if mountpoint {
		return nil
	}
	globalPDPath := makeGlobalPDName(cd.plugin.host, cd.pdName)
	if err := cd.manager.AttachDisk(cd, globalPDPath); err != nil {
		return err
	}

	flags := uintptr(0)
	if cd.readOnly {
		flags = mount.FlagReadOnly
	}

	if err := os.MkdirAll(dir, 0750); err != nil {
		// TODO: we should really eject the attach/detach out into its own control loop.
		detachDiskLogError(cd)
		return err
	}

	// Perform a bind mount to the full path to allow duplicate mounts of the same PD.
	err = cd.mounter.Mount(globalPDPath, dir, "", mount.FlagBind|flags, "")
	if err != nil {
		mountpoint, mntErr := cd.mounter.IsMountPoint(dir)
		if mntErr != nil {
			glog.Errorf("isMountpoint check failed: %v", mntErr)
			return err
		}
		if mountpoint {
			if mntErr = cd.mounter.Unmount(dir, 0); mntErr != nil {
				glog.Errorf("Failed to unmount: %v", mntErr)
				return err
			}
			mountpoint, mntErr := cd.mounter.IsMountPoint(dir)
			if mntErr != nil {
				glog.Errorf("isMountpoint check failed: %v", mntErr)
				return err
			}
			if mountpoint {
				// This is very odd, we don't expect it.  We'll try again next sync loop.
				glog.Errorf("%s is still mounted, despite call to unmount().  Will try again next sync loop.", cd.GetPath())
				return err
			}
		}
		os.Remove(dir)
		// TODO: we should really eject the attach/detach out into its own control loop.
		detachDiskLogError(cd)
		return err
	}

	return nil
}

func makeGlobalPDName(host volume.VolumeHost, devName string) string {
	return path.Join(host.GetPluginDir(cinderPersistentDiskPluginName), "mounts", devName)
}

func (cd *cinderVolume) GetPath() string {
	name := cinderPersistentDiskPluginName
	return cd.plugin.host.GetPodVolumeDir(cd.podUID, util.EscapeQualifiedNameForDisk(name), cd.volName)
}

func (cd *cinderVolume) TearDown() error {
	return cd.TearDownAt(cd.GetPath())
}

// Unmounts the bind mount, and detaches the disk only if the PD
// resource was the last reference to that disk on the kubelet.
func (cd *cinderVolume) TearDownAt(dir string) error {
	mountpoint, err := cd.mounter.IsMountPoint(dir)
	if err != nil {
		return err
	}
	if !mountpoint {
		return os.Remove(dir)
	}
	refs, err := mount.GetMountRefs(cd.mounter, dir)
	if err != nil {
		return err
	}
	if err := cd.mounter.Unmount(dir, 0); err != nil {
		return err
	}
	glog.Infof("successfully unmounted: %s\n", dir)

	// If refCount is 1, then all bind mounts have been removed, and the
	// remaining reference is the global mount. It is safe to detach.
	if len(refs) == 1 {
		cd.pdName = path.Base(refs[0])
		if err := cd.manager.DetachDisk(cd); err != nil {
			return err
		}
	}
	mountpoint, mntErr := cd.mounter.IsMountPoint(dir)
	if mntErr != nil {
		glog.Errorf("isMountpoint check failed: %v", mntErr)
		return err
	}
	if !mountpoint {
		if err := os.Remove(dir); err != nil {
			return err
		}
	}
	return nil
}
