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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/GoogleCloudPlatform/kubernetes/pkg/cloudprovider/openstack"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util/exec"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/util/mount"
	"github.com/golang/glog"
)

type CinderDiskUtil struct{}

// Attaches a disk specified by a volume.CinderPersistenDisk to the current kubelet.
// Mounts the disk to it's global path.
func (util *CinderDiskUtil) AttachDisk(cd *cinderVolume, globalPDPath string) error {

	flags := uintptr(0)
	if cd.readOnly {
		flags = mount.FlagReadOnly
	}
	cloud := cd.plugin.host.GetCloudProvider()
	if cloud == nil {
		glog.Errorf("Cloud provider not initialized properly")
		return errors.New("Cloud provider not initialized properly")
	}
	diskid, err := cloud.(*openstack.OpenStack).AttachDisk(cd.pdName)
	if err != nil {
		return err
	}

	var devicePath string
	numTries := 0
	for {
		devicePath = makeDevicePath(diskid)
		//probe the attached vol so that symlink in /dev/disk/by-id is created
		probeAttachedVolume()

		_, err := os.Stat(devicePath)
		if err == nil {
			break
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		numTries++
		if numTries == 10 {
			return errors.New("Could not attach disk: Timeout after 60s")
		}
		time.Sleep(time.Second * 6)
	}

	mountpoint, err := cd.mounter.IsMountPoint(globalPDPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(globalPDPath, 0750); err != nil {
				return err
			}
			mountpoint = false
		} else {
			return err
		}
	}
	if !mountpoint {
		glog.Infof("Safe mount for device: %q\n", devicePath)
		err = cd.diskMounter.Mount(devicePath, globalPDPath, cd.fsType, flags, "")
		if err != nil {
			os.Remove(globalPDPath)
			return err
		}
		glog.Infof("Safe mount successful: %q\n", devicePath)
	}
	return nil
}

func makeDevicePath(diskid string) string {

	files, _ := ioutil.ReadDir("/dev/disk/by-id/")
	for _, f := range files {
		if strings.Contains(f.Name(), "virtio-") {
			devid_prefix := f.Name()[len("virtio-"):len(f.Name())]
			if strings.Contains(diskid, devid_prefix) {
				glog.Infof("Found disk attached as %q; full devicepath: %s\n", f.Name(), path.Join("/dev/disk/by-id/", f.Name()))
				return path.Join("/dev/disk/by-id/", f.Name())
			}
		}
	}
	glog.Warningf("Failed to find device for the diskid: %q\n", diskid)
	return ""
}

// Unmounts the device and detaches the disk from the kubelet's host machine.
func (util *CinderDiskUtil) DetachDisk(cd *cinderVolume) error {

	globalPDPath := makeGlobalPDName(cd.plugin.host, cd.pdName)

	if err := cd.mounter.Unmount(globalPDPath, 0); err != nil {
		return err
	}
	if err := os.Remove(globalPDPath); err != nil {
		return err
	}
	glog.Infof("Successfully unmounted main device: %s\n", globalPDPath)

	cloud := cd.plugin.host.GetCloudProvider()
	if cloud == nil {
		glog.Errorf("Cloud provider not initialized properly")
		return errors.New("Cloud provider not initialized properly")
	}

	if err := cloud.(*openstack.OpenStack).DetachDisk(cd.pdName); err != nil {
		return err
	}
	glog.Infof("Successfully detached cinder volume from kubelet\n")
	return nil
}

type cinderSafeFormatAndMount struct {
	mount.Interface
	runner exec.Interface
}

/** The functions below depend on the following executables; This will have to be ported to more generic implementations
/bin/lsblk
/sbin/mkfs.ext3 or /sbin/mkfs.ext4
/usr/bin/udevadm
**/
func (diskmounter *cinderSafeFormatAndMount) Mount(device string, target string, fstype string, flags uintptr, data string) error {

	fmtRequired, err := isFormatRequired(device, fstype, diskmounter)
	if err != nil {
		glog.Warningf("Failed to determine if formating is required: %v\n", err)
		//return err
	}
	if fmtRequired {
		glog.Infof("Formatting of the vol required...will perform now")
		if _, err := formatVolume(device, fstype, diskmounter); err != nil {
			glog.Warningf("Failed to format volume: %v\n", err)
			return err
		}
	}
	return diskmounter.Interface.Mount(device, target, fstype, flags, data)

}

func isFormatRequired(devicePath string, fstype string, exec *cinderSafeFormatAndMount) (bool, error) {

	args := []string{"-f", devicePath}
	glog.Infof("exec-ing: /bin/lsblk %v\n", args)
	cmd := exec.runner.Command("/bin/lsblk", args...)
	dataOut, err := cmd.CombinedOutput()
	if err != nil {
		glog.Warningf("error running /bin/lsblk\n%s", string(dataOut))
		return false, err
	}
	if len(string(dataOut)) > 0 {
		if strings.Contains(string(dataOut), fstype) {
			return false, nil
		} else {
			return true, nil
		}
	} else {
		glog.Warningf("Failed to get any response from /bin/lsblk")
		return false, errors.New("Failed to get reponse from /bin/lsblk")
	}
	glog.Warningf("Unknown error occured executing /bin/lsblk")
	return false, errors.New("Unknown error occured executing /bin/lsblk")
}

func formatVolume(devicePath string, fstype string, exec *cinderSafeFormatAndMount) (bool, error) {

	if "ext4" != fstype && "ext3" != fstype {
		glog.Warningf("Unsupported format type: %q\n", fstype)
		return false, errors.New(fmt.Sprint("Unsupported format type: %q\n", fstype))
	}
	args := []string{devicePath}
	cmd := exec.runner.Command(fmt.Sprintf("/sbin/mkfs.%s", fstype), args...)
	dataOut, err := cmd.CombinedOutput()
	if err != nil {
		glog.Warningf("error running /sbin/mkfs for fstype: %q \n%s", fstype, string(dataOut))
		return false, err
	}
	glog.Infof("Successfully formated device: %q with fstype %q; output:\n %q\n,", devicePath, fstype, string(dataOut))
	return true, err

}

func probeAttachedVolume() error {
	executor := exec.New()
	args := []string{"trigger"}
	cmd := executor.Command("/usr/bin/udevadm", args...)
	_, err := cmd.CombinedOutput()
	if err != nil {
		glog.Errorf("error running udevadm trigger %v\n", err)
		return err
	}
	glog.Infof("Successfully probed all attachments")
	return nil
}
