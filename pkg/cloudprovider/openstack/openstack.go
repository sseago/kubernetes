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

package openstack

import (
	"errors"
	"fmt"
	"io"
	"net"
	ossys "os"
	"regexp"
	"strings"
	"time"

	"code.google.com/p/gcfg"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/api"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/api/resource"
	"github.com/GoogleCloudPlatform/kubernetes/pkg/cloudprovider"
	"github.com/golang/glog"
	"github.com/rackspace/gophercloud"
	"github.com/rackspace/gophercloud/openstack"
	"github.com/rackspace/gophercloud/openstack/blockstorage/v1/volumes"
	"github.com/rackspace/gophercloud/openstack/compute/v2/extensions/volumeattach"
	"github.com/rackspace/gophercloud/openstack/compute/v2/flavors"
	"github.com/rackspace/gophercloud/openstack/compute/v2/servers"
	"github.com/rackspace/gophercloud/openstack/networking/v2/extensions/lbaas/members"
	"github.com/rackspace/gophercloud/openstack/networking/v2/extensions/lbaas/monitors"
	"github.com/rackspace/gophercloud/openstack/networking/v2/extensions/lbaas/pools"
	"github.com/rackspace/gophercloud/openstack/networking/v2/extensions/lbaas/vips"
	"github.com/rackspace/gophercloud/pagination"
)

var ErrNotFound = errors.New("Failed to find object")
var ErrMultipleResults = errors.New("Multiple results where only one expected")
var ErrNoAddressFound = errors.New("No address found for host")
var ErrAttrNotFound = errors.New("Expected attribute not found")

const (
	MiB = 1024 * 1024
	GB  = 1000 * 1000 * 1000
)

// encoding.TextUnmarshaler interface for time.Duration
type MyDuration struct {
	time.Duration
}

func (d *MyDuration) UnmarshalText(text []byte) error {
	res, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	d.Duration = res
	return nil
}

type LoadBalancerOpts struct {
	SubnetId          string     `gcfg:"subnet-id"` // required
	CreateMonitor     bool       `gcfg:"create-monitor"`
	MonitorDelay      MyDuration `gcfg:"monitor-delay"`
	MonitorTimeout    MyDuration `gcfg:"monitor-timeout"`
	MonitorMaxRetries uint       `gcfg:"monitor-max-retries"`
}

// OpenStack is an implementation of cloud provider Interface for OpenStack.
type OpenStack struct {
	provider *gophercloud.ProviderClient
	region   string
	lbOpts   LoadBalancerOpts
}

type Config struct {
	Global struct {
		AuthUrl    string `gcfg:"auth-url"`
		Username   string
		UserId     string `gcfg:"user-id"`
		Password   string
		ApiKey     string `gcfg:"api-key"`
		TenantId   string `gcfg:"tenant-id"`
		TenantName string `gcfg:"tenant-name"`
		DomainId   string `gcfg:"domain-id"`
		DomainName string `gcfg:"domain-name"`
		Region     string
	}
	LoadBalancer LoadBalancerOpts
}

func init() {
	cloudprovider.RegisterCloudProvider("openstack", func(config io.Reader) (cloudprovider.Interface, error) {
		cfg, err := readConfig(config)
		if err != nil {
			return nil, err
		}
		return newOpenStack(cfg)
	})
}

func (cfg Config) toAuthOptions() gophercloud.AuthOptions {
	return gophercloud.AuthOptions{
		IdentityEndpoint: cfg.Global.AuthUrl,
		Username:         cfg.Global.Username,
		UserID:           cfg.Global.UserId,
		Password:         cfg.Global.Password,
		APIKey:           cfg.Global.ApiKey,
		TenantID:         cfg.Global.TenantId,
		TenantName:       cfg.Global.TenantName,

		// Persistent service, so we need to be able to renew tokens.
		AllowReauth: true,
	}
}

func readConfig(config io.Reader) (Config, error) {
	if config == nil {
		err := fmt.Errorf("no OpenStack cloud provider config file given")
		return Config{}, err
	}

	var cfg Config
	err := gcfg.ReadInto(&cfg, config)
	return cfg, err
}

func newOpenStack(cfg Config) (*OpenStack, error) {
	provider, err := openstack.AuthenticatedClient(cfg.toAuthOptions())
	if err != nil {
		return nil, err
	}

	os := OpenStack{
		provider: provider,
		region:   cfg.Global.Region,
		lbOpts:   cfg.LoadBalancer,
	}
	return &os, nil
}

type Instances struct {
	compute            *gophercloud.ServiceClient
	flavor_to_resource map[string]*api.NodeResources // keyed by flavor id
}

// Instances returns an implementation of Instances for OpenStack.
func (os *OpenStack) Instances() (cloudprovider.Instances, bool) {
	glog.V(4).Info("openstack.Instances() called")

	compute, err := openstack.NewComputeV2(os.provider, gophercloud.EndpointOpts{
		Region: os.region,
	})
	if err != nil {
		glog.Warningf("Failed to find compute endpoint: %v", err)
		return nil, false
	}

	pager := flavors.ListDetail(compute, nil)

	flavor_to_resource := make(map[string]*api.NodeResources)
	err = pager.EachPage(func(page pagination.Page) (bool, error) {
		flavorList, err := flavors.ExtractFlavors(page)
		if err != nil {
			return false, err
		}
		for _, flavor := range flavorList {
			rsrc := api.NodeResources{
				Capacity: api.ResourceList{
					api.ResourceCPU:            *resource.NewQuantity(int64(flavor.VCPUs), resource.DecimalSI),
					api.ResourceMemory:         *resource.NewQuantity(int64(flavor.RAM)*MiB, resource.BinarySI),
					"openstack.org/disk":       *resource.NewQuantity(int64(flavor.Disk)*GB, resource.DecimalSI),
					"openstack.org/rxTxFactor": *resource.NewMilliQuantity(int64(flavor.RxTxFactor)*1000, resource.DecimalSI),
					"openstack.org/swap":       *resource.NewQuantity(int64(flavor.Swap)*MiB, resource.BinarySI),
				},
			}
			flavor_to_resource[flavor.ID] = &rsrc
		}
		return true, nil
	})
	if err != nil {
		glog.Warningf("Failed to find compute flavors: %v", err)
		return nil, false
	}

	glog.V(3).Infof("Found %v compute flavors", len(flavor_to_resource))
	glog.V(1).Info("Claiming to support Instances")

	return &Instances{compute, flavor_to_resource}, true
}

func (i *Instances) List(name_filter string) ([]string, error) {
	glog.V(4).Infof("openstack List(%v) called", name_filter)

	opts := servers.ListOpts{
		Name:   name_filter,
		Status: "ACTIVE",
	}
	pager := servers.List(i.compute, opts)

	ret := make([]string, 0)
	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		sList, err := servers.ExtractServers(page)
		if err != nil {
			return false, err
		}
		for _, server := range sList {
			ret = append(ret, server.Name)
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	glog.V(3).Infof("Found %v instances matching %v: %v",
		len(ret), name_filter, ret)

	return ret, nil
}

func getServerByName(client *gophercloud.ServiceClient, name string) (*servers.Server, error) {
	opts := servers.ListOpts{
		Name:   fmt.Sprintf("^%s$", regexp.QuoteMeta(name)),
		Status: "ACTIVE",
	}
	pager := servers.List(client, opts)

	serverList := make([]servers.Server, 0, 1)

	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		s, err := servers.ExtractServers(page)
		if err != nil {
			return false, err
		}
		serverList = append(serverList, s...)
		if len(serverList) > 1 {
			return false, ErrMultipleResults
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if len(serverList) == 0 {
		return nil, ErrNotFound
	} else if len(serverList) > 1 {
		return nil, ErrMultipleResults
	}

	return &serverList[0], nil
}

func findAddrs(netblob interface{}) []string {
	// Run-time types for the win :(
	ret := []string{}
	list, ok := netblob.([]interface{})
	if !ok {
		return ret
	}
	for _, item := range list {
		props, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		tmp, ok := props["addr"]
		if !ok {
			continue
		}
		addr, ok := tmp.(string)
		if !ok {
			continue
		}
		ret = append(ret, addr)
	}
	return ret
}

func getAddressByName(api *gophercloud.ServiceClient, name string) (string, error) {
	srv, err := getServerByName(api, name)
	if err != nil {
		return "", err
	}

	var s string
	if s == "" {
		if tmp := findAddrs(srv.Addresses["private"]); len(tmp) >= 1 {
			s = tmp[0]
		}
	}
	if s == "" {
		if tmp := findAddrs(srv.Addresses["public"]); len(tmp) >= 1 {
			s = tmp[0]
		}
	}
	if s == "" {
		s = srv.AccessIPv4
	}
	if s == "" {
		s = srv.AccessIPv6
	}
	if s == "" {
		return "", ErrNoAddressFound
	}
	return s, nil
}

func (i *Instances) NodeAddresses(name string) ([]api.NodeAddress, error) {
	glog.V(4).Infof("NodeAddresses(%v) called", name)

	srv, err := getServerByName(i.compute, name)
	if err != nil {
		return nil, err
	}

	addrs := []api.NodeAddress{}

	for _, addr := range findAddrs(srv.Addresses["private"]) {
		addrs = append(addrs, api.NodeAddress{
			Type:    api.NodeInternalIP,
			Address: addr,
		})
	}

	for _, addr := range findAddrs(srv.Addresses["public"]) {
		addrs = append(addrs, api.NodeAddress{
			Type:    api.NodeExternalIP,
			Address: addr,
		})
	}

	// AccessIPs are usually duplicates of "public" addresses.
	api.AddToNodeAddresses(&addrs,
		api.NodeAddress{
			Type:    api.NodeExternalIP,
			Address: srv.AccessIPv6,
		},
		api.NodeAddress{
			Type:    api.NodeExternalIP,
			Address: srv.AccessIPv4,
		},
	)

	glog.V(4).Infof("NodeAddresses(%v) => %v", name, addrs)
	return addrs, nil
}

// ExternalID returns the cloud provider ID of the specified instance.
func (i *Instances) ExternalID(name string) (string, error) {
	srv, err := getServerByName(i.compute, name)
	if err != nil {
		return "", err
	}
	return srv.ID, nil
}

func (i *Instances) GetNodeResources(name string) (*api.NodeResources, error) {
	glog.V(4).Infof("GetNodeResources(%v) called", name)

	srv, err := getServerByName(i.compute, name)
	if err != nil {
		return nil, err
	}

	s, ok := srv.Flavor["id"]
	if !ok {
		return nil, ErrAttrNotFound
	}
	flavId, ok := s.(string)
	if !ok {
		return nil, ErrAttrNotFound
	}
	rsrc, ok := i.flavor_to_resource[flavId]
	if !ok {
		return nil, ErrNotFound
	}

	glog.V(4).Infof("GetNodeResources(%v) => %v", name, rsrc)

	return rsrc, nil
}

func (os *OpenStack) Clusters() (cloudprovider.Clusters, bool) {
	return nil, false
}

type LoadBalancer struct {
	network *gophercloud.ServiceClient
	compute *gophercloud.ServiceClient
	opts    LoadBalancerOpts
}

func (os *OpenStack) TCPLoadBalancer() (cloudprovider.TCPLoadBalancer, bool) {
	glog.V(4).Info("openstack.TCPLoadBalancer() called")

	// TODO: Search for and support Rackspace loadbalancer API, and others.
	network, err := openstack.NewNetworkV2(os.provider, gophercloud.EndpointOpts{
		Region: os.region,
	})
	if err != nil {
		glog.Warningf("Failed to find neutron endpoint: %v", err)
		return nil, false
	}

	compute, err := openstack.NewComputeV2(os.provider, gophercloud.EndpointOpts{
		Region: os.region,
	})
	if err != nil {
		glog.Warningf("Failed to find compute endpoint: %v", err)
		return nil, false
	}

	glog.V(1).Info("Claiming to support TCPLoadBalancer")

	return &LoadBalancer{network, compute, os.lbOpts}, true
}

func getVipByName(client *gophercloud.ServiceClient, name string) (*vips.VirtualIP, error) {
	opts := vips.ListOpts{
		Name: name,
	}
	pager := vips.List(client, opts)

	vipList := make([]vips.VirtualIP, 0, 1)

	err := pager.EachPage(func(page pagination.Page) (bool, error) {
		v, err := vips.ExtractVIPs(page)
		if err != nil {
			return false, err
		}
		vipList = append(vipList, v...)
		if len(vipList) > 1 {
			return false, ErrMultipleResults
		}
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	if len(vipList) == 0 {
		return nil, ErrNotFound
	} else if len(vipList) > 1 {
		return nil, ErrMultipleResults
	}

	return &vipList[0], nil
}

func (lb *LoadBalancer) TCPLoadBalancerExists(name, region string) (bool, error) {
	vip, err := getVipByName(lb.network, name)
	if err == ErrNotFound {
		return false, nil
	}
	return vip != nil, err
}

// TODO: This code currently ignores 'region' and always creates a
// loadbalancer in only the current OpenStack region.  We should take
// a list of regions (from config) and query/create loadbalancers in
// each region.

func (lb *LoadBalancer) CreateTCPLoadBalancer(name, region string, externalIP net.IP, ports []int, hosts []string, affinity api.AffinityType) (string, error) {
	glog.V(4).Infof("CreateTCPLoadBalancer(%v, %v, %v, %v, %v, %v)", name, region, externalIP, ports, hosts, affinity)

	if len(ports) > 1 {
		return "", fmt.Errorf("multiple ports are not yet supported in openstack load balancers")
	}

	var persistence *vips.SessionPersistence
	switch affinity {
	case api.AffinityTypeNone:
		persistence = nil
	case api.AffinityTypeClientIP:
		persistence = &vips.SessionPersistence{Type: "SOURCE_IP"}
	default:
		return "", fmt.Errorf("unsupported load balancer affinity: %v", affinity)
	}

	pool, err := pools.Create(lb.network, pools.CreateOpts{
		Name:     name,
		Protocol: pools.ProtocolTCP,
		SubnetID: lb.opts.SubnetId,
	}).Extract()
	if err != nil {
		return "", err
	}

	for _, host := range hosts {
		addr, err := getAddressByName(lb.compute, host)
		if err != nil {
			return "", err
		}

		_, err = members.Create(lb.network, members.CreateOpts{
			PoolID:       pool.ID,
			ProtocolPort: ports[0], //TODO: need to handle multi-port
			Address:      addr,
		}).Extract()
		if err != nil {
			pools.Delete(lb.network, pool.ID)
			return "", err
		}
	}

	var mon *monitors.Monitor
	if lb.opts.CreateMonitor {
		mon, err = monitors.Create(lb.network, monitors.CreateOpts{
			Type:       monitors.TypeTCP,
			Delay:      int(lb.opts.MonitorDelay.Duration.Seconds()),
			Timeout:    int(lb.opts.MonitorTimeout.Duration.Seconds()),
			MaxRetries: int(lb.opts.MonitorMaxRetries),
		}).Extract()
		if err != nil {
			pools.Delete(lb.network, pool.ID)
			return "", err
		}

		_, err = pools.AssociateMonitor(lb.network, pool.ID, mon.ID).Extract()
		if err != nil {
			monitors.Delete(lb.network, mon.ID)
			pools.Delete(lb.network, pool.ID)
			return "", err
		}
	}

	vip, err := vips.Create(lb.network, vips.CreateOpts{
		Name:         name,
		Description:  fmt.Sprintf("Kubernetes external service %s", name),
		Address:      externalIP.String(),
		Protocol:     "TCP",
		ProtocolPort: ports[0], //TODO: need to handle multi-port
		PoolID:       pool.ID,
		Persistence:  persistence,
	}).Extract()
	if err != nil {
		if mon != nil {
			monitors.Delete(lb.network, mon.ID)
		}
		pools.Delete(lb.network, pool.ID)
		return "", err
	}

	return vip.Address, nil
}

func (lb *LoadBalancer) UpdateTCPLoadBalancer(name, region string, hosts []string) error {
	glog.V(4).Infof("UpdateTCPLoadBalancer(%v, %v, %v)", name, region, hosts)

	vip, err := getVipByName(lb.network, name)
	if err != nil {
		return err
	}

	// Set of member (addresses) that _should_ exist
	addrs := map[string]bool{}
	for _, host := range hosts {
		addr, err := getAddressByName(lb.compute, host)
		if err != nil {
			return err
		}

		addrs[addr] = true
	}

	// Iterate over members that _do_ exist
	pager := members.List(lb.network, members.ListOpts{PoolID: vip.PoolID})
	err = pager.EachPage(func(page pagination.Page) (bool, error) {
		memList, err := members.ExtractMembers(page)
		if err != nil {
			return false, err
		}

		for _, member := range memList {
			if _, found := addrs[member.Address]; found {
				// Member already exists
				delete(addrs, member.Address)
			} else {
				// Member needs to be deleted
				err = members.Delete(lb.network, member.ID).ExtractErr()
				if err != nil {
					return false, err
				}
			}
		}

		return true, nil
	})
	if err != nil {
		return err
	}

	// Anything left in addrs is a new member that needs to be added
	for addr := range addrs {
		_, err := members.Create(lb.network, members.CreateOpts{
			PoolID:       vip.PoolID,
			Address:      addr,
			ProtocolPort: vip.ProtocolPort,
		}).Extract()
		if err != nil {
			return err
		}
	}

	return nil
}

func (lb *LoadBalancer) DeleteTCPLoadBalancer(name, region string) error {
	glog.V(4).Infof("DeleteTCPLoadBalancer(%v, %v)", name, region)

	vip, err := getVipByName(lb.network, name)
	if err != nil {
		return err
	}

	pool, err := pools.Get(lb.network, vip.PoolID).Extract()
	if err != nil {
		return err
	}

	// Have to delete VIP before pool can be deleted
	err = vips.Delete(lb.network, vip.ID).ExtractErr()
	if err != nil {
		return err
	}

	// Ignore errors for everything following here

	for _, monId := range pool.MonitorIDs {
		pools.DisassociateMonitor(lb.network, pool.ID, monId)
	}
	pools.Delete(lb.network, pool.ID)

	return nil
}

func (os *OpenStack) Zones() (cloudprovider.Zones, bool) {
	glog.V(1).Info("Claiming to support Zones")

	return os, true
}
func (os *OpenStack) GetZone() (cloudprovider.Zone, error) {
	glog.V(1).Infof("Current zone is %v", os.region)

	return cloudprovider.Zone{Region: os.region}, nil
}

func (os *OpenStack) AttachDisk(diskName string) (string, error) {
	disk, err := os.getVolume(diskName)
	if err != nil {
		return "", err
	}
	cClient, err := openstack.NewComputeV2(os.provider, gophercloud.EndpointOpts{
		Region: os.region,
	})

	if err != nil || cClient == nil {
		glog.Errorf("Unable to initialize nova client for region: %s", os.region)
		return "", err
	}

	compute_id, err := os.getComputeIDbyHostname(cClient)
	if err != nil || compute_id == "null" {
		glog.Errorf("Unable to get minion's id by minion's hostname")
		return "", err
	}

	if len(disk.Attachments) > 0 && disk.Attachments[0]["server_id"] != nil {
		if compute_id == disk.Attachments[0]["server_id"] {
			glog.Infof("Disk: %q is already attached to compute: %q\n", diskName, compute_id)
			return disk.ID, nil
		} else {
			errMsg := fmt.Sprintf("Disk %q is attached to a different compute: %q, should be detached before proceeding", diskName, disk.Attachments[0]["server_id"])
			glog.Errorf(errMsg)
			return "", errors.New(errMsg)
		}
	}
	// add read only flag here if possible spothanis
	_, err = volumeattach.Create(cClient, compute_id, &volumeattach.CreateOpts{
		VolumeID: disk.ID,
	}).Extract()
	if err != nil {
		glog.Infof("Failed to attach %s volume to %s compute", diskName, compute_id)
		return "", err
	}
	glog.Infof("Successfully attached %s volume to %s compute", diskName, compute_id)
	return disk.ID, nil
}

func (os *OpenStack) DetachDisk(partialDiskId string) error {
	disk, err := os.getVolume(partialDiskId)
	if err != nil {
		return err
	}

	cClient, err := openstack.NewComputeV2(os.provider, gophercloud.EndpointOpts{
		Region: os.region,
	})

	compute_id, err := os.getComputeIDbyHostname(cClient)
	if err != nil || compute_id == "" {
		glog.Errorf("Unable to get minion's ip while detaching disk")
		return err
	}
	if len(disk.Attachments) > 0 && disk.Attachments[0]["server_id"] != nil && compute_id == disk.Attachments[0]["server_id"] {
		err = volumeattach.Delete(cClient, compute_id, disk.ID).ExtractErr()
		if err != nil {
			glog.Errorf("Failed to delete volume %s from compute %s attached %v\n", disk.ID, compute_id, err)
			return err
		}
	} else {
		errMsg := fmt.Sprintf("Disk: %s has no attachments or is not attached to compute: %s\n", disk.Name, compute_id)
		glog.Errorf(errMsg)
		return errors.New(errMsg)
	}
	return nil
}

//Takes a partial/full disk id or diskname

func (os *OpenStack) getVolume(diskName string) (volumes.Volume, error) {
	sClient, err := openstack.NewBlockStorageV1(os.provider, gophercloud.EndpointOpts{
		Region: os.region,
	})

	var volume volumes.Volume
	if err != nil || sClient == nil {
		glog.Errorf("Unable to initialize cinder client for region: %s", os.region)
		return volume, err
	}

	err = volumes.List(sClient, nil).EachPage(func(page pagination.Page) (bool, error) {
		vols, err := volumes.ExtractVolumes(page)
		if err != nil {
			glog.Errorf("Failed to extract volumes: %v\n", err)
			return false, err
		} else {
			for _, v := range vols {
				glog.Infof("%s\t%s\t%v\n", v.ID, v.Name, v.Attachments)
				if v.Name == diskName || strings.Contains(v.ID, diskName) {
					volume = v
					return true, nil
				}
			}
		}
		//if it reached here then no disk with the given name was found.
		errmsg := fmt.Sprintf("Unable to find disk: %s in region %s", diskName, os.region)
		return false, errors.New(errmsg)
	})
	if err != nil {
		glog.Errorf("Error occured getting volume: %s\n", diskName)
		return volume, err
	}
	return volume, err
}

func (os *OpenStack) getComputeIDbyHostname(cClient *gophercloud.ServiceClient) (string, error) {

	hostname, err := ossys.Hostname()

	if err != nil {
		glog.Errorf("Failed to get Minion's hostname: %v\n", err)
		return "", err
	}

	i, ok := os.Instances()
	if !ok {
		glog.Errorf("Unable to get instances")
		return "", errors.New("Unable to get instances")
	}

	srvs, err := i.List(".")
	if err != nil {
		glog.Errorf("Failed to list servers: %v", err)
		return "", err
	}

	if len(srvs) == 0 {
		glog.Errorf("Found no servers in the region")
		return "", errors.New("Found no servers in the region")
	}
	glog.Infof("found servers: %v\n", srvs)

	for _, srvname := range srvs {
		server, err := getServerByName(cClient, srvname)
		if err != nil {
			return "", err
		} else {
			if server.Metadata["hostname"] != nil && server.Metadata["hostname"] == hostname {
				fmt.Printf("found server: %s with host :%s\n", server.Name, hostname)
				return server.ID, nil
			}
		}
	}
	return "", err
}
