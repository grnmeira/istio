package ambient

import (
	"net/netip"

	networking "k8s.io/api/networking/v1beta1"

	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/kube/krt"
	"istio.io/istio/pkg/log"
)

type allocator struct {
	cidrIndex   map[netip.Prefix]int
	cidrs       []netip.Prefix
	currentCidr int
	nextAddress netip.Addr
}

func (a *allocator) next() netip.Addr {
	for i := 0; i < len(a.cidrs); i++ {
		c := a.cidrs[a.currentCidr]
		if a.nextAddress.IsValid() && c.Contains(a.nextAddress) {
			addr := a.nextAddress
			a.nextAddress = a.nextAddress.Next()
			return addr
		}
		a.nextCidr()
	}
	return netip.Addr{}
}

func (a *allocator) add(cidr netip.Prefix) {
	if !cidr.IsValid() {
		return
	}
	if _, exists := a.cidrIndex[cidr]; exists {
		return
	}
	a.cidrs = append(a.cidrs, cidr)
	a.cidrIndex[cidr] = len(a.cidrs) - 1
	if len(a.cidrs) == 1 {
		a.currentCidr = 0
		a.nextAddress = cidr.Addr().Next()
	}
}

func (a *allocator) nextCidr() {
	if len(a.cidrs) > 0 {
		a.currentCidr = (a.currentCidr + 1) % len(a.cidrs)
		c := a.cidrs[a.currentCidr]
		a.nextAddress = c.Addr().Next()
	}
}

func (a *allocator) remove(cidr netip.Prefix) {
	if !cidr.IsValid() {
		return
	}

	ci, exists := a.cidrIndex[cidr]
	if !exists {
		return
	}

	a.cidrs[ci] = a.cidrs[len(a.cidrs)-1]
	a.cidrs = a.cidrs[:len(a.cidrs)-1]
	delete(a.cidrIndex, cidr)

	// If we have no CIDRs left
	if len(a.cidrIndex) == 0 {
		a.nextAddress = netip.Addr{}
		a.currentCidr = 0
		return
	}

	if ci < len(a.cidrs) {
		a.cidrIndex[a.cidrs[ci]] = ci
	}

	// If the next IP address belongs to the deleted cidr or
	// if the current CIDR was pointing at the last one that
	// has been deleted
	if a.currentCidr >= len(a.cidrs) || cidr.Contains(a.nextAddress) {
		a.nextCidr()
	}
}

type IPAllocator struct {
	ipAddressCollection   krt.Collection[*networking.IPAddress]
	ipAddressWriter       kclient.Writer[*networking.IPAddress]
	serviceCidrCollection krt.Collection[*networking.ServiceCIDR]
	v4Allocator           allocator
	v6Allocator           allocator
	usedAddresses         map[netip.Addr]bool
}

func NewIPAllocator(client kube.Client, stop <-chan struct{}) *IPAllocator {
	if client == nil {
		return nil
	}
	ipallocator := IPAllocator{
		ipAddressCollection:   krt.WrapClient(kclient.New[*networking.IPAddress](client), krt.WithStop(stop)),
		ipAddressWriter:       kclient.NewWriteClient[*networking.IPAddress](client),
		serviceCidrCollection: krt.WrapClient(kclient.New[*networking.ServiceCIDR](client), krt.WithStop(stop)),
		v4Allocator:           allocator{},
		v6Allocator:           allocator{},
	}

	ipallocator.ipAddressCollection.RegisterBatch(func(events []krt.Event[*networking.IPAddress]) {
		log.Debugf("IPAddress updated")
		for _, e := range events {
			switch e.Event {
			case controllers.EventAdd:
			case controllers.EventUpdate:
				addr, err := netip.ParseAddr((*e.New).Name)
				if err != nil {
					log.Debugf("can't parse IP address %s", (*e.New).Name)
					continue
				}
				ipallocator.usedAddresses[addr] = true
			case controllers.EventDelete:
				addr, err := netip.ParseAddr((*e.New).Name)
				if err != nil {
					log.Debugf("can't parse IP address %s", (*e.New).Name)
					continue
				}
				ipallocator.usedAddresses[addr] = false
			}
		}
	}, false)

	ipallocator.serviceCidrCollection.RegisterBatch(func(events []krt.Event[*networking.ServiceCIDR]) {
		log.Debugf("ServiceCIDR updated")
		for _, e := range events {
			switch e.Event {
			case controllers.EventAdd:
				ipallocator.addServiceCIDR(*e.New)
			case controllers.EventDelete:
				ipallocator.removeServiceCIDR(*e.Old)
			case controllers.EventUpdate:
				ipallocator.removeServiceCIDR(*e.Old)
				ipallocator.addServiceCIDR(*e.New)
			}

		}
	}, false)

	return &ipallocator
}

func (a *IPAllocator) addServiceCIDR(serviceCidr *networking.ServiceCIDR) {
	for _, cidr := range serviceCidr.Spec.CIDRs {
		prefix, e := netip.ParsePrefix(cidr)
		if e != nil || !prefix.IsValid() {
			continue
		}
		if prefix.Addr().Is4() {
			a.v4Allocator.add(prefix)
		} else {
			a.v6Allocator.add(prefix)
		}
	}
}

func (a *IPAllocator) removeServiceCIDR(serviceCidr *networking.ServiceCIDR) {
	for _, c := range serviceCidr.Spec.CIDRs {
		cidr, err := netip.ParsePrefix(c)
		if err != nil || !cidr.IsValid() {
			continue
		}
		a.v4Allocator.remove(cidr)
		a.v6Allocator.remove(cidr)
	}
}
