// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ipallocate

import (
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"time"

	"istio.io/istio/pilot/pkg/features"
	kubelib "istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/kube/krt"
	istiolog "istio.io/istio/pkg/log"
	"istio.io/istio/pkg/slices"
	"istio.io/istio/pkg/util/sets"
)

var log = istiolog.RegisterScope("ip-autoallocate", "IP autoallocate controller")

// IPAddressConsumer is a resource that requires auto-allocated IP addresses.
// The IPAllocator owns the address pools and conflict detection, while the consumer owns
// how addresses are read from and persisted to the underlying resource.
type IPAddressConsumer interface {
	// ResourceName is the krt key and the owner used for address tracking; it must be unique across all consumer kinds.
	ResourceName() string
	// Equals reports whether other represents the same state of this consumer.
	Equals(other IPAddressConsumer) bool
	// CreationTimestamp orders consumers on conflict; the oldest keeps a contested auto-assigned address.
	CreationTimestamp() time.Time
	// ShouldAllocate reports whether the consumer currently requires auto-assigned addresses.
	ShouldAllocate() bool
	// AutoAssignedAddresses returns the addresses currently persisted by the allocator.
	AutoAssignedAddresses() []netip.Addr
	// UserAssignedAddresses returns addresses not managed by the allocator which may still collide with its ranges.
	UserAssignedAddresses() []netip.Addr
	// AssignAddresses persists auto-assigned addresses, calling allocate for each set of addresses it needs.
	// When forceReassign is true all existing auto-assigned addresses must be replaced.
	// Addresses obtained from allocate are released by the allocator if an error is returned.
	AssignAddresses(allocate func() []netip.Addr, forceReassign bool) error
	// ClearAddresses removes all auto-assigned addresses from the resource.
	ClearAddresses() error
}

type IPAllocator struct {
	consumers krt.Collection[IPAddressConsumer]
	index     krt.Index[netip.Addr, IPAddressConsumer]
	handler   krt.HandlerRegistration
	queue     controllers.Queue

	// This controller is not safe for concurrency but it exists outside the critical path performing important but minimal functionality in a single thread.
	// If we want the multi-thread this controller we must add locking around accessing the allocators which would otherwise be racy
	v4allocator *prefixUse
	v6allocator *prefixUse
}

// HasSynced returns true when the controller has completed initial sync:
// the informer cache is populated, warming is done, and the queue is processing events.
func (c *IPAllocator) HasSynced() bool {
	return c.queue.HasSynced()
}

// Unfortunately slices are not comparable which is required by kube client-go workqueue sets.
// We want to be able to record multiple addresses as being in conflict to handle dual stack smoothly though.
// Workaround is a conversion of []netip.Addr to a string which is comparable but requires some helper
// functions to be in any way reasonable.
type conflictDetectedEvent struct {
	conflictingResourceIdentifier string
	conflictingAddresses          string
}

const conflictDetectedEventDelim = `,`

func (event conflictDetectedEvent) getAddresses() []netip.Addr {
	addresses := []netip.Addr{}
	for _, a := range strings.Split(event.conflictingAddresses, conflictDetectedEventDelim) {
		parsedAddress, err := netip.ParseAddr(a)
		if err != nil {
			continue
		}
		addresses = append(addresses, parsedAddress)
	}
	return addresses
}

func newConflictDetectedEvent(id string, addresses []netip.Addr) conflictDetectedEvent {
	var conflictingAddresses string
	if len(addresses) > 0 {
		conflictingAddresses = addresses[0].String()
		for _, a := range addresses[1:] {
			conflictingAddresses += conflictDetectedEventDelim
			conflictingAddresses += a.String()
		}
	}
	return conflictDetectedEvent{
		conflictingResourceIdentifier: id,
		conflictingAddresses:          conflictingAddresses,
	}
}

const (
	controllerName = "IP Autoallocator"
)

// NewIPAllocator creates an allocator which assigns addresses to every IPAddressConsumer in consumers.
// Consumers of different kinds can share a single allocator (and address pool) by joining their collections.
func NewIPAllocator(consumers krt.Collection[IPAddressConsumer]) *IPAllocator {
	allocator := &IPAllocator{
		consumers: consumers,
		index: krt.NewIndex(consumers, "address", func(consumer IPAddressConsumer) []netip.Addr {
			return append(consumer.AutoAssignedAddresses(), consumer.UserAssignedAddresses()...)
		}),
		// MustParsePrefix is OK because these are const. If we allow user configuration we must not use this function.
		v4allocator: newPrefixUse(netip.MustParsePrefix(features.IPAutoallocateIPv4Prefix)),
		v6allocator: newPrefixUse(netip.MustParsePrefix(features.IPAutoallocateIPv6Prefix)),
	}
	allocator.queue = controllers.NewQueue(controllerName, controllers.WithGenericReconciler(allocator.reconcile), controllers.WithMaxAttempts(5))
	allocator.handler = consumers.Register(func(e krt.Event[IPAddressConsumer]) {
		allocator.queue.Add(e.Latest().ResourceName())
	})
	return allocator
}

func (c *IPAllocator) Run(stop <-chan struct{}) {
	log.Debugf("starting %s controller", controllerName)
	kubelib.WaitForCacheSync(controllerName, stop, c.consumers.HasSynced, c.handler.HasSynced)
	// it is important that we always warm cache before we try to run the queue
	// failing to warm cache first could result in allocating IP which are already in use
	c.populateControllerDatastructures()
	// logically the v1 behavior is a state of the world function which cannot be replicated in reconcile
	// if we wish to replicate v1 assignment behavior for existing consumers it should go here and must
	// include a change to respect the used IPs from the warmed cache begin reconcile
	c.queue.Run(stop)
	c.handler.UnregisterHandler()
}

// populateControllerDatastructures reads all consumers and records any addresses in our ranges as in use
func (c *IPAllocator) populateControllerDatastructures() {
	consumers := c.consumers.List()
	for _, consumer := range consumers {
		owner := consumer.ResourceName()
		c.markUsedOrQueueConflict(consumer.UserAssignedAddresses(), owner)
		c.markUsedOrQueueConflict(consumer.AutoAssignedAddresses(), owner)
	}

	log.Debugf("discovered %v during warming", len(consumers))
}

func (c *IPAllocator) reconcile(a any) error {
	switch item := a.(type) {
	case string:
		return c.reconcileConsumer(item)
	case conflictDetectedEvent:
		return c.resolveConflict(item)
	}
	return nil
}

func (c *IPAllocator) reconcileConsumer(key string) error {
	log := log.WithLabels("consumer", key)
	log.Debugf("reconciling")
	ptr := c.consumers.GetKey(key)
	if ptr == nil {
		log.Debugf("not found, no action required")
		// probably a delete so we should remove ips from our addresses most likely
		// TODO: we never actually remove IP right now, likely this should be done a little more slowly anyway to prevent reuse if we are too fast
		return nil
	}
	consumer := *ptr

	// user assigned addresses are not managed by us but could conflict with our IP ranges
	c.markUsedOrQueueConflict(consumer.UserAssignedAddresses(), key)

	if !consumer.ShouldAllocate() {
		if len(consumer.AutoAssignedAddresses()) > 0 {
			log.Debugf("allocation no longer required, clearing stale addresses")
			return c.clearStaleAddresses(consumer)
		}
		log.Debugf("allocation not required")
		return nil
	}

	// this is likely a noop, but just to be safe we should check and potentially resolve conflict
	c.markUsedOrQueueConflict(consumer.AutoAssignedAddresses(), key)
	return c.assignAddresses(consumer, false)
}

// assignAddresses delegates persisting addresses to the consumer, freeing any newly allocated
// addresses if that fails (e.g. due to stale informer state) so they aren't leaked.
func (c *IPAllocator) assignAddresses(consumer IPAddressConsumer, forceReassign bool) error {
	owner := consumer.ResourceName()
	var allocated []netip.Addr
	err := consumer.AssignAddresses(func() []netip.Addr {
		next := c.nextAddresses(owner)
		allocated = append(allocated, next...)
		return next
	}, forceReassign)
	if err != nil {
		c.freeAddresses(allocated, owner)
	}
	return err
}

func (c *IPAllocator) resolveConflict(conflict conflictDetectedEvent) error {
	var autoConflicts []IPAddressConsumer
	seen := sets.New[string]()
	userConflict := false

	for _, conflictingAddress := range conflict.getAddresses() {
		for _, consumer := range c.index.Lookup(conflictingAddress) {
			if slices.Contains(consumer.UserAssignedAddresses(), conflictingAddress) {
				userConflict = true
			}
			if slices.Contains(consumer.AutoAssignedAddresses(), conflictingAddress) && !seen.InsertContains(consumer.ResourceName()) {
				autoConflicts = append(autoConflicts, consumer)
			}
		}
	}

	slices.SortFunc(autoConflicts, func(a, b IPAddressConsumer) int {
		return a.CreationTimestamp().Compare(b.CreationTimestamp())
	})

	// the oldest auto-assigned consumer keeps the address, unless a user assigned it
	start := 1
	if userConflict {
		start = 0
	}
	if len(autoConflicts) <= start {
		return nil
	}

	var errs error
	for _, resolveMe := range autoConflicts[start:] {
		log.Warnf("reassigned %s due to IP Address conflict", resolveMe.ResourceName())
		errs = errors.Join(errs, c.assignAddresses(resolveMe, true))
	}

	return errs
}

func (c *IPAllocator) nextAddresses(owner string) []netip.Addr {
	results := []netip.Addr{}
	v4address, err := c.v4allocator.allocateNext(owner)
	if err != nil {
		log.Error("failed to allocate v4 address")
	} else {
		results = append(results, v4address)
	}
	v6address, err := c.v6allocator.allocateNext(owner)
	if err != nil {
		log.Error("failed to allocate v6 address")
	} else {
		results = append(results, v6address)
	}
	return results
}

// freeAddresses releases addresses that were allocated by nextAddresses but never
// successfully written (e.g. because the patch was rejected due to stale informer state).
func (c *IPAllocator) freeAddresses(addrs []netip.Addr, owner string) {
	for _, a := range addrs {
		a = a.Unmap()
		if a.Is6() {
			c.v6allocator.free(a, owner)
		} else {
			c.v4allocator.free(a, owner)
		}
	}
}

func (c *IPAllocator) markUsedOrQueueConflict(maybeMappedAddrs []netip.Addr, owner string) {
	conflictingAddrs := []netip.Addr{}
	for _, maybeMappedAddr := range maybeMappedAddrs {
		a := maybeMappedAddr.Unmap()
		if a.Is6() {
			if used, usedByOwner := c.v6allocator.markUsed(a, owner); used && !usedByOwner {
				conflictingAddrs = append(conflictingAddrs, a)
			}
		} else {
			if used, usedByOwner := c.v4allocator.markUsed(a, owner); used && !usedByOwner {
				conflictingAddrs = append(conflictingAddrs, a)
			}
		}
	}
	if len(conflictingAddrs) > 0 {
		c.queue.Add(newConflictDetectedEvent(owner, conflictingAddrs))
	}
}

// clearStaleAddresses removes auto-allocated addresses from a consumer which no longer
// qualifies for auto-allocation and releases them from the pool.
func (c *IPAllocator) clearStaleAddresses(consumer IPAddressConsumer) error {
	owner := consumer.ResourceName()
	originalAddresses := consumer.AutoAssignedAddresses()
	if err := consumer.ClearAddresses(); err != nil {
		// The consumer may have changed between our read and the write.
		// Re-read and retry once before falling back to the queue's rate-limited retry.
		latest := c.consumers.GetKey(owner)
		if latest == nil || len((*latest).AutoAssignedAddresses()) == 0 {
			// Already cleared or deleted; free the original addresses.
			c.freeAddresses(originalAddresses, owner)
			return nil
		}
		if err2 := (*latest).ClearAddresses(); err2 != nil {
			log.Errorf("failed to clear stale addresses for %v: %v", owner, err2)
			return err2
		}
	}
	c.freeAddresses(originalAddresses, owner)
	return nil
}

type prefixUse struct {
	prefix netip.Prefix
	used   map[netip.Addr]string
	next   netip.Addr
}

func newPrefixUse(p netip.Prefix) *prefixUse {
	n := p.Addr().Next()
	return &prefixUse{
		prefix: p,
		used:   map[netip.Addr]string{},
		next:   n,
	}
}

func (i *prefixUse) allocateNext(owner string) (netip.Addr, error) {
	n := i.next
	var looped bool
	if !n.IsValid() || !i.prefix.Contains(n) {
		// we started outside of our range, that is unlucky but we can start over
		looped = true
		n = i.prefix.Addr().Next() // take the net address of the prefix and select next, this should be the first usable
	}

	for i.isUsed(n) {
		n = n.Next()
		if !n.IsValid() || !i.prefix.Contains(n) {
			if looped {
				// prefix is exhausted
				return netip.Addr{}, fmt.Errorf("%s is exhausted", i.prefix.String())
			}
			looped = true
			n = i.prefix.Addr().Next() // take the net address of the prefix and select next, this should be the first usable
		}
	}
	i.markUsed(n, owner)

	i.next = n.Next()

	return n, nil
}

func (i prefixUse) isUsed(n netip.Addr) bool {
	_, found := i.used[n]
	return found
}

func (i prefixUse) isUsedBy(n netip.Addr, owner string) (used, usedByOwner bool) {
	if foundOwner, found := i.used[n]; found {
		used = true // it is in use
		usedByOwner = foundOwner == owner // is it in use by the provided owner?
		return used, usedByOwner
	}
	return used, usedByOwner
}

// free removes an address from the used map. If the address is before the current
// next pointer, next is reset so the address can be reused by the next allocation.
func (i *prefixUse) free(n netip.Addr, owner string) {
	if foundOwner, found := i.used[n]; found && foundOwner == owner {
		delete(i.used, n)
		if !i.next.IsValid() || !i.prefix.Contains(i.next) || n.Less(i.next) {
			i.next = n
		}
	}
}

// markUsed will store the provided addr as used in this ipAllocator
// markUsed returns true if the addr was already used and false if it was a new insert
func (i prefixUse) markUsed(n netip.Addr, owner string) (used, usedByOwner bool) {
	if !i.prefix.Contains(n) {
		// not in our range, no need to track it
		return used, usedByOwner
	}
	used, usedByOwner = i.isUsedBy(n, owner)
	if used {
		return used, usedByOwner
	}
	i.used[n] = owner

	return false, false
}
