package ambient

import (
	"context"
	"net/netip"
	"testing"

	networking "k8s.io/api/networking/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"istio.io/istio/pkg/kube"
	"istio.io/istio/pkg/kube/kclient"
	"istio.io/istio/pkg/test/util/assert"
)

func TestIPAllocatorAllocateMissingCIDR(t *testing.T) {
	client := kube.NewFakeClient()
	ipallocator := &IPAllocator{
		ipAddressWriter: kclient.NewWriteClient[*networking.IPAddress](client),
		usedAddresses:   make(map[netip.Addr]bool),
	}
	ipallocator.allocate()

	addresses, err := client.Kube().NetworkingV1beta1().IPAddresses().List(context.Background(), metav1.ListOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(addresses.Items), 0)
	assert.Equal(t, len(ipallocator.usedAddresses), 0)
}

func TestIPAllocatorAllocate(t *testing.T) {
	address := func(ip string) networking.IPAddress {
		return networking.IPAddress{
			ObjectMeta: metav1.ObjectMeta{Name: ip},
		}
	}
	shortCIDR := netip.MustParsePrefix("10.0.0.0/28")
	var exhaustedAddresses []networking.IPAddress
	for addr := shortCIDR.Addr().Next(); shortCIDR.Contains(addr); addr = addr.Next() {
		exhaustedAddresses = append(exhaustedAddresses, address(addr.String()))
	}
	tests := []struct {
		name         string
		cidrs        []string
		deletedCIDRs []string
		currentCIDR  int
		nextAddress  string
		initial      []networking.IPAddress
		want         []networking.IPAddress
		staleCache   bool
	}{
		{
			name:  "creates next address",
			cidrs: []string{"10.0.0.0/24"},
			want:  []networking.IPAddress{address("10.0.0.1")},
		},
		{
			name:    "exhausted /28 creates no IPAddress",
			cidrs:   []string{shortCIDR.String()},
			initial: exhaustedAddresses,
			want:    exhaustedAddresses,
		},
		{
			name:    "skips used address",
			cidrs:   []string{"10.0.0.0/24"},
			initial: []networking.IPAddress{address("10.0.0.1")},
			want: []networking.IPAddress{
				address("10.0.0.1"),
				address("10.0.0.2"),
			},
		},
		{
			name:    "retries existing IPAddress",
			cidrs:   []string{"10.0.0.0/24"},
			initial: []networking.IPAddress{address("10.0.0.1")},
			want: []networking.IPAddress{
				address("10.0.0.1"),
				address("10.0.0.2"),
			},
			staleCache: true,
		},
		{
			name:    "uses next CIDR when first range is used",
			cidrs:   []string{"10.0.0.0/31", "10.1.0.0/24"},
			initial: []networking.IPAddress{address("10.0.0.1")},
			want: []networking.IPAddress{
				address("10.0.0.1"),
				address("10.1.0.1"),
			},
		},
		{
			name:         "deletes the last CIDR while another remains",
			cidrs:        []string{"10.0.0.0/24", "10.1.0.0/24"},
			deletedCIDRs: []string{"10.1.0.0/24"},
			want:         []networking.IPAddress{address("10.0.0.1")},
		},
		{
			name:         "deletes the active CIDR",
			cidrs:        []string{"10.0.0.0/24", "10.1.0.0/24"},
			deletedCIDRs: []string{"10.0.0.0/24"},
			want:         []networking.IPAddress{address("10.1.0.1")},
		},
		{
			name:         "preserves the cursor when the active CIDR moves",
			cidrs:        []string{"10.0.0.0/24", "10.1.0.0/24", "10.2.0.0/24"},
			deletedCIDRs: []string{"10.0.0.0/24"},
			currentCIDR:  2,
			nextAddress:  "10.2.0.5",
			want:         []networking.IPAddress{address("10.2.0.5")},
		},
		{
			name:         "deletes the active CIDR after its cursor leaves the range",
			cidrs:        []string{"10.0.0.0/24", "10.1.0.0/24", "10.2.0.0/24"},
			deletedCIDRs: []string{"10.0.0.0/24"},
			nextAddress:  "10.1.0.5",
			want:         []networking.IPAddress{address("10.2.0.1")},
		},
		{
			name:         "repeated and unknown CIDR deletions are harmless",
			cidrs:        []string{"10.0.0.0/24", "10.1.0.0/24", "10.2.0.0/24"},
			deletedCIDRs: []string{"10.1.0.0/24", "10.1.0.0/24", "10.3.0.0/24", "10.2.0.0/24"},
			want:         []networking.IPAddress{address("10.0.0.1")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var objects []runtime.Object
			for _, initial := range tt.initial {
				objects = append(objects, initial.DeepCopy())
			}
			client := kube.NewFakeClient(objects...)
			ipallocator := &IPAllocator{
				ipAddressWriter: kclient.NewWriteClient[*networking.IPAddress](client),
				usedAddresses:   make(map[netip.Addr]bool),
			}
			ipallocator.v4Allocator = allocator{
				cidrIndex: make(map[netip.Prefix]int),
			}
			for _, cidr := range tt.cidrs {
				ipallocator.v4Allocator.add(netip.MustParsePrefix(cidr))
			}
			if !tt.staleCache {
				for _, initial := range tt.initial {
					ipallocator.usedAddresses[netip.MustParseAddr(initial.Name)] = true
				}
			}
			ipallocator.v4Allocator.currentCidr = tt.currentCIDR
			if tt.nextAddress != "" {
				ipallocator.v4Allocator.nextAddress = netip.MustParseAddr(tt.nextAddress)
			}
			ipallocator.removeServiceCIDR(&networking.ServiceCIDR{
				Spec: networking.ServiceCIDRSpec{CIDRs: tt.deletedCIDRs},
			})
			assert.Equal(t, len(ipallocator.v4Allocator.cidrIndex), len(ipallocator.v4Allocator.cidrs))
			for index, cidr := range ipallocator.v4Allocator.cidrs {
				actualIndex, exists := ipallocator.v4Allocator.cidrIndex[cidr]
				assert.Equal(t, exists, true)
				assert.Equal(t, actualIndex, index)
			}
			for _, cidr := range tt.deletedCIDRs {
				_, exists := ipallocator.v4Allocator.cidrIndex[netip.MustParsePrefix(cidr)]
				assert.Equal(t, exists, false)
			}

			ipallocator.allocate()

			addresses, err := client.Kube().NetworkingV1beta1().IPAddresses().List(context.Background(), metav1.ListOptions{})
			assert.NoError(t, err)
			got := make(map[string]networking.IPAddress)
			for _, actual := range addresses.Items {
				actual.TypeMeta = metav1.TypeMeta{}
				actual.ManagedFields = nil
				got[actual.Name] = actual
			}
			want := make(map[string]networking.IPAddress)
			for _, expected := range tt.want {
				want[expected.Name] = expected
			}
			assert.Equal(t, got, want)
			for _, expected := range tt.want {
				assert.Equal(t, ipallocator.usedAddresses[netip.MustParseAddr(expected.Name)], true)
			}
		})
	}
}
