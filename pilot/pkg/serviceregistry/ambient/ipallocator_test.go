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
	const cidr = "10.0.0.0/24"
	address := func(ip string) networking.IPAddress {
		return networking.IPAddress{
			ObjectMeta: metav1.ObjectMeta{Name: ip},
		}
	}
	tests := []struct {
		name       string
		initial    []networking.IPAddress
		want       []networking.IPAddress
		staleCache bool
	}{
		{
			name: "creates next address",
			want: []networking.IPAddress{address("10.0.0.1")},
		},
		{
			name:    "skips used address",
			initial: []networking.IPAddress{address("10.0.0.1")},
			want: []networking.IPAddress{
				address("10.0.0.1"),
				address("10.0.0.2"),
			},
		},
		{
			name:    "retries existing IPAddress",
			initial: []networking.IPAddress{address("10.0.0.1")},
			want: []networking.IPAddress{
				address("10.0.0.1"),
				address("10.0.0.2"),
			},
			staleCache: true,
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
				cidrIndex:   map[netip.Prefix]int{netip.MustParsePrefix(cidr): 0},
				cidrs:       []netip.Prefix{netip.MustParsePrefix(cidr)},
				nextAddress: netip.MustParseAddr("10.0.0.1"),
			}
			if !tt.staleCache {
				for _, initial := range tt.initial {
					ipallocator.usedAddresses[netip.MustParseAddr(initial.Name)] = true
				}
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
