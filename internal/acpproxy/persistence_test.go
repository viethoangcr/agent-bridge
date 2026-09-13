package acpproxy

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"testing"

	"github.com/viethoangcr/agent-bridge/internal/acpruntime"
	"github.com/viethoangcr/agent-bridge/internal/acpstore"
)

// TestProxyPersistenceFailuresClassified proves proxy-owned persistence
// failures carry acpruntime.ErrPersistence (HTTP 507) while lifecycle
// sentinels and process-spawn failures stay untouched.
func TestProxyPersistenceFailuresClassified(t *testing.T) {
	agent := "alpha"
	tests := []struct {
		name   string
		setup  func(t *testing.T, p *Proxy, f *testFactory)
		want   error
		reject error
	}{
		{
			name: "server read",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(context.Context, string) (acpstore.Server, error) {
					return acpstore.Server{}, errBoom
				}
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "create server",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(context.Context, string) (acpstore.Server, error) {
					return acpstore.Server{}, acpstore.ErrNotFound
				}
				p.createServer = func(context.Context, string, string) (acpstore.Server, error) {
					return acpstore.Server{}, errBoom
				}
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "set status",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(_ context.Context, serverID string) (acpstore.Server, error) {
					return acpstore.Server{ServerID: serverID, Agent: agent}, nil
				}
				p.setStatus = func(context.Context, string, acpstore.Status) error {
					return errBoom
				}
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "startup live publication",
			setup: func(_ *testing.T, _ *Proxy, f *testFactory) {
				f.setOnCreate(func(context.Context, *acpstore.Store, string, acpruntime.LaunchSpec) error {
					return errBoom
				})
			},
			want: acpruntime.ErrPersistence,
		},
		{
			name: "spawn failure stays a process failure",
			setup: func(_ *testing.T, _ *Proxy, f *testFactory) {
				f.setOnCreate(func(context.Context, *acpstore.Store, string, acpruntime.LaunchSpec) error {
					return &exec.Error{Name: "alpha-bin", Err: exec.ErrNotFound}
				})
			},
			want:   exec.ErrNotFound,
			reject: acpruntime.ErrPersistence,
		},
		{
			name: "create conflict preserved",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(context.Context, string) (acpstore.Server, error) {
					return acpstore.Server{}, acpstore.ErrNotFound
				}
				p.createServer = func(context.Context, string, string) (acpstore.Server, error) {
					return acpstore.Server{}, fmt.Errorf("%w: duplicate", acpstore.ErrConflict)
				}
			},
			want:   acpstore.ErrConflict,
			reject: acpruntime.ErrPersistence,
		},
		{
			name: "set status not found preserved",
			setup: func(_ *testing.T, p *Proxy, _ *testFactory) {
				p.storeServer = func(_ context.Context, serverID string) (acpstore.Server, error) {
					return acpstore.Server{ServerID: serverID, Agent: agent}, nil
				}
				p.setStatus = func(context.Context, string, acpstore.Status) error {
					return fmt.Errorf("%w: gone", acpstore.ErrNotFound)
				}
			},
			want:   acpstore.ErrNotFound,
			reject: acpruntime.ErrPersistence,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTestFactory(t)
			p, _ := newProxyForTest(t, f)
			tc.setup(t, p, f)

			_, err := p.Post(t.Context(), "persistence", &agent, "initialize", initPayload)
			if err == nil {
				t.Fatal("Post succeeded, want a classified failure")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("Post error = %v, want %v", err, tc.want)
			}
			if tc.reject != nil && errors.Is(err, tc.reject) {
				t.Fatalf("Post error = %v, must not match %v", err, tc.reject)
			}
		})
	}
}
