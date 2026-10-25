package postgres

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// silentServer accepts TCP connections and never answers, so pgx fails on its own
// connect_timeout deadline while the caller's context is still live (FIN-277).
func silentServer(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			c, err := lis.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		<-done
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	return "postgres://user:never-return-this@" + lis.Addr().String() + "/db?sslmode=disable"
}

func TestIntegrationPoolsPingClassifiesDriverTimeoutByCallerContext(t *testing.T) {
	pool, err := newManagedPool(t.Context(), silentServer(t)+"&connect_timeout=1", PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p := &Pools{primary: pool.(*pgxpool.Pool), replica: pool.(*pgxpool.Pool), lifecycle: &poolPair{primary: pool, replica: pool}}
	t.Cleanup(p.Close)
	err = p.Ping(t.Context())
	if !errors.Is(err, ErrPoolUnavailable) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "never-return-this") {
		t.Fatalf("driver connect timeout with a live caller: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := p.Ping(ctx); !errors.Is(err, context.Canceled) || errors.Is(err, ErrPoolUnavailable) {
		t.Fatalf("canceled caller: %v", err)
	}
}

// Readiness probes usually have no connect_timeout in the DSN (pgx default: none) and a
// short caller deadline. The deadline expires while pgx is still connecting: that is
// the caller's own timeout, and Close must not wait for the abandoned connect.
func TestIntegrationPoolsPingCallerDeadlineDuringConnect(t *testing.T) {
	pool, err := newManagedPool(t.Context(), silentServer(t), PoolOptions{})
	if err != nil {
		t.Fatal(err)
	}
	p := &Pools{primary: pool.(*pgxpool.Pool), replica: pool.(*pgxpool.Pool), lifecycle: &poolPair{primary: pool, replica: pool}}
	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()
	err = p.Ping(ctx)
	if !errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrPoolUnavailable) || strings.Contains(err.Error(), "never-return-this") {
		t.Fatalf("caller deadline during connect: %v", err)
	}
	closed := make(chan struct{})
	go func() { p.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close waited for an abandoned connect to a silent primary")
	}
}
