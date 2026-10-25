package postgres

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

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
	return "postgres://user:never-return-this@" + lis.Addr().String() + "/db?sslmode=disable&connect_timeout=1"
}

func TestIntegrationPoolsPingClassifiesDriverTimeoutByCallerContext(t *testing.T) {
	pool, err := newManagedPool(t.Context(), silentServer(t), PoolOptions{})
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
