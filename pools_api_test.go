package postgres_test

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	postgres "github.com/fin-assist/go-postgres"
	"github.com/fin-assist/go-postgres/internal/pgtest"
)

func TestAPIOwnedPoolsAndBorrowingManagerHaveSeparateLifetimes(t *testing.T) {
	for _, replica := range []string{"absent", "same", "distinct"} {
		t.Run(replica, func(t *testing.T) {
			dsn := pgtest.DSN(t)
			options := postgres.PoolOptions{PrimaryDSN: dsn, MaxConns: 1, AppName: "api-consumer"}
			switch replica {
			case "same":
				options.ReplicaDSN = dsn
			case "distinct":
				options.ReplicaDSN = pgtest.DSN(t)
			}
			pools, err := postgres.OpenPools(t.Context(), options)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(pools.Close)
			if (pools.Primary() == pools.Replica()) != (replica != "distinct") {
				t.Fatal("replica sharing does not match service configuration")
			}
			manager := postgres.New(pools.Primary())
			if err := manager.Run(t.Context(), func(ctx context.Context) error {
				tx, ok := manager.Tx(ctx)
				if !ok {
					t.Fatal("manager lost the consumer's transaction")
				}
				var name string
				if err := tx.QueryRow(ctx, "SHOW application_name").Scan(&name); err != nil {
					return err
				}
				if name != options.AppName {
					t.Fatalf("application_name=%q", name)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			if err := pools.Ping(t.Context()); err != nil {
				t.Fatal("manager closed a caller-owned pool", err)
			}
			if err := pools.Replica().Ping(t.Context()); err != nil {
				t.Fatal("replica unavailable", err)
			}
			if pools.Primary().Config().MaxConns != 1 || pools.Primary().Stat().AcquiredConns() != 0 {
				t.Fatal("pool options or transaction release changed")
			}
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(pools.Close)
			}
			wg.Wait()
			if !errors.Is(pools.Ping(t.Context()), postgres.ErrPoolUnavailable) {
				t.Fatal("closed primary still looks ready")
			}
			if err := pools.Replica().Ping(t.Context()); err == nil {
				t.Fatal("owned replica survived Close")
			}
		})
	}
}

func TestAPIPoolStartupErrorsAreSafeAndNeverReturnPartialResources(t *testing.T) {
	valid := pgtest.DSN(t)
	const malformed = "postgres://user:never-return-this@%zz"
	for _, options := range []postgres.PoolOptions{
		{},
		{PrimaryDSN: malformed},
		{PrimaryDSN: valid, ReplicaDSN: malformed},
		{PrimaryDSN: "postgres://user:never-return-this@127.0.0.1:1/test?sslmode=disable&connect_timeout=1"},
	} {
		pools, err := postgres.OpenPools(t.Context(), options)
		if pools != nil {
			pools.Close()
			t.Fatal("startup failure returned partial owned resources")
		}
		if err == nil || strings.Contains(err.Error(), "never-return-this") ||
			(!errors.Is(err, postgres.ErrInvalidDSN) && !errors.Is(err, postgres.ErrPoolUnavailable)) {
			t.Fatal("unsafe or unclassified startup error", err)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if pools, err := postgres.OpenPools(ctx, postgres.PoolOptions{PrimaryDSN: valid}); pools != nil || !errors.Is(err, context.Canceled) {
		if pools != nil {
			pools.Close()
		}
		t.Fatal("cancelled startup did not preserve cancellation", err)
	}
	// A failed rollout must not poison a later healthy restart on the same server.
	pools, err := postgres.OpenPools(t.Context(), postgres.PoolOptions{PrimaryDSN: valid})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pools.Close)
	if err := pools.Ping(t.Context()); err != nil {
		t.Fatal(err)
	}
}

// FIN-277: an unresponsive primary is "database unavailable", not the service's own
// deadline, even though pgx reports its connect_timeout as context.DeadlineExceeded.
func TestAPIUnresponsivePrimaryIsUnavailableNotCallerTimeout(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan net.Conn, 16)
	go func() {
		for {
			c, err := lis.Accept()
			if err != nil {
				close(accepted)
				return
			}
			accepted <- c
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		for c := range accepted {
			_ = c.Close()
		}
	})
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	pools, err := postgres.OpenPools(ctx, postgres.PoolOptions{
		PrimaryDSN: "postgres://user:never-return-this@" + lis.Addr().String() + "/db?sslmode=disable&connect_timeout=1",
	})
	if pools != nil {
		pools.Close()
		t.Fatal("startup against an unresponsive primary returned pools")
	}
	if !errors.Is(err, postgres.ErrPoolUnavailable) || errors.Is(err, context.DeadlineExceeded) ||
		ctx.Err() != nil || strings.Contains(err.Error(), "never-return-this") {
		t.Fatalf("unresponsive primary misclassified: %v", err)
	}
}
