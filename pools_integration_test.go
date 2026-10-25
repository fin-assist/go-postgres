package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	postgres "github.com/fin-assist/go-postgres"
	"github.com/fin-assist/go-postgres/internal/pgtest"
)

func TestIntegrationPoolsKeepReplicaLazyAndPrimaryRequired(t *testing.T) {
	for _, replica := range []string{"", "same", "unreachable"} {
		t.Run(replica, func(t *testing.T) {
			dsn := pgtest.DSN(t)
			options := postgres.PoolOptions{PrimaryDSN: dsn, AppName: "integration-pools", MaxConns: 2}
			switch replica {
			case "same":
				options.ReplicaDSN = dsn
			case "unreachable":
				options.ReplicaDSN = "postgres://postgres@127.0.0.1:1/test?sslmode=disable&connect_timeout=1"
			}
			pools, err := postgres.OpenPools(t.Context(), options)
			if err != nil {
				t.Fatal("replica unavailability must not prevent primary startup", err)
			}
			t.Cleanup(pools.Close)
			if err := pools.Ping(t.Context()); err != nil {
				t.Fatal(err)
			}
			if (pools.Primary() == pools.Replica()) != (replica != "unreachable") {
				t.Fatal("replica routing changed")
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if err := pools.Ping(ctx); !errors.Is(err, context.Canceled) {
				t.Fatal("primary ping lost cancellation", err)
			}
			pools.Close()
			pools.Close()
			if err := pools.Ping(t.Context()); !errors.Is(err, postgres.ErrPoolUnavailable) {
				t.Fatal("shutdown did not close primary", err)
			}
		})
	}
}

func TestIntegrationPoolStartupSanitizesFailuresAndPreservesContext(t *testing.T) {
	dsn := pgtest.DSN(t)
	for _, options := range []postgres.PoolOptions{
		{},
		{PrimaryDSN: "postgres://postgres:secret-value@%invalid"},
		{PrimaryDSN: dsn, ReplicaDSN: "postgres://postgres:secret-value@%invalid"},
		{PrimaryDSN: "postgres://postgres:secret-value@127.0.0.1:1/test?sslmode=disable&connect_timeout=1"},
	} {
		pools, err := postgres.OpenPools(t.Context(), options)
		if pools != nil {
			pools.Close()
			t.Fatal("partial pool returned after failed startup")
		}
		if err == nil || strings.Contains(err.Error(), "secret-value") {
			t.Fatal("startup error exposed connection secrets", err)
		}
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
	defer cancel()
	if pools, err := postgres.OpenPools(ctx, postgres.PoolOptions{PrimaryDSN: dsn}); pools != nil || !errors.Is(err, context.DeadlineExceeded) {
		if pools != nil {
			pools.Close()
		}
		t.Fatal("cancelled startup lost its context", err)
	}
}
