package postgres

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type poolFake struct {
	pingErr error
	pings   atomic.Int32
	closes  atomic.Int32
}

func (p *poolFake) Ping(context.Context) error { p.pings.Add(1); return p.pingErr }
func (p *poolFake) Close()                     { p.closes.Add(1) }

func TestUnitPoolPairOwnershipAndRouting(t *testing.T) {
	for _, replica := range []string{"", "primary", "replica"} {
		t.Run("replica="+replica, func(t *testing.T) {
			primary, secondary := &poolFake{}, &poolFake{}
			ctx := t.Context()
			options := PoolOptions{PrimaryDSN: "primary", ReplicaDSN: replica, MaxConns: 3, AppName: "service"}
			opened := []string{}
			pair, err := openPoolPair(ctx, options, func(got context.Context, dsn string, settings PoolOptions) (managedPool, error) {
				if got != ctx || settings != options {
					t.Fatal("pool construction lost context or settings")
				}
				opened = append(opened, dsn)
				if dsn == "primary" {
					return primary, nil
				}
				return secondary, nil
			})
			if err != nil || pair.primary != primary || primary.pings.Load() != 1 || secondary.pings.Load() != 0 {
				t.Fatalf("primary startup: pair=%v err=%v", pair, err)
			}
			wantOpens := 1
			if replica == "replica" {
				wantOpens = 2
				if pair.replica != secondary {
					t.Fatal("distinct replica was not retained")
				}
			} else if pair.replica != primary {
				t.Fatal("empty or identical replica must share primary")
			}
			if len(opened) != wantOpens {
				t.Fatalf("opened=%v", opened)
			}
			pools := &Pools{lifecycle: pair}
			var wg sync.WaitGroup
			for range 8 {
				wg.Go(pools.Close)
			}
			wg.Wait()
			if primary.closes.Load() != 1 || secondary.closes.Load() != int32(wantOpens-1) {
				t.Fatalf("close counts primary=%d replica=%d", primary.closes.Load(), secondary.closes.Load())
			}
		})
	}
}

func TestUnitPoolPairFailureClosesOnlyOwnedResources(t *testing.T) {
	for _, stage := range []string{"primary", "replica", "ping"} {
		t.Run(stage, func(t *testing.T) {
			primary, secondary := &poolFake{}, &poolFake{}
			secret := errors.New("driver failure with password=never-return-this")
			if stage == "ping" {
				primary.pingErr = secret
			}
			pair, err := openPoolPair(t.Context(), PoolOptions{PrimaryDSN: "primary", ReplicaDSN: "replica"},
				func(_ context.Context, dsn string, _ PoolOptions) (managedPool, error) {
					if dsn == stage {
						return nil, secret
					}
					if dsn == "primary" {
						return primary, nil
					}
					return secondary, nil
				})
			if pair != nil || !errors.Is(err, ErrPoolUnavailable) || strings.Contains(err.Error(), "never-return-this") || errors.Is(err, secret) {
				t.Fatalf("failure returned a resource or exposed driver diagnostics: pair=%v err=%v", pair, err)
			}
			wantPrimary, wantReplica := int32(0), int32(0)
			if stage != "primary" {
				wantPrimary = 1
			}
			if stage == "ping" {
				wantReplica = 1
			}
			if primary.closes.Load() != wantPrimary || secondary.closes.Load() != wantReplica {
				t.Fatalf("partial startup leaked or closed an unowned pool: %d/%d", primary.closes.Load(), secondary.closes.Load())
			}
		})
	}
}

func TestUnitPoolFailuresPreserveOnlySafeCauses(t *testing.T) {
	ended := func(cause error) context.Context {
		if errors.Is(cause, context.Canceled) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			return ctx
		}
		ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, 0))
		t.Cleanup(cancel)
		return ctx
	}
	for _, tc := range []struct {
		name        string
		callerEnded bool
		driver      error
		want        error
	}{
		{"invalid DSN", false, ErrInvalidDSN, ErrInvalidDSN},
		// pgx implements connect_timeout with its own deadline: the caller did not time out.
		{"driver deadline with live caller", false, context.DeadlineExceeded, ErrPoolUnavailable},
		{"driver cancellation with live caller", false, context.Canceled, ErrPoolUnavailable},
		{"caller canceled", true, context.Canceled, context.Canceled},
		{"caller deadline", true, context.DeadlineExceeded, context.DeadlineExceeded},
		{"caller ended, opaque driver error", true, errors.New("driver: password=secret"), nil},
	} {
		for _, stage := range []string{"create", "ping"} {
			t.Run(tc.name+"/"+stage, func(t *testing.T) {
				ctx := t.Context()
				want := tc.want
				if tc.callerEnded {
					cause := tc.want
					if cause == nil {
						cause = context.Canceled
					}
					ctx = ended(cause)
					want = ctx.Err()
				}
				primary := &poolFake{}
				if stage == "ping" {
					primary.pingErr = tc.driver
				}
				_, err := openPoolPair(ctx, PoolOptions{PrimaryDSN: "primary"}, func(context.Context, string, PoolOptions) (managedPool, error) {
					if stage == "create" {
						return nil, tc.driver
					}
					return primary, nil
				})
				if !errors.Is(err, want) {
					t.Fatalf("safe cause: got %v, want %v", err, want)
				}
				for _, other := range []error{ErrInvalidDSN, ErrPoolUnavailable, context.Canceled, context.DeadlineExceeded} {
					if !errors.Is(want, other) && errors.Is(err, other) {
						t.Fatalf("error %v also reports %v", err, other)
					}
				}
				if strings.Contains(err.Error(), "secret") {
					t.Fatal("driver diagnostics leaked", err)
				}
			})
		}
	}
}

func TestUnitPoolConfigIsLazyAndPreservesExplicitSettings(t *testing.T) {
	// Creating a pgxpool is lazy; no Ping/Acquire is performed in this unit test.
	const dsn = "postgres://test:password@127.0.0.1:1/test?sslmode=disable&application_name=dsn-name"
	defaults, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, options := range []PoolOptions{{}, {MaxConns: -1}, {MaxConns: 3, AppName: "service"}} {
		pool, err := newManagedPool(t.Context(), dsn, options)
		if err != nil {
			t.Fatal(err)
		}
		cfg := pool.(*pgxpool.Pool).Config()
		pool.Close()
		maxConns, appName := defaults.MaxConns, "dsn-name"
		if options.MaxConns > 0 {
			maxConns = options.MaxConns
			appName = options.AppName
		}
		if cfg.MaxConns != maxConns || cfg.ConnConfig.RuntimeParams["application_name"] != appName {
			t.Fatal("pool defaults or explicit settings changed")
		}
	}
	_, err = newManagedPool(t.Context(), "postgres://user:secret-pw@%zz", PoolOptions{})
	if !errors.Is(err, ErrInvalidDSN) || strings.Contains(err.Error(), "secret-pw") {
		t.Fatal("DSN parsing leaked credentials or lost its safe error")
	}
	for _, dsn := range []string{"", "  \t\n"} {
		if pool, err := newManagedPool(t.Context(), dsn, PoolOptions{}); pool != nil || !errors.Is(err, ErrInvalidDSN) {
			t.Fatal("empty DSN selected an ambient database", err)
		}
	}
}

func TestUnitZeroPoolsDoNotOwnResources(t *testing.T) {
	var pools *Pools
	pools.Close()
	if !errors.Is(pools.Ping(t.Context()), ErrPoolUnavailable) {
		t.Fatal("nil pools looked healthy")
	}
	pools = &Pools{}
	pools.Close()
	if pools.Primary() != nil || pools.Replica() != nil || !errors.Is(pools.Ping(t.Context()), ErrPoolUnavailable) {
		t.Fatal("zero pools looked configured")
	}
}
