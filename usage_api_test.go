package postgres_test

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	postgres "github.com/fin-assist/go-postgres"
)

// Two service repositories participate in one business operation. Neither may
// commit nested work; all effects follow the outer request's completion.
func TestAPIComposedRepositoriesFollowOwningTransaction(t *testing.T) {
	for _, finish := range []string{"commit", "error", "panic", "cancel", "goexit"} {
		t.Run(finish, func(t *testing.T) {
			pool, manager := database(t)
			if tx, ok := manager.Tx(t.Context()); ok || tx != nil {
				t.Fatal("transaction escaped its operation")
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			refusal := errors.New("business operation refused")
			done := make(chan struct{})
			var outcome error
			var panicValue any
			returned := false
			go func() {
				defer close(done)
				defer func() { panicValue = recover() }()
				outcome = manager.RunWith(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable}, func(ctx context.Context) error {
					tx := transaction(t, manager, ctx)
					if shared, ok := postgres.Tx(ctx); !ok || shared != tx {
						t.Error("consumer transaction-provider contract changed")
						return refusal
					}
					if _, err := tx.Exec(ctx, `INSERT INTO child VALUES (1,7)`); err != nil {
						return err
					}
					if err := manager.RunWith(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(nested context.Context) error {
						if nested != ctx {
							return errors.New("nested operation replaced the transaction context")
						}
						var isolation string
						if err := tx.QueryRow(nested, `SHOW transaction_isolation`).Scan(&isolation); err != nil {
							return err
						}
						if isolation != "serializable" {
							return errors.New("nested options changed the owning transaction")
						}
						return writeParent(nested, manager, 7)
					}); err != nil {
						return err
					}
					switch finish {
					case "error":
						return refusal
					case "panic":
						panic(refusal)
					case "cancel":
						cancel()
					case "goexit":
						cancel()
						runtime.Goexit()
					}
					return nil
				})
				returned = true
			}()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("business operation did not release its transaction")
			}
			wantRows := 0
			switch finish {
			case "commit":
				wantRows = 1
				if !returned || outcome != nil || panicValue != nil {
					t.Fatalf("commit: returned=%v err=%v panic=%v", returned, outcome, panicValue)
				}
			case "error":
				if !errors.Is(outcome, refusal) {
					t.Fatal("business error changed", outcome)
				}
			case "panic":
				if panicValue != refusal { //nolint:errorlint // The panic payload must preserve identity.
					t.Fatal("panic payload changed", panicValue)
				}
			case "cancel":
				if !errors.Is(outcome, context.Canceled) {
					t.Fatalf("cancelled request must fail with context.Canceled: %v", outcome)
				}
			case "goexit":
				if returned || panicValue != nil {
					t.Fatal("Goexit became a normal return or panic")
				}
			}
			if got := rowCount(t, pool); got != wantRows || pool.Stat().AcquiredConns() != 0 {
				t.Fatalf("effects=%d want=%d acquired=%d", got, wantRows, pool.Stat().AcquiredConns())
			}
		})
	}
}

func TestAPIRefusesForeignOwnershipAndNestedMappersWithoutEffects(t *testing.T) {
	for _, foreign := range []bool{false, true} {
		t.Run(map[bool]string{false: "nested-mapper", true: "foreign-owner"}[foreign], func(t *testing.T) {
			pool, manager := database(t)
			other := postgres.New(pool)
			called := false
			err := manager.Run(t.Context(), func(ctx context.Context) error {
				if _, err := transaction(t, manager, ctx).Exec(ctx, `INSERT INTO child VALUES (1,NULL)`); err != nil {
					return err
				}
				fn := func(context.Context) error { called = true; return nil }
				if foreign {
					if tx, ok := other.Tx(ctx); ok || tx != nil {
						t.Error("foreign manager acquired another operation")
					}
					return other.Run(ctx, fn)
				}
				return manager.RunMapped(ctx, pgx.TxOptions{}, func(error) error { called = true; return nil }, fn)
			})
			want := postgres.ErrNestedMapper
			if foreign {
				want = postgres.ErrForeignTransaction
			}
			if !errors.Is(err, want) || called || rowCount(t, pool) != 0 {
				t.Fatalf("ownership refusal: err=%v callback=%v", err, called)
			}
		})
	}
}

func TestAPIDeferredConstraintBecomesDomainErrorOnlyAfterCommit(t *testing.T) {
	pool, manager := database(t)
	want := errors.New("parent no longer exists")
	finished, mapped := false, false
	err := manager.RunMapped(t.Context(), pgx.TxOptions{}, func(err error) error {
		mapped = true
		if !finished || !postgres.IsViolation(err, "23503", "child_parent_fk") {
			t.Error("mapper did not observe the completed operation's exact constraint")
		}
		return want
	}, func(ctx context.Context) error {
		_, err := transaction(t, manager, ctx).Exec(ctx, `INSERT INTO child VALUES (1,912345)`)
		finished = err == nil
		return err
	})
	var raw *pgconn.PgError
	if !mapped || !errors.Is(err, want) || errors.As(err, &raw) || strings.Contains(err.Error(), "912345") || rowCount(t, pool) != 0 {
		t.Fatal("commit refusal leaked SQL details or persisted effects", err)
	}
}

func TestAPIClosedPoolRefusesBeforeBusinessCallback(t *testing.T) {
	pool, manager := database(t)
	pool.Close()
	called := false
	err := manager.Run(t.Context(), func(context.Context) error { called = true; return nil })
	if err == nil || called {
		t.Fatal("closed dependency ran the operation", err, called)
	}
}
