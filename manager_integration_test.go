package postgres_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	postgres "github.com/fin-assist/go-postgres"
	"github.com/fin-assist/go-postgres/internal/pgtest"
)

func database(t *testing.T) (*pgxpool.Pool, *postgres.Manager) {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), pgtest.DSN(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(context.Background(), `CREATE TABLE parent (id int PRIMARY KEY);
 CREATE TABLE child (id int PRIMARY KEY, parent_id int, CONSTRAINT child_parent_fk FOREIGN KEY(parent_id) REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);
 CREATE TABLE immediate_child (id int PRIMARY KEY, parent_id int, CONSTRAINT immediate_parent_fk FOREIGN KEY(parent_id) REFERENCES parent(id));`)
	if err != nil {
		t.Fatal(err)
	}
	return pool, postgres.New(pool)
}

func transaction(t *testing.T, m *postgres.Manager, ctx context.Context) pgx.Tx {
	t.Helper()
	tx, ok := m.Tx(ctx)
	if !ok {
		t.Fatal("no transaction")
	}
	return tx
}

func rowCount(t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM child`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestDeferredViolationAtCommit(t *testing.T) {
	pool, m := database(t)
	ctx := context.Background()
	missing := errors.New("parent not found")
	callbackFinished := false
	err := m.RunMapped(ctx, pgx.TxOptions{}, func(err error) error {
		if !callbackFinished {
			t.Fatal("deferred error reported before callback finished")
		}
		if postgres.IsViolation(err, "23503", "child_parent_fk") {
			return missing
		}
		return nil
	}, func(ctx context.Context) error {
		_, err := transaction(t, m, ctx).Exec(ctx, `INSERT INTO child VALUES (1, 912345)`)
		if err != nil {
			t.Fatalf("INSERT must succeed until commit: %v", err)
		}
		callbackFinished = true
		return nil
	})
	if !errors.Is(err, missing) || strings.Contains(err.Error(), "912345") {
		t.Fatalf("domain error: %v", err)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		t.Fatal("domain error retains raw key detail")
	}
	if rowCount(t, pool) != 0 {
		t.Fatal("failed commit persisted a row")
	}
}

func TestDeferredReferenceCanBeRepairedByAnotherRepository(t *testing.T) {
	pool, m := database(t)
	err := m.Run(context.Background(), func(ctx context.Context) error {
		tx := transaction(t, m, ctx)
		if _, err := tx.Exec(ctx, `INSERT INTO child VALUES (1,7)`); err != nil {
			return err
		}
		// Simulates another repository/library using the consumer-side Tx interface.
		return writeParent(ctx, m, 7)
	})
	if err != nil || rowCount(t, pool) != 1 {
		t.Fatalf("repaired transaction: %v", err)
	}
}

func writeParent(ctx context.Context, provider interface {
	Tx(context.Context) (pgx.Tx, bool)
}, id int) error {
	tx, ok := provider.Tx(ctx)
	if !ok {
		return errors.New("transaction required")
	}
	_, err := tx.Exec(ctx, `INSERT INTO parent VALUES ($1)`, id)
	return err
}

func TestImmediateViolations(t *testing.T) {
	for _, tc := range []struct{ name, sql, state, constraint string }{
		{"foreign key", `INSERT INTO immediate_child VALUES (1,42)`, "23503", "immediate_parent_fk"},
		{"unique", `INSERT INTO parent VALUES (1),(1)`, "23505", "parent_pkey"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool, m := database(t)
			want := errors.New("expected operation refusal")
			err := m.RunMapped(context.Background(), pgx.TxOptions{}, func(err error) error {
				if postgres.IsViolation(err, tc.state, tc.constraint) {
					return want
				}
				return nil
			}, func(ctx context.Context) error {
				_, err := transaction(t, m, ctx).Exec(ctx, tc.sql)
				return fmt.Errorf("write: %w", err)
			})
			if !errors.Is(err, want) {
				t.Fatalf("error %v", err)
			}
			if rowCount(t, pool) != 0 {
				t.Fatal("unexpected persisted data")
			}
		})
	}
}

func TestUnknownCommitViolationIsPreserved(t *testing.T) {
	_, m := database(t)
	err := m.RunMapped(context.Background(), pgx.TxOptions{}, func(err error) error {
		if postgres.IsViolation(err, "23503", "some_other_fk") {
			return errors.New("must not happen")
		}
		return nil
	}, func(ctx context.Context) error {
		_, err := transaction(t, m, ctx).Exec(ctx, `INSERT INTO child VALUES (1,8)`)
		return err
	})
	if !postgres.IsViolation(err, "23503", "child_parent_fk") {
		t.Fatalf("lost diagnostic cause: %v", err)
	}
}

func TestNestedRollbackAndPanic(t *testing.T) {
	for _, panics := range []bool{false, true} {
		t.Run(fmt.Sprint(panics), func(t *testing.T) {
			pool, m := database(t)
			boom := errors.New("outer failure")
			func() {
				defer func() {
					p := recover()
					if panics && p != boom { //nolint:errorlint // Panic payload identity must survive cleanup.
						t.Errorf("panic: %v", p)
					}
					if !panics && p != nil {
						t.Errorf("unexpected panic: %v", p)
					}
				}()
				err := m.Run(context.Background(), func(ctx context.Context) error {
					err := m.Run(ctx, func(ctx context.Context) error {
						_, err := transaction(t, m, ctx).Exec(ctx, `INSERT INTO child VALUES (1,NULL)`)
						return err
					})
					if err != nil {
						return err
					}
					if panics {
						panic(boom)
					}
					return boom
				})
				if !panics && !errors.Is(err, boom) {
					t.Errorf("lost outer error: %v", err)
				}
			}()
			if rowCount(t, pool) != 0 {
				t.Fatal("nested run committed early")
			}
		})
	}
}

func TestConcurrentIndependentTransactions(t *testing.T) {
	pool, m := database(t)
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for id := 1; id <= 4; id++ {
		wg.Go(func() {
			errs <- m.Run(context.Background(), func(ctx context.Context) error {
				_, err := transaction(t, m, ctx).Exec(ctx, `INSERT INTO child VALUES ($1,NULL)`, id)
				return err
			})
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if rowCount(t, pool) != 4 {
		t.Fatal("independent transactions lost writes")
	}
}
