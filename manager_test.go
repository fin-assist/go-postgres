package postgres

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type managerFakeBeginner struct {
	tx    pgx.Tx
	err   error
	calls int
	opts  pgx.TxOptions
}

func (b *managerFakeBeginner) BeginTx(_ context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	b.calls++
	b.opts = opts
	return b.tx, b.err
}

type managerFakeTx struct {
	pgx.Tx
	commits, rollbacks     int
	commitErr, rollbackErr error
	onCommit               func(context.Context)
	onRollback             func(context.Context)
}

func (tx *managerFakeTx) Commit(ctx context.Context) error {
	tx.commits++
	if tx.onCommit != nil {
		tx.onCommit(ctx)
	}
	return tx.commitErr
}

func (tx *managerFakeTx) Rollback(ctx context.Context) error {
	tx.rollbacks++
	if tx.onRollback != nil {
		tx.onRollback(ctx)
	}
	return tx.rollbackErr
}

func TestUnitManagerRunWithSuccess(t *testing.T) {
	tx := &managerFakeTx{}
	pool := &managerFakeBeginner{tx: tx}
	m := New(pool)
	opts := pgx.TxOptions{IsoLevel: pgx.Serializable, AccessMode: pgx.ReadOnly, DeferrableMode: pgx.Deferrable}
	if _, ok := Tx(context.Background()); ok {
		t.Fatal("transaction outside Run")
	}
	if _, ok := m.Tx(context.Background()); ok {
		t.Fatal("manager transaction outside Run")
	}
	called := false
	err := m.RunWith(context.Background(), opts, func(ctx context.Context) error {
		called = true
		if got, ok := Tx(ctx); !ok || got != tx {
			t.Fatal("package Tx did not return active transaction")
		}
		if got, ok := m.Tx(ctx); !ok || got != tx {
			t.Fatal("manager Tx did not return active transaction")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if !called || pool.calls != 1 || pool.opts != opts || tx.commits != 1 || tx.rollbacks != 0 {
		t.Fatalf("called=%v begins=%d opts=%+v commits=%d rollbacks=%d", called, pool.calls, pool.opts, tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerBeginErrorIsNotMapped(t *testing.T) {
	original := &pgconn.PgError{Code: "23503", ConstraintName: "category_fk"}
	pool := &managerFakeBeginner{err: original}
	m := New(pool)
	err := m.RunMapped(context.Background(), pgx.TxOptions{}, func(error) error {
		t.Fatal("mapper called for BEGIN")
		return nil
	}, func(context.Context) error {
		t.Fatal("callback called after failed BEGIN")
		return nil
	})
	if !errors.Is(err, original) {
		t.Fatalf("BEGIN cause lost: %v", err)
	}
}

func TestUnitManagerCallbackErrorPreserved(t *testing.T) {
	domain := errors.New("domain failure")
	original := fmt.Errorf("operation: %w", domain)
	tx := &managerFakeTx{}
	m := New(&managerFakeBeginner{tx: tx})
	err := m.RunMapped(context.Background(), pgx.TxOptions{}, func(error) error {
		t.Fatal("mapper called for domain error")
		return nil
	}, func(context.Context) error { return original })
	if err != original || !errors.Is(err, domain) { //nolint:errorlint // Preserve the exact existing domain wrapper.
		t.Fatalf("callback error changed: %v", err)
	}
	if tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerMapsStatementAndCommit(t *testing.T) {
	for _, stage := range []string{"statement", "commit"} {
		t.Run(stage, func(t *testing.T) {
			original := &pgconn.PgError{Code: "23503", ConstraintName: "category_fk", Detail: "private-key"}
			domain := errors.New("category not found")
			tx := &managerFakeTx{rollbackErr: pgx.ErrTxClosed}
			if stage == "commit" {
				tx.commitErr = original
			}
			m := New(&managerFakeBeginner{tx: tx})
			mapped := 0
			err := m.RunMapped(context.Background(), pgx.TxOptions{}, func(err error) error {
				mapped++
				if !IsViolation(err, "23503", "category_fk") {
					t.Fatalf("wrong mapper input: %v", err)
				}
				return domain
			}, func(context.Context) error {
				if stage == "statement" {
					return original
				}
				return nil
			})
			if !errors.Is(err, domain) || mapped != 1 {
				t.Fatalf("err=%v mapper calls=%d", err, mapped)
			}
			if errors.Is(err, original) {
				t.Fatal("mapped error retains PostgreSQL detail")
			}
			wantCommits := 0
			if stage == "commit" {
				wantCommits = 1
			}
			if tx.commits != wantCommits || tx.rollbacks != 1 {
				t.Fatalf("commits=%d rollbacks=%d", tx.commits, tx.rollbacks)
			}
		})
	}
}

func TestUnitManagerCommitErrorPreserved(t *testing.T) {
	original := errors.New("connection lost on COMMIT")
	tx := &managerFakeTx{commitErr: original, rollbackErr: pgx.ErrTxClosed}
	m := New(&managerFakeBeginner{tx: tx})
	err := m.Run(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, original) {
		t.Fatalf("COMMIT cause lost: %v", err)
	}
	if tx.commits != 1 || tx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerRollbackFailureAndCanceledContext(t *testing.T) {
	original := errors.New("callback failed")
	rollbackErr := &pgconn.PgError{Code: "23503", ConstraintName: "category_fk"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &managerFakeTx{rollbackErr: rollbackErr}
	tx.onRollback = func(cleanup context.Context) {
		if cleanup.Err() != nil {
			t.Fatalf("cleanup already canceled: %v", cleanup.Err())
		}
		deadline, ok := cleanup.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 6*time.Second {
			t.Fatalf("cleanup deadline=%v exists=%v", deadline, ok)
		}
	}
	m := New(&managerFakeBeginner{tx: tx})
	err := m.RunMapped(ctx, pgx.TxOptions{}, func(error) error {
		t.Fatal("rollback error was mapped")
		return nil
	}, func(context.Context) error {
		cancel()
		return original
	})
	if !errors.Is(err, original) || !errors.Is(err, rollbackErr) {
		t.Fatalf("rollback or callback cause lost: %v", err)
	}
	if tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerRollbackFailurePreventsDomainMapping(t *testing.T) {
	original := &pgconn.PgError{Code: "23505", ConstraintName: "name_unique"}
	domain, cleanup := errors.New("name taken"), errors.New("rollback connection lost")
	tx := &managerFakeTx{rollbackErr: cleanup}
	m := New(&managerFakeBeginner{tx: tx})
	mapped := 0
	err := m.RunMapped(context.Background(), pgx.TxOptions{}, func(error) error { mapped++; return domain }, func(context.Context) error { return original })
	if mapped != 0 || errors.Is(err, domain) || !errors.Is(err, cleanup) || !errors.Is(err, original) {
		t.Fatalf("mapped=%d err=%v", mapped, err)
	}
}

func TestUnitManagerPanicRollsBackAndRepanics(t *testing.T) {
	panicValue := &struct{ message string }{"callback panic"}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := &managerFakeTx{rollbackErr: errors.New("rollback failure")}
	tx.onRollback = func(cleanup context.Context) {
		if cleanup.Err() != nil {
			t.Fatal("panic rollback uses canceled context")
		}
		if _, ok := cleanup.Deadline(); !ok {
			t.Fatal("panic rollback has no deadline")
		}
	}
	m := New(&managerFakeBeginner{tx: tx})
	func() {
		defer func() {
			if got := recover(); got != panicValue {
				t.Fatalf("panic changed: %v", got)
			}
		}()
		_ = m.Run(ctx, func(context.Context) error { cancel(); panic(panicValue) })
		t.Fatal("Run swallowed panic")
	}()
	if tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("commits=%d rollbacks=%d", tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerGoexitRollsBack(t *testing.T) {
	for _, nested := range []bool{false, true} {
		for _, rollbackFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("nested=%v/rollbackFails=%v", nested, rollbackFails), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				tx := &managerFakeTx{}
				if rollbackFails {
					tx.rollbackErr = &pgconn.PgError{Code: "23503", ConstraintName: "category_fk"}
				}
				tx.onRollback = func(cleanup context.Context) {
					if cleanup.Err() != nil {
						t.Errorf("Goexit cleanup already canceled: %v", cleanup.Err())
					}
					deadline, ok := cleanup.Deadline()
					if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 6*time.Second {
						t.Errorf("cleanup deadline=%v exists=%v", deadline, ok)
					}
				}
				pool := &managerFakeBeginner{tx: tx}
				m := New(pool)
				mapped, returned := false, false
				done := make(chan struct{})
				go func() {
					defer close(done)
					_ = m.RunMapped(ctx, pgx.TxOptions{}, func(error) error {
						mapped = true
						return nil
					}, func(ctx context.Context) error {
						stop := func(context.Context) error {
							cancel()
							runtime.Goexit()
							return nil
						}
						if nested {
							return m.Run(ctx, stop)
						}
						return stop(ctx)
					})
					returned = true
				}()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
					t.Fatal("Goexit cleanup did not finish")
				}
				if pool.calls != 1 || tx.commits != 0 || tx.rollbacks != 1 || mapped || returned {
					t.Fatalf("begins=%d commits=%d rollbacks=%d mapped=%v returned=%v", pool.calls, tx.commits, tx.rollbacks, mapped, returned)
				}
			})
		}
	}
}

func TestUnitManagerNestedRunJoinsAndIgnoresOptions(t *testing.T) {
	tx := &managerFakeTx{}
	pool := &managerFakeBeginner{tx: tx}
	m := New(pool)
	outerOpts := pgx.TxOptions{IsoLevel: pgx.Serializable}
	err := m.RunWith(context.Background(), outerOpts, func(ctx context.Context) error {
		return m.RunWith(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly}, func(nested context.Context) error {
			if nested != ctx {
				t.Fatal("nested Run replaced context")
			}
			if current, ok := m.Tx(nested); !ok || current != tx {
				t.Fatal("nested Run lost transaction")
			}
			if pool.calls != 1 || tx.commits != 0 || tx.rollbacks != 0 {
				t.Fatal("nested Run began or completed transaction")
			}
			return m.Run(nested, func(context.Context) error { return nil })
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if pool.calls != 1 || pool.opts != outerOpts || tx.commits != 1 || tx.rollbacks != 0 {
		t.Fatalf("begins=%d opts=%+v commits=%d rollbacks=%d", pool.calls, pool.opts, tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerNestedErrorRollsBackOuter(t *testing.T) {
	domain := errors.New("nested failure")
	tx := &managerFakeTx{}
	m := New(&managerFakeBeginner{tx: tx})
	err := m.Run(context.Background(), func(ctx context.Context) error {
		return m.Run(ctx, func(context.Context) error { return domain })
	})
	if !errors.Is(err, domain) || tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("err=%v commits=%d rollbacks=%d", err, tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerNestedMapperRejected(t *testing.T) {
	tx := &managerFakeTx{}
	m := New(&managerFakeBeginner{tx: tx})
	err := m.Run(context.Background(), func(ctx context.Context) error {
		return m.RunMapped(ctx, pgx.TxOptions{}, func(error) error { t.Fatal("nested mapper called"); return nil }, func(context.Context) error { t.Fatal("nested mapped callback called"); return nil })
	})
	if !errors.Is(err, ErrNestedMapper) || tx.commits != 0 || tx.rollbacks != 1 {
		t.Fatalf("err=%v commits=%d rollbacks=%d", err, tx.commits, tx.rollbacks)
	}
}

func TestUnitManagerForeignTransactionRejected(t *testing.T) {
	for _, mode := range []string{"Run", "RunWith", "RunMapped"} {
		t.Run(mode, func(t *testing.T) {
			outerTx := &managerFakeTx{}
			owner := New(&managerFakeBeginner{tx: outerTx})
			foreignPool := &managerFakeBeginner{tx: &managerFakeTx{}}
			foreign := New(foreignPool)
			err := owner.Run(context.Background(), func(ctx context.Context) error {
				if got, ok := foreign.Tx(ctx); ok || got != nil {
					t.Fatal("foreign manager exposes transaction")
				}
				callback := func(context.Context) error { t.Fatal("foreign callback ran"); return nil }
				switch mode {
				case "Run":
					return foreign.Run(ctx, callback)
				case "RunWith":
					return foreign.RunWith(ctx, pgx.TxOptions{}, callback)
				default:
					return foreign.RunMapped(ctx, pgx.TxOptions{}, func(error) error { t.Fatal("foreign mapper ran"); return nil }, callback)
				}
			})
			if !errors.Is(err, ErrForeignTransaction) || foreignPool.calls != 0 || outerTx.commits != 0 || outerTx.rollbacks != 1 {
				t.Fatalf("err=%v foreign begins=%d outer commits=%d rollbacks=%d", err, foreignPool.calls, outerTx.commits, outerTx.rollbacks)
			}
		})
	}
}
