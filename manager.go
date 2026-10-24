package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ErrForeignTransaction means that ctx belongs to another Manager. No callback is run.
var ErrForeignTransaction = errors.New("postgres: transaction belongs to another manager")

// ErrNestedMapper means a nested call tried to install an operation mapper. Only the
// outer transaction can select a mapper: a nested callback cannot observe COMMIT.
var ErrNestedMapper = errors.New("postgres: mapper requires an outer transaction")

// Beginner is implemented by pgxpool.Pool. The caller owns and closes the pool.
type Beginner interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

// Manager executes callbacks in transactions. It is safe for concurrent use when its
// Beginner is (as pgxpool.Pool is). A transaction context must not be used concurrently
// or retained after its callback returns. Construct one Manager per pool, not per call.
type Manager struct {
	pool Beginner
}

// New creates a manager for a non-nil pool; it does not open or own the pool.
func New(pool Beginner) *Manager { return &Manager{pool: pool} }

type txKey struct{}

type transaction struct {
	owner *Manager
	tx    pgx.Tx
}

// Tx returns the transaction carried by ctx, for integrations that require pgx.Tx.
// The transaction is owned by Run; the caller must not commit or roll it back.
func Tx(ctx context.Context) (pgx.Tx, bool) {
	state, ok := ctx.Value(txKey{}).(*transaction)
	if !ok {
		return nil, false
	}
	return state.tx, true
}

// Tx implements the transaction-provider interface used by idempotency and outbox.
// A context belonging to another Manager has no transaction for this manager.
func (m *Manager) Tx(ctx context.Context) (pgx.Tx, bool) {
	state, ok := ctx.Value(txKey{}).(*transaction)
	if !ok || state.owner != m {
		return nil, false
	}
	return state.tx, true
}

// Run commits when fn succeeds and rolls back on error, panic or runtime.Goexit. A nested Run joins
// the same manager's outer transaction without committing it or creating a savepoint.
// It returns ErrForeignTransaction for another manager's transaction context.
// Callbacks must propagate nested errors and must not call remote services.
func (m *Manager) Run(ctx context.Context, fn func(context.Context) error) error {
	return m.RunWith(ctx, pgx.TxOptions{}, fn)
}

// RunWith is Run with explicit PostgreSQL transaction options. When joining an outer
// transaction, opts are ignored; the outer call already selected the options.
func (m *Manager) RunWith(ctx context.Context, opts pgx.TxOptions, fn func(context.Context) error) error {
	return m.run(ctx, opts, nil, fn)
}

// RunMapped runs one business operation with a mapper supplied by its adapter. The
// mapper sees constraint errors from fn and from the actual outer COMMIT. No mapper
// is installed on a nested transaction: that returns ErrNestedMapper before fn runs.
// Use Run for nested work and define all expected constraints in the outer mapper.
// Mapping never applies to BEGIN or ROLLBACK errors. See MapError for its contract.
func (m *Manager) RunMapped(ctx context.Context, opts pgx.TxOptions, mapper ErrorMapper, fn func(context.Context) error) error {
	return m.run(ctx, opts, mapper, fn)
}

func (m *Manager) run(ctx context.Context, opts pgx.TxOptions, mapper ErrorMapper, fn func(context.Context) error) (err error) {
	if state, ok := ctx.Value(txKey{}).(*transaction); ok {
		if state.owner != m {
			return ErrForeignTransaction
		}
		if mapper != nil {
			return ErrNestedMapper
		}
		return fn(ctx)
	}
	tx, err := m.pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	committed := false
	defer func() {
		// Cleanup must reach PostgreSQL after request cancellation, but is bounded.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if p := recover(); p != nil {
			_ = tx.Rollback(cleanupCtx)
			panic(p)
		}
		// Goexit runs defers without a panic or an assignment to err.
		if !committed {
			if rbErr := tx.Rollback(cleanupCtx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				err = errors.Join(err, fmt.Errorf("rollback: %w", rbErr))
				return
			}
		}
		err = MapError(err, mapper)
	}()
	if err = fn(context.WithValue(ctx, txKey{}, &transaction{owner: m, tx: tx})); err != nil {
		return err
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}
