package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrInvalidDSN means that a pool's DSN cannot be parsed. Its text never contains the DSN.
var ErrInvalidDSN = errors.New("postgres: invalid DSN")

// ErrPoolUnavailable means that a pool could not be created or its primary did not answer.
// Driver diagnostics are intentionally omitted because they can contain connection secrets.
var ErrPoolUnavailable = errors.New("postgres: pool unavailable")

// PoolOptions configures pools owned by OpenPools. Empty ReplicaDSN, or a ReplicaDSN
// identical to PrimaryDSN, shares the primary. A distinct replica is created lazily;
// startup checks the primary only, as in the service template.
type PoolOptions struct {
	// PrimaryDSN is required; an empty value must not select an ambient default database.
	PrimaryDSN string
	ReplicaDSN string
	// MaxConns overrides pgxpool's default only when positive.
	MaxConns int32
	// AppName sets application_name on both pools when nonempty.
	AppName string
}

// Pools owns the pools created by OpenPools. It is safe for concurrent use, including
// repeated Close calls. Complete transactions and release acquired connections before
// Close, which waits for pgxpool users. A zero Pools holds no usable connections.
//
// Pools does not own a Manager's transaction boundaries. Construct New(p.Primary())
// once, then select routing in the service using that manager's transaction ownership.
type Pools struct {
	primary, replica *pgxpool.Pool
	lifecycle        *poolPair
}

// OpenPools creates the primary and optional replica and pings the primary. On failure
// it closes every pool it created. It never acquires ownership of a caller-supplied pool:
// New(pool) continues to leave pool ownership with its caller.
//
// Errors wrap ErrInvalidDSN, ErrPoolUnavailable, context.Canceled or
// context.DeadlineExceeded. Raw driver errors and DSNs are not returned.
func OpenPools(ctx context.Context, options PoolOptions) (*Pools, error) {
	pair, err := openPoolPair(ctx, options, newManagedPool)
	if err != nil {
		return nil, err
	}
	// The public constructor always uses newManagedPool, which returns *pgxpool.Pool.
	return &Pools{primary: pair.primary.(*pgxpool.Pool), replica: pair.replica.(*pgxpool.Pool), lifecycle: pair}, nil
}

// Primary returns the primary pool owned by p. The caller must not close it separately.
func (p *Pools) Primary() *pgxpool.Pool { return p.primary }

// Replica returns the read pool (the primary when no distinct replica was configured).
// Routing and read-after-write policy belong to the service. Do not close it separately.
func (p *Pools) Replica() *pgxpool.Pool { return p.replica }

// Ping checks the primary without exposing driver diagnostics or connection secrets.
func (p *Pools) Ping(ctx context.Context) error {
	if p == nil || p.primary == nil {
		return ErrPoolUnavailable
	}
	if err := p.primary.Ping(ctx); err != nil {
		return poolFailure("primary ping", err)
	}
	return nil
}

// Close closes each owned pool exactly once. It is a no-op for a nil or zero Pools.
func (p *Pools) Close() {
	if p != nil && p.lifecycle != nil {
		p.lifecycle.close()
	}
}

// managedPool is the lifecycle dependency; queries and transaction routing stay with consumers.
type managedPool interface {
	Ping(context.Context) error
	Close()
}

type poolPair struct {
	primary, replica managedPool
	distinct         bool
	once             sync.Once
}

func (p *poolPair) close() {
	p.once.Do(func() {
		if p.distinct {
			p.replica.Close()
		}
		p.primary.Close()
	})
}

func openPoolPair(ctx context.Context, options PoolOptions, open func(context.Context, string, PoolOptions) (managedPool, error)) (*poolPair, error) {
	primary, err := open(ctx, options.PrimaryDSN, options)
	if err != nil {
		return nil, poolFailure("primary pool", err)
	}
	pair := &poolPair{primary: primary, replica: primary}
	if options.ReplicaDSN != "" && options.ReplicaDSN != options.PrimaryDSN {
		replica, err := open(ctx, options.ReplicaDSN, options)
		if err != nil {
			pair.close()
			return nil, poolFailure("replica pool", err)
		}
		pair.replica, pair.distinct = replica, true
	}
	if err := primary.Ping(ctx); err != nil {
		pair.close()
		return nil, poolFailure("primary ping", err)
	}
	return pair, nil
}

func newManagedPool(ctx context.Context, dsn string, options PoolOptions) (managedPool, error) {
	if strings.TrimSpace(dsn) == "" {
		return nil, ErrInvalidDSN
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		// Parse errors can quote the password; never wrap the original.
		return nil, ErrInvalidDSN
	}
	if options.MaxConns > 0 {
		cfg.MaxConns = options.MaxConns
	}
	if options.AppName != "" {
		cfg.ConnConfig.RuntimeParams["application_name"] = options.AppName
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, poolFailure("create pool", err)
	}
	return pool, nil
}

func poolFailure(operation string, cause error) error {
	safe := ErrPoolUnavailable
	for _, known := range []error{ErrInvalidDSN, context.Canceled, context.DeadlineExceeded} {
		if errors.Is(cause, known) {
			safe = known
			break
		}
	}
	return fmt.Errorf("postgres: %s: %w", operation, safe)
}
