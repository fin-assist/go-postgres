package postgres_test

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	postgres "github.com/fin-assist/go-postgres"
)

func ExampleNew() {
	var pool *pgxpool.Pool // opened and owned by the service
	manager := postgres.New(pool)
	_ = manager
}

func ExampleManager_Run() {
	var pool *pgxpool.Pool // opened and owned by the service
	manager := postgres.New(pool)
	_ = manager.Run(context.Background(), func(ctx context.Context) error {
		tx, _ := manager.Tx(ctx)
		_, err := tx.Exec(ctx, `INSERT INTO operation(id) VALUES ($1)`, 1)
		return err
	})
}

func ExampleManager_RunMapped() {
	var pool *pgxpool.Pool // opened and owned by the service
	manager := postgres.New(pool)
	missing := errors.New("category not found") // a service's domain sentinel
	mapper := func(err error) error {
		if postgres.IsViolation(err, "23503", "operation_category_fk") {
			return missing
		}
		return nil
	}
	_ = manager.RunMapped(context.Background(), pgx.TxOptions{}, mapper, func(ctx context.Context) error {
		tx, _ := manager.Tx(ctx)
		_, err := tx.Exec(ctx, `INSERT INTO operation(id,category_id) VALUES ($1,$2)`, 1, 7)
		return err
	})
}
