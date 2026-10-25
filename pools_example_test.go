package postgres_test

import (
	"context"

	postgres "github.com/fin-assist/go-postgres"
)

func ExampleOpenPools() {
	ctx := context.Background()
	pools, err := postgres.OpenPools(ctx, postgres.PoolOptions{
		PrimaryDSN: "postgres://localhost/service?sslmode=disable",
		MaxConns:   4,
		AppName:    "service",
	})
	if err != nil {
		panic(err)
	}
	defer pools.Close()
	manager := postgres.New(pools.Primary())
	_ = manager // pass the manager to service adapters
}
