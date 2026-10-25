// Package postgres provides transaction management and constraint error mapping for
// pgx v5 (FIN-195). A service supplies a pool; its adapters supply operation-specific
// mappers; its usecases choose transaction boundaries through a Run interface.
//
// New leaves a supplied pool caller-owned. OpenPools optionally creates and owns
// primary/replica pools; the service still selects routing and transaction boundaries.
// The package owns no schema objects and has no global logger, environment settings,
// domain errors or client-facing error codes.
// See README.md for integration and the deferred-constraint COMMIT example.
package postgres
