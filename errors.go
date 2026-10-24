package postgres

import "github.com/jackc/pgx/v5/pgconn"

// ErrorMapper returns a domain error for an expected constraint violation, or nil
// when the error is not expected in this business operation. Define it in the service
// adapter, not the usecase. Match SQLSTATE and constraint name with IsViolation.
// Never wrap the PostgreSQL error or copy Detail into a domain error: it contains keys.
type ErrorMapper func(error) error

// IsViolation reports whether err is a single PostgreSQL error with exactly the
// SQLSTATE and constraint name requested. Empty names and joined error trees never
// match: choosing one violation from several causes would be ambiguous.
func IsViolation(err error, sqlstate, constraint string) bool {
	pgErr := singlePgError(err)
	return pgErr != nil && sqlstate != "" && constraint != "" &&
		pgErr.Code == sqlstate && pgErr.ConstraintName == constraint
}

// MapError translates a single FK (23503) or UNIQUE/primary-key (23505) violation
// using mapper. It preserves nil, non-PostgreSQL/domain errors, joined error trees,
// and unrecognised errors unchanged. A nil mapper or a nil mapping preserves err.
// Use it in a repository for statements outside RunMapped; inside RunMapped return
// query/Scan/Rows.Err/BatchResults errors to the outer operation mapper unchanged.
func MapError(err error, mapper ErrorMapper) error {
	pgErr := singlePgError(err)
	if mapper == nil || pgErr == nil || (pgErr.Code != "23503" && pgErr.Code != "23505") {
		return err
	}
	if mapped := mapper(err); mapped != nil {
		return mapped
	}
	return err
}

func singlePgError(err error) *pgconn.PgError {
	for err != nil {
		if pgErr, ok := err.(*pgconn.PgError); ok { //nolint:errorlint // Walk single-cause wrappers explicitly; errors.As also selects arbitrary joined causes.
			return pgErr
		}
		wrapped, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil
		}
		err = wrapped.Unwrap()
	}
	return nil
}
