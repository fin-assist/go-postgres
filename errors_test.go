package postgres

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestUnitIsViolation(t *testing.T) {
	fk := &pgconn.PgError{Code: "23503", ConstraintName: "category_fk"}
	wrapped := fmt.Errorf("query: %w", fmt.Errorf("repository: %w", fk))
	for _, tc := range []struct {
		name             string
		err              error
		code, constraint string
		want             bool
	}{
		{"direct", fk, "23503", "category_fk", true},
		{"wrapped", wrapped, "23503", "category_fk", true},
		{"wrong SQLSTATE", fk, "23505", "category_fk", false},
		{"wrong constraint", fk, "23503", "another_fk", false},
		{"empty SQLSTATE", fk, "", "category_fk", false},
		{"empty constraint", fk, "23503", "", false},
		{"nil", nil, "23503", "category_fk", false},
		{"ordinary", errors.New("domain error"), "23503", "category_fk", false},
		{"joined violations", errors.Join(fk, &pgconn.PgError{Code: "23503", ConstraintName: "another_fk"}), "23503", "category_fk", false},
		{"joined with unrelated error", errors.Join(fk, errors.New("cleanup failure")), "23503", "category_fk", false},
		{"wrapped join", fmt.Errorf("transaction: %w", errors.Join(fk, errors.New("cleanup failure"))), "23503", "category_fk", false},
		{"single-element join", errors.Join(fk), "23503", "category_fk", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsViolation(tc.err, tc.code, tc.constraint); got != tc.want {
				t.Fatalf("IsViolation=%v want %v", got, tc.want)
			}
		})
	}
}

func TestUnitMapErrorPreservesUnmappedErrors(t *testing.T) {
	fk := &pgconn.PgError{Code: "23503", ConstraintName: "category_fk"}
	domain := errors.New("domain error")
	for _, tc := range []struct {
		name  string
		err   error
		calls int
	}{
		{"nil", nil, 0},
		{"domain", domain, 0},
		{"wrapped domain", fmt.Errorf("operation: %w", domain), 0},
		{"unknown constraint", fk, 1},
		{"unsupported SQLSTATE", &pgconn.PgError{Code: "23502", ConstraintName: "category_fk"}, 0},
		{"joined errors", errors.Join(fk, domain), 0},
		{"wrapped join", fmt.Errorf("operation: %w", errors.Join(fk, domain)), 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			got := MapError(tc.err, func(error) error { calls++; return nil })
			if got != tc.err || calls != tc.calls { //nolint:errorlint // Require the exact wrapper, not just a matching cause.
				t.Fatalf("got=%v calls=%d want unchanged error and calls=%d", got, calls, tc.calls)
			}
		})
	}
	if got := MapError(fk, nil); got != fk { //nolint:errorlint // An absent mapper must return the identical error.
		t.Fatalf("nil mapper changed error: %v", got)
	}
}

func TestUnitMapErrorExpectedViolationHasNoDatabaseDetail(t *testing.T) {
	for _, code := range []string{"23503", "23505"} {
		t.Run(code, func(t *testing.T) {
			original := &pgconn.PgError{Code: code, ConstraintName: "expected_constraint", Detail: "Key (user_id)=(secret-user-id)"}
			wrapped := fmt.Errorf("query: %w", original)
			domain := errors.New("expected domain refusal")
			got := MapError(wrapped, func(err error) error {
				if err != wrapped { //nolint:errorlint // Assert the wrapper itself is preserved.
					t.Fatal("mapper lost original wrapper")
				}
				if IsViolation(err, code, "expected_constraint") {
					return domain
				}
				return nil
			})
			if !errors.Is(got, domain) {
				t.Fatalf("got=%v want domain sentinel", got)
			}
			var pgErr *pgconn.PgError
			if errors.As(got, &pgErr) || strings.Contains(got.Error(), "secret-user-id") || strings.Contains(got.Error(), original.Detail) {
				t.Fatal("domain error exposes PostgreSQL detail")
			}
		})
	}
}
