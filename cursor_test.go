package pgxquery_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxquery"
	"go.einride.tech/aip/ordering"
)

var _ = Describe("CursorRewriter", func() {
	It("emits a single-column predicate for ASC ordering", func() {
		r := &pgxquery.CursorRewriter{
			Fields:  []ordering.Field{{Path: "id"}},
			Values:  []any{"abc"},
			Columns: map[string]string{"id": "id"},
		}
		sql, args, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE (("id" > $1)) AND TRUE`))
		Expect(args).To(Equal([]any{"abc"}))
	})

	It("flips comparison direction for DESC columns", func() {
		r := &pgxquery.CursorRewriter{
			Fields:  []ordering.Field{{Path: "name", Desc: true}},
			Values:  []any{"Z"},
			Columns: map[string]string{"name": "name"},
		}
		sql, _, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`"name" < $1`))
	})

	It("maps AIP paths to their backing DB columns", func() {
		r := &pgxquery.CursorRewriter{
			Fields:  []ordering.Field{{Path: "create_time", Desc: true}},
			Values:  []any{"2025-01-01"},
			Columns: map[string]string{"create_time": "created_at"},
		}
		sql, _, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`"created_at" < $1`))
	})

	It("emits a compound predicate with equality prefixes", func() {
		r := &pgxquery.CursorRewriter{
			Fields: []ordering.Field{
				{Path: "name", Desc: true},
				{Path: "id"},
			},
			Values:  []any{"Alice", "uuid-1"},
			Columns: map[string]string{"name": "name", "id": "id"},
		}
		sql, args, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE (("name" < $1) OR ("name" = $1 AND "id" > $2)) AND TRUE`))
		Expect(args).To(Equal([]any{"Alice", "uuid-1"}))
	})

	It("numbers placeholders after existing args", func() {
		r := &pgxquery.CursorRewriter{
			Fields:  []ordering.Field{{Path: "id"}},
			Values:  []any{"x"},
			Columns: map[string]string{"id": "id"},
		}
		sql, _, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, []any{int32(10), int32(0)})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`"id" > $3`))
	})

	It("removes the sentinel when no cursor is supplied", func() {
		r := &pgxquery.CursorRewriter{}
		sql, args, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, []any{"keep"})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`WHERE  TRUE`))
		Expect(args).To(Equal([]any{"keep"}))
	})

	It("errors on length mismatch between Fields and Values", func() {
		r := &pgxquery.CursorRewriter{
			Fields:  []ordering.Field{{Path: "id"}},
			Values:  []any{"x", "y"},
			Columns: map[string]string{"id": "id"},
		}
		_, _, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, nil)
		Expect(err).To(MatchError(ContainSubstring("cursor: expected 1 values for 1 fields, got 2")))
	})

	It("fails closed when a cursor field is not in Columns", func() {
		r := &pgxquery.CursorRewriter{
			Fields:  []ordering.Field{{Path: "name"}},
			Values:  []any{"Alice"},
			Columns: map[string]string{"other": "other"},
		}
		_, _, err := r.Substitute(`WHERE /* query.cursor AND */ TRUE`, nil)
		Expect(err).To(MatchError(ContainSubstring(`cursor: unknown field "name"`)))
	})
})
