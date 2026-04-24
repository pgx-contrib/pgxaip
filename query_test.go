package pgxaip_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/pagination"
)

var _ = Describe("Query.Rewrite", func() {
	It("returns all zeroes for an empty query", func() {
		q := pgxaip.Query{}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(BeEmpty())
		Expect(order).To(BeEmpty())
		Expect(args).To(BeEmpty())
	})

	It("combines filter + order + cursor with correct placeholder numbering", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice"`,
				filtering.DeclareIdent("name", filtering.TypeString)),
			OrderBy:   parseOrderBy("name desc, id"),
			PageToken: pagination.PageToken{Cursor: []any{"Bob", "uuid-7"}},
			Columns:   map[string]string{"name": "name", "id": "id"},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`("name" = $1) AND (("name" < $2) OR ("name" = $2 AND "id" > $3))`))
		Expect(order).To(Equal(`"name" DESC, "id" ASC`))
		Expect(args).To(Equal([]any{"Alice", "Bob", "uuid-7"}))
	})

	It("ignores PageToken.Offset", func() {
		q := pgxaip.Query{
			OrderBy:   parseOrderBy("id"),
			PageToken: pagination.PageToken{Offset: 42},
			Columns:   map[string]string{"id": "id"},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(BeEmpty())
		Expect(order).To(Equal(`"id" ASC`))
		Expect(args).To(BeEmpty())
	})

	It("uses a single Columns map for both filter and order", func() {
		q := pgxaip.Query{
			Filter: parseFilter(`name = "Alice"`,
				filtering.DeclareIdent("name", filtering.TypeString)),
			OrderBy: parseOrderBy("create_time desc"),
			Columns: map[string]string{
				"name":        "name",
				"create_time": "created_at",
			},
		}
		where, order, args, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(where).To(Equal(`"name" = $1`))
		Expect(order).To(Equal(`"created_at" DESC`))
		Expect(args).To(Equal([]any{"Alice"}))
	})
})
