package pgxaip_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
)

var _ = Describe("Query.Rewrite order_by", func() {
	It("emits a single ASC term", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"name": "name"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"name" ASC`))
	})

	It("emits mixed directions and multiple fields", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("name desc, age asc"),
			Columns: map[string]string{"name": "name", "age": "age"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"name" DESC, "age" ASC`))
	})

	It("maps AIP paths to their backing DB columns", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("create_time desc"),
			Columns: map[string]string{"create_time": "created_at"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"created_at" DESC`))
	})

	It("sanitizes dotted paths on the mapped column", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("address.city"),
			Columns: map[string]string{"address.city": "addr.city"},
		}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(Equal(`"addr"."city" ASC`))
	})

	It("returns empty order when no fields are supplied", func() {
		q := pgxaip.Query{}
		_, order, _, err := q.Rewrite()
		Expect(err).NotTo(HaveOccurred())
		Expect(order).To(BeEmpty())
	})

	It("fails closed when an order field is not in Columns", func() {
		q := pgxaip.Query{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"other": "other"},
		}
		_, _, _, err := q.Rewrite()
		Expect(err).To(MatchError(ContainSubstring(`order: unknown field "name"`)))
	})
})
