package pgxquery_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxquery"
	"go.einride.tech/aip/ordering"
)

var _ = Describe("OrderByRewriter", func() {
	It("substitutes with a single ASC field", func() {
		r := &pgxquery.OrderByRewriter{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"name": "name"},
		}
		sql, args, err := r.Substitute(`ORDER BY /* query.order , */ id`, []any{int32(10)})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY "name" ASC , id`))
		Expect(args).To(Equal([]any{int32(10)}))
	})

	It("substitutes with mixed directions and multiple fields", func() {
		r := &pgxquery.OrderByRewriter{
			OrderBy: parseOrderBy("name desc, age asc"),
			Columns: map[string]string{"name": "name", "age": "age"},
		}
		sql, _, err := r.Substitute(`ORDER BY /* query.order , */ id`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY "name" DESC, "age" ASC , id`))
	})

	It("maps AIP paths to their backing DB columns", func() {
		r := &pgxquery.OrderByRewriter{
			OrderBy: parseOrderBy("create_time desc"),
			Columns: map[string]string{"create_time": "created_at"},
		}
		sql, _, err := r.Substitute(`ORDER BY /* query.order , */ id`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY "created_at" DESC , id`))
	})

	It("sanitizes dotted paths on the mapped column", func() {
		r := &pgxquery.OrderByRewriter{
			OrderBy: parseOrderBy("address.city"),
			Columns: map[string]string{"address.city": "addr.city"},
		}
		sql, _, err := r.Substitute(`ORDER BY /* query.order , */ id`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`"addr"."city" ASC`))
	})

	It("removes the sentinel when no fields are supplied", func() {
		r := &pgxquery.OrderByRewriter{OrderBy: ordering.OrderBy{}}
		sql, _, err := r.Substitute(`ORDER BY /* query.order , */ id`, nil)
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY  id`))
	})

	It("fails closed when an order field is not in Columns", func() {
		r := &pgxquery.OrderByRewriter{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"other": "other"},
		}
		_, _, err := r.Substitute(`ORDER BY /* query.order , */ id`, nil)
		Expect(err).To(MatchError(ContainSubstring(`order: unknown field "name"`)))
	})
})
