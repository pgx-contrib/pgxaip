package pgxaip_test

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxaip"
	"go.einride.tech/aip/ordering"
)

var _ = Describe("OrderByRewriter", func() {
	var ctx context.Context

	BeforeEach(func() {
		ctx = context.Background()
	})

	It("substitutes with a single ASC field", func() {
		r := &pgxaip.OrderByRewriter{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"name": "name"},
		}
		sql, args, err := r.RewriteQuery(ctx, nil,
			`ORDER BY /* query.order , */ id`, []any{r, int32(10)})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY "name" ASC , id`))
		Expect(args).To(Equal([]any{int32(10)}))
	})

	It("substitutes with mixed directions and multiple fields", func() {
		r := &pgxaip.OrderByRewriter{
			OrderBy: parseOrderBy("name desc, age asc"),
			Columns: map[string]string{"name": "name", "age": "age"},
		}
		sql, _, err := r.RewriteQuery(ctx, nil,
			`ORDER BY /* query.order , */ id`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY "name" DESC, "age" ASC , id`))
	})

	It("maps AIP paths to their backing DB columns", func() {
		r := &pgxaip.OrderByRewriter{
			OrderBy: parseOrderBy("create_time desc"),
			Columns: map[string]string{"create_time": "created_at"},
		}
		sql, _, err := r.RewriteQuery(ctx, nil,
			`ORDER BY /* query.order , */ id`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY "created_at" DESC , id`))
	})

	It("sanitizes dotted paths on the mapped column", func() {
		r := &pgxaip.OrderByRewriter{
			OrderBy: parseOrderBy("address.city"),
			Columns: map[string]string{"address.city": "addr.city"},
		}
		sql, _, err := r.RewriteQuery(ctx, nil,
			`ORDER BY /* query.order , */ id`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(ContainSubstring(`"addr"."city" ASC`))
	})

	It("removes the sentinel when no fields are supplied", func() {
		r := &pgxaip.OrderByRewriter{OrderBy: ordering.OrderBy{}}
		sql, _, err := r.RewriteQuery(ctx, nil,
			`ORDER BY /* query.order , */ id`, []any{r})
		Expect(err).NotTo(HaveOccurred())
		Expect(sql).To(Equal(`ORDER BY  id`))
	})

	It("fails closed when an order field is not in Columns", func() {
		r := &pgxaip.OrderByRewriter{
			OrderBy: parseOrderBy("name"),
			Columns: map[string]string{"other": "other"},
		}
		_, _, err := r.RewriteQuery(ctx, nil,
			`ORDER BY /* query.order , */ id`, []any{r})
		Expect(err).To(MatchError(ContainSubstring(`order: unknown field "name"`)))
	})
})
