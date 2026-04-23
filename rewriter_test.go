package pgxquery_test

import (
	"context"

	"github.com/jackc/pgx/v5"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/pgx-contrib/pgxquery"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

func parseFilter(expr string, opts ...filtering.DeclarationOption) filtering.Filter {
	GinkgoHelper()
	base := []filtering.DeclarationOption{filtering.DeclareStandardFunctions()}
	decls, err := filtering.NewDeclarations(append(base, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	filter, err := filtering.ParseFilterString(expr, decls)
	Expect(err).NotTo(HaveOccurred())
	return filter
}

func parseOrderBy(s string) ordering.OrderBy {
	GinkgoHelper()
	var o ordering.OrderBy
	Expect(o.UnmarshalString(s)).To(Succeed())
	return o
}

var _ = Describe("QueryRewriter", func() {
	var (
		ctx   context.Context
		query string
	)

	BeforeEach(func() {
		ctx = context.Background()
		query = "SELECT * FROM customer"
	})

	Describe("no-op", func() {
		It("passes the query through when both filter and order by are empty", func() {
			r := &pgxquery.QueryRewriter{}
			sql, args, err := r.RewriteQuery(ctx, nil, query, []any{"existing"})
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(Equal(query))
			Expect(args).To(Equal([]any{"existing"}))
		})

		It("passes through when filter has no expression", func() {
			r := &pgxquery.QueryRewriter{
				Filter: filtering.Filter{CheckedExpr: &exprpb.CheckedExpr{}},
			}
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(Equal(query))
		})
	})

	Describe("filter", func() {
		It("rewrites a simple equality", func() {
			r := pgxquery.New(
				parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
				ordering.OrderBy{},
			)
			sql, args, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(Equal(`WITH query AS (SELECT * FROM customer) SELECT * FROM query WHERE "name" = $1`))
			Expect(args).To(Equal([]any{"Alice"}))
		})

		DescribeTable("comparison operators",
			func(expr, op string) {
				r := pgxquery.New(
					parseFilter(expr, filtering.DeclareIdent("age", filtering.TypeInt)),
					ordering.OrderBy{},
				)
				sql, args, err := r.RewriteQuery(ctx, nil, query, nil)
				Expect(err).NotTo(HaveOccurred())
				Expect(sql).To(ContainSubstring(`"age" ` + op + ` $1`))
				Expect(args).To(Equal([]any{int64(18)}))
			},
			Entry("equals", `age = 18`, "="),
			Entry("not equals", `age != 18`, "!="),
			Entry("greater than", `age > 18`, ">"),
			Entry("greater or equal", `age >= 18`, ">="),
			Entry("less than", `age < 18`, "<"),
			Entry("less or equal", `age <= 18`, "<="),
		)

		It("combines expressions with AND", func() {
			r := pgxquery.New(
				parseFilter(`name = "Alice" AND age > 18`,
					filtering.DeclareIdent("name", filtering.TypeString),
					filtering.DeclareIdent("age", filtering.TypeInt),
				),
				ordering.OrderBy{},
			)
			sql, args, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`("name" = $1 AND "age" > $2)`))
			Expect(args).To(Equal([]any{"Alice", int64(18)}))
		})

		It("combines expressions with OR", func() {
			r := pgxquery.New(
				parseFilter(`name = "Alice" OR name = "Bob"`,
					filtering.DeclareIdent("name", filtering.TypeString),
				),
				ordering.OrderBy{},
			)
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`("name" = $1 OR "name" = $2)`))
		})

		It("supports NOT", func() {
			r := pgxquery.New(
				parseFilter(`NOT (name = "Alice")`,
					filtering.DeclareIdent("name", filtering.TypeString),
				),
				ordering.OrderBy{},
			)
			sql, args, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`(NOT "name" = $1)`))
			Expect(args).To(Equal([]any{"Alice"}))
		})

		It("supports HAS via ILIKE", func() {
			r := pgxquery.New(
				parseFilter(`name:"li"`,
					filtering.DeclareIdent("name", filtering.TypeString),
				),
				ordering.OrderBy{},
			)
			sql, args, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`"name" ILIKE '%' || $1 || '%'`))
			Expect(args).To(Equal([]any{"li"}))
		})

		It("supports nested field selection", func() {
			decls, err := filtering.NewDeclarations(
				filtering.DeclareStandardFunctions(),
				filtering.DeclareIdent("address", filtering.TypeMap(filtering.TypeString, filtering.TypeString)),
			)
			Expect(err).NotTo(HaveOccurred())
			filter, err := filtering.ParseFilterString(`address.city = "Boston"`, decls)
			Expect(err).NotTo(HaveOccurred())

			r := pgxquery.New(filter, ordering.OrderBy{})
			sql, args, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`"address"."city" = $1`))
			Expect(args).To(Equal([]any{"Boston"}))
		})

		It("supports float constants", func() {
			r := pgxquery.New(
				parseFilter(`price > 9.99`,
					filtering.DeclareIdent("price", filtering.TypeFloat),
				),
				ordering.OrderBy{},
			)
			_, args, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(args).To(Equal([]any{9.99}))
		})

		It("preserves existing args and assigns new placeholders after them", func() {
			r := pgxquery.New(
				parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
				ordering.OrderBy{},
			)
			sql, args, err := r.RewriteQuery(ctx, nil, query, []any{"existing"})
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`"name" = $2`))
			Expect(args).To(Equal([]any{"existing", "Alice"}))
		})

		It("sanitizes identifiers with quotes", func() {
			r := pgxquery.New(
				parseFilter(`name = "Alice"`, filtering.DeclareIdent("name", filtering.TypeString)),
				ordering.OrderBy{},
			)
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`"name"`))
		})
	})

	Describe("ordering", func() {
		It("appends ORDER BY for a single ascending field", func() {
			r := pgxquery.New(filtering.Filter{}, parseOrderBy("age"))
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(Equal(`WITH query AS (SELECT * FROM customer) SELECT * FROM query ORDER BY "age" ASC`))
		})

		It("appends ORDER BY for a single descending field", func() {
			r := pgxquery.New(filtering.Filter{}, parseOrderBy("age desc"))
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`ORDER BY "age" DESC`))
		})

		It("joins multiple fields with commas in declaration order", func() {
			r := pgxquery.New(filtering.Filter{}, parseOrderBy("age desc, name"))
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`ORDER BY "age" DESC, "name" ASC`))
		})

		It("sanitizes dotted paths", func() {
			r := pgxquery.New(filtering.Filter{}, parseOrderBy("address.city"))
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(ContainSubstring(`ORDER BY "address"."city" ASC`))
		})
	})

	Describe("filter and ordering combined", func() {
		It("emits WHERE before ORDER BY", func() {
			r := pgxquery.New(
				parseFilter(`age > 18`, filtering.DeclareIdent("age", filtering.TypeInt)),
				parseOrderBy("age desc"),
			)
			sql, _, err := r.RewriteQuery(ctx, nil, query, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(sql).To(Equal(
				`WITH query AS (SELECT * FROM customer) SELECT * FROM query WHERE "age" > $1 ORDER BY "age" DESC`,
			))
		})
	})

	Describe("interface compliance", func() {
		It("satisfies pgx.QueryRewriter", func() {
			var _ pgx.QueryRewriter = &pgxquery.QueryRewriter{}
		})
	})
})
