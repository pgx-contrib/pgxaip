package pgxaip

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var _ = Describe("transpiler internals", func() {
	Describe("transpile", func() {
		It("errors on an unsupported expression kind", func() {
			t := &transpiler{startParam: 1}
			_, err := t.transpile(&exprpb.Expr{
				ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
			})
			Expect(err).To(MatchError(ContainSubstring("unsupported expression kind")))
		})

		It("errors when a Select operand is not an identifier chain", func() {
			t := &transpiler{startParam: 1}
			_, err := t.transpile(&exprpb.Expr{
				ExprKind: &exprpb.Expr_SelectExpr{
					SelectExpr: &exprpb.Expr_Select{
						Operand: &exprpb.Expr{
							ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
						},
						Field: "x",
					},
				},
			})
			Expect(err).To(MatchError(ContainSubstring("unsupported identifier expression")))
		})

		It("resolves a dotted Select path through the column map", func() {
			t := &transpiler{
				columns:    map[string]string{"address.city": "addr.city"},
				startParam: 1,
			}
			out, err := t.transpile(&exprpb.Expr{
				ExprKind: &exprpb.Expr_SelectExpr{
					SelectExpr: &exprpb.Expr_Select{
						Operand: &exprpb.Expr{
							ExprKind: &exprpb.Expr_IdentExpr{IdentExpr: &exprpb.Expr_Ident{Name: "address"}},
						},
						Field: "city",
					},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal(`"addr"."city"`))
		})
	})

	Describe("transpileConst", func() {
		It("binds a uint64 value", func() {
			t := &transpiler{startParam: 1}
			out, err := t.transpileConst(&exprpb.Constant{
				ConstantKind: &exprpb.Constant_Uint64Value{Uint64Value: 42},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal("$1"))
			Expect(t.args).To(Equal([]any{uint64(42)}))
		})

		It("respects startParam for the first placeholder", func() {
			t := &transpiler{startParam: 5}
			out, err := t.transpileConst(&exprpb.Constant{
				ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 1},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal("$5"))
		})

		It("errors on an unsupported constant kind", func() {
			t := &transpiler{startParam: 1}
			_, err := t.transpileConst(&exprpb.Constant{
				ConstantKind: &exprpb.Constant_NullValue{},
			})
			Expect(err).To(MatchError(ContainSubstring("unsupported constant kind")))
		})
	})

	Describe("transpileCall", func() {
		It("errors on an unsupported function", func() {
			t := &transpiler{startParam: 1}
			_, err := t.transpileCall(&exprpb.Expr_Call{Function: "unknown"})
			Expect(err).To(MatchError(ContainSubstring("unsupported function")))
		})

		It("rejects unary minus on a non-literal operand", func() {
			t := &transpiler{startParam: 1, columns: map[string]string{"age": "age"}}
			ident := &exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{IdentExpr: &exprpb.Expr_Ident{Name: "age"}},
			}
			_, err := t.transpileCall(&exprpb.Expr_Call{Function: "-_", Args: []*exprpb.Expr{ident}})
			Expect(err).To(MatchError(ContainSubstring("unary minus requires a numeric literal")))
		})

		It("rejects timestamp() without a string literal argument", func() {
			t := &transpiler{startParam: 1}
			_, err := t.transpileCall(&exprpb.Expr_Call{
				Function: "timestamp",
				Args: []*exprpb.Expr{{
					ExprKind: &exprpb.Expr_ConstExpr{
						ConstExpr: &exprpb.Constant{
							ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 1},
						},
					},
				}},
			})
			Expect(err).To(MatchError(ContainSubstring("argument must be a string literal")))
		})

		It("rejects timestamp() with an unparseable string", func() {
			t := &transpiler{startParam: 1}
			_, err := t.transpileCall(&exprpb.Expr_Call{
				Function: "timestamp",
				Args: []*exprpb.Expr{{
					ExprKind: &exprpb.Expr_ConstExpr{
						ConstExpr: &exprpb.Constant{
							ConstantKind: &exprpb.Constant_StringValue{StringValue: "not-a-date"},
						},
					},
				}},
			})
			Expect(err).To(MatchError(ContainSubstring("timestamp")))
		})
	})
})
