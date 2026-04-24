package pgxquery

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"
)

var _ = Describe("transpiler", func() {
	var t *transpiler

	BeforeEach(func() {
		t = &transpiler{}
	})

	Describe("transpile", func() {
		It("errors on an unsupported expression kind", func() {
			// ListExpr is not supported by the transpiler.
			expr := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
			}
			_, err := t.transpile(expr)
			Expect(err).To(MatchError(ContainSubstring("unsupported expression kind")))
		})

		It("errors when a SelectExpr operand is not an identifier chain", func() {
			expr := &exprpb.Expr{
				ExprKind: &exprpb.Expr_SelectExpr{
					SelectExpr: &exprpb.Expr_Select{
						Operand: &exprpb.Expr{
							ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
						},
						Field: "x",
					},
				},
			}
			_, err := t.transpile(expr)
			Expect(err).To(MatchError(ContainSubstring("unsupported identifier expression")))
		})

		It("resolves an Ident through the column map", func() {
			t.columns = map[string]string{"name": "name"}
			out, err := t.transpile(&exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{IdentExpr: &exprpb.Expr_Ident{Name: "name"}},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal(`"name"`))
		})

		It("resolves a dotted Select path through the column map", func() {
			t.columns = map[string]string{"address.city": "addr.city"}
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

		It("errors when an identifier is not in the column map", func() {
			out, err := t.transpile(&exprpb.Expr{
				ExprKind: &exprpb.Expr_IdentExpr{IdentExpr: &exprpb.Expr_Ident{Name: "nope"}},
			})
			Expect(err).To(MatchError(ContainSubstring(`unknown field "nope"`)))
			Expect(out).To(BeEmpty())
		})
	})

	Describe("transpileConst", func() {
		It("appends a uint64 value as a bound argument", func() {
			out, err := t.transpileConst(&exprpb.Constant{
				ConstantKind: &exprpb.Constant_Uint64Value{Uint64Value: 42},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(out).To(Equal("$1"))
			Expect(t.args).To(Equal([]any{uint64(42)}))
		})

		It("errors on an unsupported constant kind", func() {
			_, err := t.transpileConst(&exprpb.Constant{
				ConstantKind: &exprpb.Constant_NullValue{},
			})
			Expect(err).To(MatchError(ContainSubstring("unsupported constant kind")))
		})
	})

	Describe("transpileCall", func() {
		It("errors on an unsupported function", func() {
			_, err := t.transpileCall(&exprpb.Expr_Call{Function: "unknown"})
			Expect(err).To(MatchError(ContainSubstring("unsupported function")))
		})

		It("propagates errors from the LHS of a binary op", func() {
			badArg := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
			}
			okArg := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ConstExpr{
					ConstExpr: &exprpb.Constant{
						ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 1},
					},
				},
			}
			_, err := t.transpileCall(&exprpb.Expr_Call{
				Function: "=",
				Args:     []*exprpb.Expr{badArg, okArg},
			})
			Expect(err).To(HaveOccurred())
		})

		It("propagates errors from the RHS of a binary op", func() {
			okArg := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ConstExpr{
					ConstExpr: &exprpb.Constant{
						ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 1},
					},
				},
			}
			badArg := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
			}
			_, err := t.transpileCall(&exprpb.Expr_Call{
				Function: "=",
				Args:     []*exprpb.Expr{okArg, badArg},
			})
			Expect(err).To(HaveOccurred())
		})

		It("propagates errors from AND/OR/HAS operand failures", func() {
			bad := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ListExpr{ListExpr: &exprpb.Expr_CreateList{}},
			}
			ok := &exprpb.Expr{
				ExprKind: &exprpb.Expr_ConstExpr{
					ConstExpr: &exprpb.Constant{
						ConstantKind: &exprpb.Constant_Int64Value{Int64Value: 1},
					},
				},
			}
			for _, fn := range []string{"AND", "OR", ":"} {
				_, err := t.transpileCall(&exprpb.Expr_Call{
					Function: fn,
					Args:     []*exprpb.Expr{bad, ok},
				})
				Expect(err).To(HaveOccurred(), "expected LHS error for %s", fn)
				_, err = t.transpileCall(&exprpb.Expr_Call{
					Function: fn,
					Args:     []*exprpb.Expr{ok, bad},
				})
				Expect(err).To(HaveOccurred(), "expected RHS error for %s", fn)
			}
			_, err := t.transpileCall(&exprpb.Expr_Call{
				Function: "NOT",
				Args:     []*exprpb.Expr{bad},
			})
			Expect(err).To(HaveOccurred())
		})
	})
})
