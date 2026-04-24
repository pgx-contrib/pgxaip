package pgxaip_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.einride.tech/aip/filtering"
	"go.einride.tech/aip/ordering"
)

func TestPgxquery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxaip Suite")
}

func parseFilter(expr string, opts ...filtering.DeclarationOption) filtering.Filter {
	GinkgoHelper()
	base := []filtering.DeclarationOption{filtering.DeclareStandardFunctions()}
	decls, err := filtering.NewDeclarations(append(base, opts...)...)
	Expect(err).NotTo(HaveOccurred())
	f, err := filtering.ParseFilterString(expr, decls)
	Expect(err).NotTo(HaveOccurred())
	return f
}

func parseOrderBy(s string) ordering.OrderBy {
	GinkgoHelper()
	var o ordering.OrderBy
	Expect(o.UnmarshalString(s)).To(Succeed())
	return o
}
