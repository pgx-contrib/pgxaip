package pgxaip_test

import (
	"testing"

	"github.com/google/cel-go/cel"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	aip "github.com/protoc-contrib/aip-go"
)

func TestPgxquery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxaip Suite")
}

// parseFilter compiles a CEL expression the way the generated ParseFilter
// does, against an environment declaring vars.
func parseFilter(expr string, vars ...cel.EnvOption) *cel.Ast {
	GinkgoHelper()
	env, err := cel.NewEnv(vars...)
	Expect(err).NotTo(HaveOccurred())
	ast, issues := env.Compile(expr)
	Expect(issues.Err()).NotTo(HaveOccurred())
	return ast
}

func parseOrderBy(s string) aip.OrderBy {
	GinkgoHelper()
	var o aip.OrderBy
	Expect(o.UnmarshalString(s)).To(Succeed())
	return o
}
