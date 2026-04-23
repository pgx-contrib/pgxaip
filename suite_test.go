package pgxquery_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgxquery(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxquery Suite")
}
