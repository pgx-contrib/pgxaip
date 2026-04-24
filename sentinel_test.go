package pgxaip

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("sentinel", func() {
	Describe("replaceSentinel", func() {
		It("replaces with pre-glue only", func() {
			got := replaceSentinel(`/* AND query.filter */ x`, "filter", "EXPR")
			Expect(got).To(Equal(`AND EXPR x`))
		})

		It("replaces with post-glue only", func() {
			got := replaceSentinel(`/* query.filter AND */ x`, "filter", "EXPR")
			Expect(got).To(Equal(`EXPR AND x`))
		})

		It("replaces with pre and post glue", func() {
			got := replaceSentinel(`/* ( query.filter ) */`, "filter", "EXPR")
			Expect(got).To(Equal(`( EXPR )`))
		})

		It("replaces with no glue", func() {
			got := replaceSentinel(`/* query.filter */`, "filter", "EXPR")
			Expect(got).To(Equal(`EXPR`))
		})

		It("removes the comment when fragment is empty", func() {
			got := replaceSentinel(`WHERE /* query.filter AND */ TRUE`, "filter", "")
			Expect(got).To(Equal(`WHERE  TRUE`))
		})

		It("does not match sentinels with a different name", func() {
			got := replaceSentinel(`/* query.order , */`, "filter", "EXPR")
			Expect(got).To(Equal(`/* query.order , */`))
		})

		It("does not match identifiers that share a name prefix", func() {
			got := replaceSentinel(`/* query.filterX */`, "filter", "EXPR")
			Expect(got).To(Equal(`/* query.filterX */`))
		})

		It("replaces every occurrence", func() {
			got := replaceSentinel(`/* query.filter */ AND /* query.filter */`, "filter", "E")
			Expect(got).To(Equal(`E AND E`))
		})
	})

	Describe("sanitizePath", func() {
		It("quotes a simple column name", func() {
			Expect(sanitizePath("name")).To(Equal(`"name"`))
		})

		It("quotes each segment of a dotted path separately", func() {
			Expect(sanitizePath("address.city")).To(Equal(`"address"."city"`))
		})
	})
})
