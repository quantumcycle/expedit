package google

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Publisher", func() {
	It("should return an error if the routing function is missing", func() {
		_, err := NewGooglePublisher(nil, PublisherOption{})
		Expect(err).To(HaveOccurred())
		Expect(err).To(MatchError("routing function is required"))
	})
})
