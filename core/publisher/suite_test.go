package publisher_test

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"testing"
)

func TestBrokeryIntegration(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "expedit integration tests")
}
