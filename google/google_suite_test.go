package google

import (
	"github.com/joho/godotenv"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func Test(t *testing.T) {
	err := godotenv.Load(".env")
	if err != nil {
		panic("cannot parse .env for tests")
	}

	RegisterFailHandler(Fail)
	RunSpecs(t, "Google publisher - suite")
}
