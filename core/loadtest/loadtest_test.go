package loadtest_test

import (
	"fmt"
	"sync"
	"testing"

	. "github.com/onsi/gomega"
	"github.com/quantumcycle/expedit/core/loadtest"
)

func TestRecorder(t *testing.T) {
	t.Run("should return the recorded IDs per key in recording order", func(t *testing.T) {
		g := NewGomegaWithT(t)
		r := loadtest.NewRecorder()
		r.Record("a", "1")
		r.Record("b", "2")
		r.Record("a", "3")

		g.Expect(r.IDs("a")).To(Equal([]string{"1", "3"}))
		g.Expect(r.IDs("b")).To(Equal([]string{"2"}))
		g.Expect(r.Count("a")).To(Equal(2))
		g.Expect(r.IDs("unknown")).To(BeEmpty())
	})

	t.Run("should return a copy not affected by later records", func(t *testing.T) {
		g := NewGomegaWithT(t)
		r := loadtest.NewRecorder()
		r.Record("a", "1")
		ids := r.IDs("a")
		r.Record("a", "2")

		g.Expect(ids).To(Equal([]string{"1"}))
	})

	t.Run("should be safe for concurrent use", func(t *testing.T) {
		g := NewGomegaWithT(t)
		r := loadtest.NewRecorder()
		var wg sync.WaitGroup
		for i := range 50 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				r.Record("a", fmt.Sprint(i))
				_ = r.IDs("a")
			}()
		}
		wg.Wait()

		g.Expect(r.Count("a")).To(Equal(50))
	})
}

func TestMissing(t *testing.T) {
	t.Run("should return the expected IDs that are not in actual", func(t *testing.T) {
		g := NewGomegaWithT(t)
		g.Expect(loadtest.Missing([]string{"1", "2", "3"}, []string{"3", "1"})).To(Equal([]string{"2"}))
		g.Expect(loadtest.Missing([]string{"1"}, []string{"1"})).To(BeEmpty())
	})
}

func TestDuplicates(t *testing.T) {
	t.Run("should return the IDs present in both lists", func(t *testing.T) {
		g := NewGomegaWithT(t)
		g.Expect(loadtest.Duplicates([]string{"1", "2"}, []string{"2", "3"})).To(Equal([]string{"2"}))
		g.Expect(loadtest.Duplicates([]string{"1"}, []string{"2"})).To(BeEmpty())
	})
}

func TestRandomInt(t *testing.T) {
	t.Run("should stay within the inclusive bounds", func(t *testing.T) {
		g := NewGomegaWithT(t)
		for range 1000 {
			g.Expect(loadtest.RandomInt(1, 3)).To(BeNumerically(">=", 1))
			g.Expect(loadtest.RandomInt(1, 3)).To(BeNumerically("<=", 3))
		}
	})
}
