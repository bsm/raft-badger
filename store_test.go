package raftbadger

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/hashicorp/raft"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("LogStore", func() {
	var subject raft.LogStore
	var dir string

	BeforeEach(func() {
		var err error

		dir, err = ioutil.TempDir("", "raftbadger")
		Expect(err).NotTo(HaveOccurred())

		subject, err = NewLogStore(dir, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dir)).To(Succeed())
	})

	It("should get FirstIndex", func() {
		idx, err := subject.FirstIndex()
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(uint64(0)))

		logs := []*raft.Log{
			testRaftLog(1, "log1"),
			testRaftLog(2, "log2"),
			testRaftLog(3, "log3"),
		}
		Expect(subject.StoreLogs(logs)).To(Succeed())

		idx, err = subject.FirstIndex()
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(uint64(1)))
	})

	It("should get LastIndex", func() {
		idx, err := subject.LastIndex()
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(uint64(0)))

		logs := []*raft.Log{
			testRaftLog(1, "log1"),
			testRaftLog(2, "log2"),
			testRaftLog(3, "log3"),
		}
		Expect(subject.StoreLogs(logs)).To(Succeed())

		idx, err = subject.LastIndex()
		Expect(err).NotTo(HaveOccurred())
		Expect(idx).To(Equal(uint64(3)))
	})

	It("should StoreLogs/GetLog", func() {
		log := new(raft.Log)
		Expect(subject.GetLog(1, log)).To(Equal(raft.ErrLogNotFound))

		logs := []*raft.Log{
			testRaftLog(1, "log1"),
			testRaftLog(2, "log2"),
			testRaftLog(3, "log3"),
		}
		Expect(subject.StoreLogs(logs)).To(Succeed())

		Expect(subject.GetLog(2, log)).To(Succeed())
		Expect(log).To(Equal(logs[1]))
	})

	It("should StoreLog", func() {
		log := new(raft.Log)

		err := subject.GetLog(1, log)
		Expect(err).To(Equal(raft.ErrLogNotFound))

		fix := testRaftLog(1, "log1")
		Expect(subject.StoreLog(fix))

		Expect(subject.GetLog(1, log)).To(Succeed())
		Expect(log).To(Equal(fix))
	})

	It("should DeleteRange", func() {
		log := new(raft.Log)
		logs := []*raft.Log{
			testRaftLog(1, "log1"),
			testRaftLog(2, "log2"),
			testRaftLog(3, "log3"),
		}
		Expect(subject.StoreLogs(logs)).To(Succeed())

		Expect(subject.DeleteRange(1, 2)).To(Succeed())
		Expect(subject.GetLog(1, log)).To(Equal(raft.ErrLogNotFound))
		Expect(subject.GetLog(2, log)).To(Equal(raft.ErrLogNotFound))
		Expect(subject.GetLog(3, log)).To(Succeed())
		Expect(log).To(Equal(logs[2]))
	})

})

var _ = Describe("StableStore", func() {
	var subject raft.StableStore
	var dir string

	BeforeEach(func() {
		var err error

		dir, err = ioutil.TempDir("", "raftbadger")
		Expect(err).NotTo(HaveOccurred())

		subject, err = NewStableStore(dir, nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(os.RemoveAll(dir)).To(Succeed())
	})

	It("should Get/Set", func() {
		_, err := subject.Get([]byte("bad"))
		Expect(err).To(Equal(ErrKeyNotFound))

		k, v := []byte("hello"), []byte("world")
		Expect(subject.Set(k, v)).To(Succeed())

		val, err := subject.Get(k)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(v))
	})

	It("should GetUint64/SetUint64", func() {
		_, err := subject.GetUint64([]byte("bad"))
		Expect(err).To(Equal(ErrKeyNotFound))

		k, v := []byte("abc"), uint64(123)
		Expect(subject.SetUint64(k, v)).To(Succeed())

		val, err := subject.GetUint64(k)
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal(v))
	})
})

// --------------------------------------------------------------------

func testRaftLog(idx uint64, data string) *raft.Log {
	return &raft.Log{
		Data:  []byte(data),
		Index: idx,
	}
}

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "raft-badger")
}
