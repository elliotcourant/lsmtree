package lsmtree

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	code := m.Run()
	os.Exit(code)
}

func NewTempDirectory(t assert.TestingT) (string, func()) {
	dir, err := ioutil.TempDir("", "lsmtree-test")
	if !assert.NoError(t, err) {
		panic(err)
	}
	if !assert.NotEmpty(t, dir) {
		panic("temp directory path is blank")
	}
	return dir, func() {
		os.RemoveAll(dir)
	}
}
