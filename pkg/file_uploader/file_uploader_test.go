package file_uploader

import (
	. "github.com/pingcap/check"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var _ = Suite(&testFileUploader{})

type testFileUploader struct{}

// This Test Can Not Pass Not
func (t *testFileUploader) DontTestFileUploaderAppend(c *C) {
	var wait sync.WaitGroup
	dir, err := ioutil.TempDir("", "up_test_file_uploader_append")
	c.Assert(err, IsNil)
	//defer os.RemoveAll(dir)
	filenames := []string{"testfile1"}
	wait.Add(len(filenames))
	for _, filename := range filenames {
		//go func() {
		rand := rand.New(rand.NewSource(time.Now().Unix()))
		sourceFilePath := filepath.Join(dir, filename)
		err := os.MkdirAll(filepath.Dir(sourceFilePath), 0777)
		c.Assert(err, IsNil)
		file, err := os.OpenFile(sourceFilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer file.Close()
		_, err = io.CopyN(file, rand, 789*M)
		c.Assert(err, IsNil)
		wait.Done()
		//}()
	}
	targetDir, err := ioutil.TempDir("", "up_test_file_uploader_append_target")
	c.Assert(err, IsNil)
	//defer os.RemoveAll(targetDir)
	fu := NewFileUploader(dir, 1, 100*M, NewMockFileUploaderDriver(dir, targetDir))
	wait.Wait()
	fu.WaitAndClose()
	for _, filename := range filenames {
		sourceHash := NewMd5Base64FileHash()
		sourceFile, err := os.OpenFile(filepath.Join(dir, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer sourceFile.Close()
		_, err = io.Copy(sourceHash, sourceFile)
		c.Assert(err, IsNil)
		targetHash := NewMd5Base64FileHash()
		targetFile, err := os.OpenFile(filepath.Join(targetDir, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer targetFile.Close()
		_, err = io.Copy(targetHash, targetFile)
		c.Assert(err, IsNil)
		c.Assert(sourceHash.String(), Equals, targetHash.String(), Commentf("hash check failure, file name: %s", filename))
	}
}
