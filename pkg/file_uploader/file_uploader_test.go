package file_uploader

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
)

var _ = Suite(&testFileUploader{})

type testFileUploader struct{}

func TestSuite(t *testing.T) {
	TestingT(t)
}

func (t *testFileUploader) TestFileUploaderAppend(c *C) {
	checkPointRunning.Set(0)
	var wait sync.WaitGroup
	dir, err := ioutil.TempDir("", "up_test_file_uploader_append")
	c.Assert(err, IsNil)
	defer os.RemoveAll(dir)
	filenames := []string{"testfile1", "testfile2", "testdir/testfile1"}
	wait.Add(len(filenames))
	for _, filename := range filenames {
		writeFile := func(filename string) {
			rand := rand.New(rand.NewSource(time.Now().Unix()))
			sourceFilePath := filepath.Join(dir, filename)
			err := os.MkdirAll(filepath.Dir(sourceFilePath), 0777)
			c.Assert(err, IsNil)
			file, err := os.OpenFile(sourceFilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
			c.Assert(err, IsNil)
			defer file.Close()
			_, err = io.CopyN(file, rand, 123*M)
			c.Assert(err, IsNil)
			wait.Done()
		}
		go writeFile(filename)
		time.Sleep(1 * time.Second)
	}
	targetDir, err := ioutil.TempDir("", "up_test_file_uploader_append_target")
	c.Assert(err, IsNil)
	defer os.RemoveAll(targetDir)
	fu := NewFileUploader(dir, 8, 10*M, NewMockFileUploaderDriver(dir, targetDir))
	wait.Wait()

	// modify file to test checker
	filePath := filepath.Join(dir, filenames[0])
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	defer file.Close()
	c.Assert(err, IsNil)
	randBytes := make([]byte, 1024)
	_, err = rand.Read(randBytes)
	c.Assert(err, IsNil)
	_, err = file.Write(randBytes)
	c.Assert(err, IsNil)

	fu.WaitAndClose()
	for _, filename := range filenames {
		sourceHash := NewMd5FileHash()
		sourceFile, err := os.OpenFile(filepath.Join(dir, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer sourceFile.Close()
		_, err = io.Copy(sourceHash, sourceFile)
		c.Assert(err, IsNil)
		targetHash := NewMd5FileHash()
		targetFile, err := os.OpenFile(filepath.Join(targetDir, filename), os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		c.Assert(err, IsNil)
		defer targetFile.Close()
		_, err = io.Copy(targetHash, targetFile)
		c.Assert(err, IsNil)
		c.Assert(sourceHash.String(), Equals, targetHash.String(), Commentf("hash check failure, file name: %s", filename))
		log.Infof("file: %s sourceHash: %s targetHash: %s", filename, sourceHash, targetHash)
	}
}
