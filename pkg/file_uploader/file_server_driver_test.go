package file_uploader

import (
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
)

var _ = Suite(&testServerDriver{})

type testServerDriver struct{}

type MockFileUploaderDriver struct {
	workDir   string
	targetDir string
}

func NewMockFileUploaderDriver(workDir, targetDir string) *MockFileUploaderDriver {
	return &MockFileUploaderDriver{workDir, targetDir}
}

func (m *MockFileUploaderDriver) Upload(sliceInfo *Slice) (string, error) {
	srcFile, err := os.OpenFile(sliceInfo.FilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer srcFile.Close()
	_, err = srcFile.Seek(sliceInfo.Offset, 0)
	if err != nil {
		return "", errors.Trace(err)
	}
	relPath, err := filepath.Rel(m.workDir, sliceInfo.FilePath)
	if err != nil {
		return "", errors.Trace(err)
	}
	targetPath := filepath.Join(m.targetDir, relPath)
	err = os.MkdirAll(filepath.Dir(targetPath), 0777)
	if err != nil {
		return "", errors.Trace(err)
	}
	targetFile, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer targetFile.Close()
	_, err = targetFile.Seek(sliceInfo.Offset, 0)
	if err != nil {
		return "", errors.Trace(err)
	}
	buf := make([]byte, 1024)
	hash := m.Hash()
	for {
		n, _ := srcFile.Read(buf)
		if 0 == n {
			break
		}
		_, err := hash.Write(buf[:n])
		if err != nil {
			return "", errors.Trace(err)
		}
		_, err = targetFile.Write(buf[:n])
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	return hash.String(), nil
}

func (m *MockFileUploaderDriver) Hash() FileHash {
	return NewMd5FileHash()
}

func (m *MockFileUploaderDriver) Complete(path string) (string, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	defer file.Close()
	if err != nil {
		return "", errors.Trace(err)
	}
	hash := m.Hash()
	_, err = io.Copy(hash, file)
	if err != nil {
		return "", errors.Trace(err)
	}
	return hash.String(), nil
}

func (m *MockFileUploaderDriver) Close() error {
	return nil
}

func (t *testServerDriver) ManualTestAWSS3ServerDriver(c *C) {
	dir, err := ioutil.TempDir("", "up_awss3_server_sriver")
	defer os.RemoveAll(dir)
	c.Assert(err, IsNil)
	aws, err := NewAWSS3FileUploaderDriver("", "", "", "", dir, "")
	c.Assert(err, IsNil)

	// create the file to be uploaded
	filename := "test_fil1e1"
	rand := rand.New(rand.NewSource(time.Now().Unix()))
	sourceFilePath := filepath.Join(dir, filename)
	file, err := os.OpenFile(sourceFilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	c.Assert(err, IsNil)
	defer file.Close()
	_, err = io.CopyN(file, rand, 33*M)
	c.Assert(err, IsNil)

	// slice file
	fileSlicer, err := NewFileSlicer(dir, 10*M)
	fileInfo, err := os.Stat(sourceFilePath)
	slices, err := fileSlicer.DoSlice(sourceFilePath, fileInfo)
	c.Assert(err, IsNil)

	// upload slice
	for _, slice := range slices {
		_, err := aws.Upload(&slice)
		c.Assert(err, IsNil)
	}
	_, err = aws.Complete(sourceFilePath)
	c.Assert(err, IsNil)

	// check hash
	sourceFile, err := os.OpenFile(sourceFilePath, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	c.Assert(err, IsNil)
	defer sourceFile.Close()
	err = aws.Close()
	c.Assert(err, IsNil)
}
