package file_uploader

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"os"
	"path/filepath"
)

var _ = Suite(&testFileUploader{})

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
	return NewMd5Base64FileHash()
}
