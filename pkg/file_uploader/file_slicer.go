package file_uploader

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pingcap/errors"
)

// FileSlicer slices file into `Slice`.
// It can restore from checkpoint and designed for append only file.
// Don't worry about random write file, `Checker` will guarantee file consistency.
type FileSlicer struct {
	workDir     string
	sliceStatus *sliceStatus
}

type sliceStatus struct {
	statusFile     string
	SliceSize      int64            `json:"slice_size"`
	SliceTotalSize map[string]int64 `json:"slice_total_size"`
}

type Slice struct {
	FilePath string
	FileName string
	Index    int64
	Offset   int64
	Length   int64
}

func (s *Slice) isValid() bool {
	fileInfo, err := os.Stat(s.FilePath)
	if err != nil || fileInfo.IsDir() || fileInfo.Size() < s.Offset+s.Length {
		return false
	}
	return true
}

func (s *Slice) writeTo(writer io.Writer) (int64, error) {
	file, err := os.OpenFile(s.FilePath, os.O_RDONLY, 0666)
	if err != nil {
		return 0, errors.Annotatef(err, "open file failed %#v", file)
	}
	defer file.Close()
	ret, err := file.Seek(s.Offset, 0)
	if err != nil {
		return 0, errors.Annotatef(err, "seek file failed %#v", file)
	}
	if ret != s.Offset {
		return 0, errors.Errorf("seek file failed %#v", file)
	}
	written, err := io.CopyN(writer, file, s.Length)
	if err != nil {
		return 0, errors.Annotate(err, "write io.Writer failed")
	}
	return written, nil
}

const SliceStatusFile = ".fu_slice_status"

// NewFileSlicer creates a `FileSlicer` load from `statusFile`
func NewFileSlicer(workDir string, sliceSize int64) (*FileSlicer, error) {
	statusFile := filepath.Join(workDir, SliceStatusFile)
	sliceStatus, err := loadSliceStatus(statusFile, sliceSize)
	if err != nil {
		return nil, errors.Annotate(err, "error thrown during load slice status")
	}
	return &FileSlicer{
		workDir,
		sliceStatus,
	}, nil
}

func loadSliceStatus(statusFile string, sliceSize int64) (*sliceStatus, error) {
	var sliceStatus sliceStatus
	if _, err := os.Stat(statusFile); err == nil {
		// statusFile is exist
		jsonBytes, err := ioutil.ReadFile(statusFile)
		if err != nil {
			return nil, errors.Annotate(err, "error thrown during read statusFile file")
		}
		if err := json.Unmarshal(jsonBytes, &sliceStatus); err != nil {
			return nil, errors.Annotate(err, "error thrown during unmarshal json")
		}
		if sliceSize != sliceStatus.SliceSize {
			return nil, errors.New("can't restore from checkpoint, the slice_size is different from status file")
		}
	} else {
		sliceStatus.SliceTotalSize = make(map[string]int64)
		sliceStatus.SliceSize = sliceSize
	}
	sliceStatus.statusFile = statusFile
	return &sliceStatus, nil
}

func (ss *sliceStatus) flush() error {
	jsonBytes, err := json.Marshal(ss)
	if err != nil {
		return errors.Annotate(err, "error thrown during marshaling json")
	}
	statusFile, err := os.OpenFile(ss.statusFile, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0660)
	if err != nil {
		return errors.Annotate(err, "error thrown during open statusFile file")
	}
	defer statusFile.Close()
	_, err = statusFile.Write(jsonBytes)
	if err != nil {
		return errors.Annotate(err, "error thrown during write statusFile file")
	}
	return nil
}

// DoSlice slices `file` and returns Slice Arrays.
func (fs *FileSlicer) DoSlice(path string, file os.FileInfo) ([]Slice, error) {
	sliceSize := fs.sliceStatus.SliceSize
	fileName := file.Name()
	fileSize := file.Size()
	oldSliceTotalSize := fs.sliceStatus.SliceTotalSize[fileName]
	fs.sliceStatus.SliceTotalSize[fileName] = fileSize
	var sliceInfos []Slice
	var i int64
	for i = oldSliceTotalSize / sliceSize; i*sliceSize < fileSize; i++ {
		thisSliceSize := sliceSize
		if thisSliceSize+i*sliceSize > fileSize {
			thisSliceSize = fileSize - i*sliceSize
		}
		sliceInfo := Slice{
			FilePath: path,
			FileName: fileName,
			Index:    i,
			Offset:   i * sliceSize,
			Length:   thisSliceSize,
		}
		sliceInfos = append(sliceInfos, sliceInfo)
	}
	err := fs.sliceStatus.flush()
	if err != nil {
		return nil, errors.Annotate(err, "error thrown during flushing slice status")
	}
	return sliceInfos, nil
}
