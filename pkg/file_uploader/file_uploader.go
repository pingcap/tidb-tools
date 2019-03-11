package file_uploader

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
)

// FileUploader checks the files changes in the work directory,
// uploads files by FileUploaderDriver.
type FileUploader struct {
	workDir        string
	slicer         *FileSlicer
	uploaderDriver FileUploaderDriver
	slicesChan     chan Slice
	uploaderWait   sync.WaitGroup
	watcherWait    sync.WaitGroup
	lastChangeTime int64
	closed         bool
}

// NewFileUploader creates a FileUploader instance.
func NewFileUploader(workDir string, workerNum int, slicesSize int64, uploaderDriver FileUploaderDriver) *FileUploader {
	fileSlicer, err := NewFileSlicer(workDir, slicesSize)
	fu := &FileUploader{
		workDir:        workDir,
		slicer:         fileSlicer,
		uploaderDriver: uploaderDriver,
		slicesChan:     make(chan Slice, 4),
		closed:         false,
	}
	if err != nil {
		log.Errorf("watcher load failure: %#v", err)
	}
	fu.watcherWait.Add(1)
	if err != nil {
		fu.watcherWait.Done()
		log.Errorf("watcher load failure: %#v", err)
	}
	go fu.process()
	fu.uploaderWait.Add(workerNum)
	fu.createWorker(workerNum)
	return fu
}
func (fu *FileUploader) createWorker(workerNum int) {
	log.Info("start", workerNum, "worker.")
	cp, err := loadCheckPoint(fu.workDir)
	if err != nil {
		log.Fatalf("check point load failure: %#v", err)
		os.Exit(-1)
	}
	for i := 0; i < workerNum; i++ {
		go func() {
			mu := newMultipartUploader(fu.workDir, cp, fu.uploaderDriver)
			for slice := range fu.slicesChan {
				err := mu.upload(&slice)
				if err != nil {
					log.Errorf("slice %#v upload failure: %#v", slice, err)
				}
			}
			log.Debugf("worker done")
			fu.uploaderWait.Done()
		}()
	}
}

func (fu *FileUploader) process() {
	workFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}
		if strings.HasPrefix(info.Name(), ".fu_") {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		slices, err := fu.slicer.DoSlice(path, info)
		for _, slice := range slices {
			log.Debugf("output slice %#v", slice)
			fu.slicesChan <- slice
		}
		return nil
	}
	for !fu.closed {
		err := filepath.Walk(fu.workDir, workFunc)
		if err != nil {
			log.Errorf("watch workDir failure: %#v", err)
		}
		time.Sleep(1 * time.Second)
	}
	fu.watcherWait.Done()
}

// WaitAndClose stops checking the files changes in work directory.
// And waits for all files uploaded completed.
func (fu *FileUploader) WaitAndClose() {
	time.Sleep(1 * time.Second)
	fu.closed = true
	fu.watcherWait.Wait()
	err := fu.checkAndCompleteUpload()
	if err != nil {
		log.Errorf("check failure: %#v", err)
	}
	close(fu.slicesChan)
	fu.uploaderWait.Wait()
	fu.uploaderDriver.Close()
}

func (fu *FileUploader) checkAndCompleteUpload() error {
	var filePaths []string
	workFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}
		if strings.HasPrefix(info.Name(), ".fu_") {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		filePaths = append(filePaths, path)
		slices, err := fu.slicer.DoSlice(path, info)
		for _, slice := range slices {
			log.Debugf("check slice %#v", slice)
			fu.slicesChan <- slice
		}
		return nil
	}
	log.Info("overall check")
	fu.slicer.reset()
	err := filepath.Walk(fu.workDir, workFunc)
	for _, path := range filePaths {
		targetHash, err := fu.uploaderDriver.Complete(path)
		if err != nil {
			return errors.Trace(err)
		}
		file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
		defer file.Close()
		if err != nil {
			return errors.Trace(err)
		}
		hash := fu.uploaderDriver.Hash()
		_, err = io.Copy(hash, file)
		if err != nil {
			return errors.Trace(err)
		}
		sourceHash := hash.String()
		log.Debugf("file hash check %s, sourceHash: %s, targetHash: %s", path, sourceHash, targetHash)
		if targetHash != targetHash {
			err = errors.New("some file check failure")
			log.Errorf("file check failure: %s, sourceHash: %s, targetHash: %s", path, sourceHash, targetHash)
		}
	}
	return errors.Trace(err)
}
