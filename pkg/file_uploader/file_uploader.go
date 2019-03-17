package file_uploader

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
)

// FileUploader checks the files changes in the work directory,
// uploads files by FileUploaderDriver.
type FileUploader struct {
	workDir         string
	slicer          *FileSlicer
	uploaderDriver  FileUploaderDriver
	slicesChan      chan Slice
	slicesSize      int64
	uploaderWait    sync.WaitGroup
	watcherWait     sync.WaitGroup
	uploadingStatus sync.Map
	lastChangeTime  int64
	// closedStatus 0: working, 1: watcher closed, 2: slicesChan closed.
	workingStatus sync2.AtomicInt32
}

const (
	statusWorking          = 0
	statusWatcherClosed    = 1
	statusSlicesChanClosed = 2
)

// NewFileUploader creates a FileUploader instance.
func NewFileUploader(workDir string, workerNum int, slicesSize int64, uploaderDriver FileUploaderDriver) *FileUploader {
	fileSlicer, err := NewFileSlicer(workDir, slicesSize)
	fu := &FileUploader{
		workDir:        workDir,
		slicer:         fileSlicer,
		uploaderDriver: uploaderDriver,
		slicesChan:     make(chan Slice, 4),
		slicesSize:     slicesSize,
	}
	fu.workingStatus.Set(statusWorking)
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
				_, uploading := fu.uploadingStatus.Load(slice)
				if uploading {
					fu.slicesChan <- slice
					time.Sleep(1 * time.Second)
					continue
				}
				if fu.workingStatus.Get() == statusSlicesChanClosed && len(fu.slicesChan) == 0 {
					close(fu.slicesChan)
				}
				fu.uploadingStatus.Store(slice, true)
				err := mu.upload(&slice)
				fu.uploadingStatus.Delete(slice)
				if err != nil && err.Tp != SliceAlreadyUpdated {
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
			if slice.Length < fu.slicesSize {
				continue
			}
			log.Debugf("output slice %#v", slice)
			fu.slicesChan <- slice
		}
		return nil
	}
	for fu.workingStatus.Get() == statusWorking {
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
	fu.workingStatus.Set(statusWatcherClosed)
	fu.watcherWait.Wait()
	err := fu.checkAndCompleteUpload()
	if err != nil {
		log.Errorf("check failure: %#v", err)
	}
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
	if err != nil {
		return errors.Trace(err)
	}
	fu.workingStatus.Set(statusSlicesChanClosed)
	fu.uploaderWait.Wait()
	var previousErr error
	for _, path := range filePaths {
		_, err := fu.uploaderDriver.Complete(path)
		if err != nil {
			previousErr = err
			log.Errorf("Complete Uploading failure: %#v", err)
		}
		log.Infof("file upload completed: %s", path)
	}
	return errors.Trace(previousErr)
}
