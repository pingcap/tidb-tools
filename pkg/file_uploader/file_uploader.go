package file_uploader

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

/*
1 watcher to slice
2 slice to multi uploader
3 mock driver
4 test
*/
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

func NewFileUploader(workDir string, workerNum int, slicesSize int64, uploaderDriver FileUploaderDriver) *FileUploader {
	fileSlicer, err := NewFileSlicer(workDir, slicesSize)
	fu := &FileUploader{
		workDir:        workDir,
		slicer:         fileSlicer,
		uploaderDriver: uploaderDriver,
		slicesChan:     make(chan Slice, 64),
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
	cp, err := loadCheckPoint(fu.workDir)
	if err != nil {
		log.Fatalf("check point load failure: %#v", err)
		os.Exit(-1)
	}
	for i := 0; i < workerNum; i++ {
		go func() {
			mu := NewMultipartUploader(fu.workDir, cp, fu.uploaderDriver)
			for slice := range fu.slicesChan {
				log.Infof("out slice %#v", slice)
				err := mu.upload(&slice)
				if err != nil {
					log.Errorf("slice %#v upload failure: %#v", slice, err)
				}
			}
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
		log.Infof("path: %#v info %#v", path, info)
		slices, err := fu.slicer.DoSlice(path, info)
		for _, slice := range slices {
			log.Infof("slice %#v", slice)
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

func (fu *FileUploader) WaitAndClose() {
	fu.closed = true
	fu.watcherWait.Wait()
	close(fu.slicesChan)
	fu.uploaderWait.Wait()
	//check
}
