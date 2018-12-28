package file_uploader

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/watcher"
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
	watcher        *watcher.Watcher
	slicer         *FileSlicer
	uploaderDriver FileUploaderDriver
	slicesChan     chan Slice
	wait           sync.WaitGroup
}

func NewFileUploader(workDir string, workerNum int, slicesSize int64, uploaderDriver FileUploaderDriver) *FileUploader {
	watcher := watcher.NewWatcher()
	err := watcher.Add(workDir)
	if err != nil {
		log.Errorf("watcher load failure: %#v", err)
	}
	err = watcher.Start(5 * time.Second)
	if err != nil {
		log.Errorf("watcher load failure: %#v", err)
	}
	fileSlicer, err := NewFileSlicer(workDir, slicesSize)
	fu := &FileUploader{
		workDir:        workDir,
		watcher:        watcher,
		slicer:         fileSlicer,
		uploaderDriver: uploaderDriver,
		slicesChan:     make(chan Slice, 64),
	}
	go fu.process()
	fu.wait.Add(workerNum + 1)
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
			fu.wait.Done()
		}()
	}
}

func (fu *FileUploader) process() {
	workFunc := func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return errors.Trace(err)
		}
		if info.IsDir() {
			return nil
		}
		if strings.HasPrefix(info.Name(), ".fu_") {
			return nil
		}
		//log.Errorf("path: %#v info %#v", path, info)
		slices, err := fu.slicer.DoSlice(path, info)
		for _, slice := range slices {
			log.Infof("slice %#v", slice)
			fu.slicesChan <- slice
		}
		return nil
	}
	err := filepath.Walk(fu.workDir, workFunc)
	if err != nil {
		log.Errorf("watch workDir failure: %#v", err)
	}
loop:
	for {
		select {
		case ev, opened := <-fu.watcher.Events:
			if !opened {
				break loop
			}
			if ev.Op != watcher.Create && ev.Op != watcher.Modify {
				continue
			}
			log.Infof("ev %#v", ev)
			err := workFunc(ev.Path, ev.FileInfo, nil)
			if err != nil {
				log.Errorf("watch workDir failure: %#v", err)
			}
		case err2, opened := <-fu.watcher.Errors:
			if !opened {
				break loop
			}
			if err2 != nil {
				log.Errorf("watch workDir failure: %#v", err2)
			}
		}
	}
	fu.wait.Done()
}

func (fu *FileUploader) WaitAndClose() {
	fu.watcher.Close()
	close(fu.slicesChan)
	fu.wait.Wait()
	//check
}
