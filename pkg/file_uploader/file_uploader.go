package file_uploader

import (
	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

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

	log.Infof("wait %d", workerNum)
	fu.uploaderWait.Add(workerNum)
	fu.createWorker(workerNum)
	return fu
}
func (fu *FileUploader) createWorker(workerNum int) {
	log.Info("create worker")
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
			log.Infof("wait done")
			fu.uploaderWait.Done()
		}()
	}
}

func (fu *FileUploader) process() {
	log.Info("in process")
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
		log.Info("work...")
		err := filepath.Walk(fu.workDir, workFunc)
		if err != nil {
			log.Errorf("watch workDir failure: %#v", err)
		}
		time.Sleep(1 * time.Second)
	}
	fu.watcherWait.Done()
}

func (fu *FileUploader) WaitAndClose() {
	time.Sleep(1 * time.Second)
	log.Info("close")
	fu.closed = true
	fu.watcherWait.Wait()
	log.Info("watcherWait wc")
	err := fu.checkAndCompleteUpload()
	if err != nil {
		log.Errorf("check failure: %#v", err)
	}
	close(fu.slicesChan)
	fu.uploaderWait.Wait()
	log.Info("uploaderWait wc")
	//check
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
			fu.slicesChan <- slice
		}
		return nil
	}
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
		log.Infof("file hash check %s, sourceHash: %s, targetHash: %s", path, sourceHash, targetHash)
		if targetHash != targetHash {
			err = errors.New("some file check failure")
			log.Errorf("file check failure: %s, sourceHash: %s, targetHash: %s", path, sourceHash, targetHash)
		}
	}
	return errors.Trace(err)
}
