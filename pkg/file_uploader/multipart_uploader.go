// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package file_uploader

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
)

// UploadErrorType is the upload error code.
type UploadErrorType int

const (
	FileOrSliceNotExist UploadErrorType = iota + 1
	SliceAlreadyUpdated
	SliceUpdatedFailed
	CheckPointUpdatedFailed
	SliceHashCalculateFailed
)

// UploadError indicate one kind of error about uploading.
type UploadError struct {
	Tp      UploadErrorType
	message string
}

// Error implements Error interface.
func (up UploadError) Error() string {
	return up.message
}

func newUploadError(tp UploadErrorType, format string, args ...interface{}) *UploadError {
	return &UploadError{tp, fmt.Sprintf(format, args...)}
}

type multipartUploader struct {
	workDir        string
	checkPoint     *checkPoint
	uploaderDriver FileUploaderDriver
}

func newMultipartUploader(workDir string, checkPoint *checkPoint, fileUploader FileUploaderDriver) *multipartUploader {
	return &multipartUploader{workDir, checkPoint, fileUploader}
}

func (mu *multipartUploader) upload(si *Slice) *UploadError {
	if !si.isValid() {
		return newUploadError(FileOrSliceNotExist, "file or slice is't exist; Slice %#v", si)
	}
	if mu.checkPoint.isSliceUploadSuccessful(si) {
		hash := mu.uploaderDriver.Hash()
		_, err := si.writeTo(hash)
		if err != nil {
			return newUploadError(SliceHashCalculateFailed, "can't calculate the hash of slice; Slice %#v ; Err %#v", si, err)
		}
		if mu.checkPoint.checkHash(si, hash.String()) {
			return newUploadError(SliceAlreadyUpdated, "slice is already updated; Slice %#v", si)
		}
	}
	hash, err := mu.uploaderDriver.Upload(si)
	if err != nil {
		return newUploadError(SliceUpdatedFailed, "slice is updated failed; Slice %#v ; Err %#v", si, err)
	}
	err = mu.checkPoint.logSliceUpload(si, hash, true)
	if err != nil {
		return newUploadError(CheckPointUpdatedFailed, "checkpoint is updated failed; Slice %#v", si)
	}
	log.Infof("Slice upload completed, slice:%#v", si)
	return nil
}

var checkPointRunning sync2.AtomicInt32

const CheckPointFile = ".fu_check_point"

type checkPoint struct {
	status         map[string]map[int64]*indexCheckPoint
	rwLock         *sync.RWMutex
	checkPointFile string
}

type indexCheckPoint struct {
	Uploaded bool      `json:"uploaded"`
	Date     time.Time `json:"date"`
	Hash     string    `json:"hash"`
	Offset   int64     `json:"offset"`
	Length   int64     `json:"length"`
}

func loadCheckPoint(workDir string) (*checkPoint, error) {
	if !checkPointRunning.CompareAndSwap(0, 1) {
		return nil, errors.New("checkPoint is already running")
	}
	checkPoint := checkPoint{rwLock: new(sync.RWMutex)}
	checkPoint.checkPointFile = filepath.Join(workDir, CheckPointFile)
	if _, err := os.Stat(checkPoint.checkPointFile); err == nil {
		// checkPointFile is exist
		jsonBytes, err := ioutil.ReadFile(checkPoint.checkPointFile)
		if err != nil {
			return nil, errors.Annotate(err, "error thrown during read checkPointFile file")
		}
		if err := json.Unmarshal(jsonBytes, &checkPoint.status); err != nil {
			return nil, errors.Annotate(err, "error thrown during unmarshal json")
		}
	} else {
		checkPoint.status = make(map[string]map[int64]*indexCheckPoint)
	}
	return &checkPoint, nil
}

func (cp *checkPoint) logSliceUpload(si *Slice, hash string, successful bool) error {
	cp.rwLock.Lock()
	defer cp.rwLock.Unlock()
	fileCp, exist := cp.status[si.FilePath]
	if !exist {
		fileCp = make(map[int64]*indexCheckPoint)
		cp.status[si.FilePath] = fileCp
	}
	fileCp[si.Index] = &indexCheckPoint{successful, time.Now(), hash, si.Offset, si.Length}

	jsonBytes, err := json.Marshal(cp.status)
	if err != nil {
		return errors.Annotate(err, "error thrown during marshaling json")
	}
	checkPointFile, err := os.OpenFile(cp.checkPointFile, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return errors.Annotate(err, "error thrown during open checkPointFile file")
	}
	defer checkPointFile.Close()
	_, err = checkPointFile.Write(jsonBytes)
	if err != nil {
		return errors.Annotate(err, "error thrown during write checkPointFile file")
	}
	return nil
}

func (cp *checkPoint) isSliceUploadSuccessful(si *Slice) bool {
	cp.rwLock.RLock()
	defer cp.rwLock.RUnlock()
	fileCp, exist := cp.status[si.FilePath]
	if !exist {
		return exist
	}
	indexCp, exist := fileCp[si.Index]
	if !exist {
		return exist
	}
	return indexCp.Uploaded && indexCp.Offset == si.Offset && indexCp.Length == si.Length
}

func (cp *checkPoint) checkHash(si *Slice, hash string) bool {
	cp.rwLock.RLock()
	defer cp.rwLock.RUnlock()
	indexCp := cp.status[si.FilePath][si.Index]
	return hash == indexCp.Hash && indexCp.Uploaded &&
		indexCp.Offset == si.Offset && indexCp.Length == si.Length
}
