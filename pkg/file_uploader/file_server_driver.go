package file_uploader

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/juju/errors"
	"github.com/siddontang/go/sync2"
	"hash"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type FileHash interface {
	hash.Hash
	fmt.Stringer
}

type FileUploaderDriver interface {
	Upload(sliceInfo *Slice) (string, error)
	Hash() FileHash
}

type Md5Base64FileHash struct {
	h hash.Hash
}

func NewMd5Base64FileHash() *Md5Base64FileHash {
	return &Md5Base64FileHash{md5.New()}
}

func (fh *Md5Base64FileHash) Write(p []byte) (n int, err error) {
	return fh.h.Write(p)
}

func (fh *Md5Base64FileHash) Sum(b []byte) []byte {
	return fh.h.Sum(b)
}

func (fh *Md5Base64FileHash) Reset() {
	fh.h.Reset()
}

func (fh *Md5Base64FileHash) Size() int {
	return fh.h.Size()
}

func (fh *Md5Base64FileHash) BlockSize() int {
	return fh.h.BlockSize()
}

func (fh *Md5Base64FileHash) String() string {
	return base64.StdEncoding.EncodeToString(fh.Sum(nil))
}

type AWSS3FileUploaderDriver struct {
	s3             *s3.S3
	bucketName     string
	awsUploadIdSet *awsUploadIdSet
	remoteDir      string
}

func NewAWSS3FileUploaderDriver(accessKeyID string, secretAccessKey string, bucketRegion string, bucketName string, awsUploadIdSet *awsUploadIdSet, remoteDir string) (*AWSS3FileUploaderDriver, error) {
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	_, err := creds.Get()
	if err != nil {
		return nil, errors.Annotatef(err, "invalid AWSAccessKey")
	}
	cfg := aws.NewConfig().WithRegion(bucketRegion).WithCredentials(creds)
	session, err := session.NewSession()
	if err != nil {
		return nil, errors.Annotatef(err, "create aws session failed")
	}
	s3 := s3.New(session, cfg)
	return &AWSS3FileUploaderDriver{s3, bucketName, awsUploadIdSet, remoteDir}, nil
}

func (fud *AWSS3FileUploaderDriver) Upload(sliceInfo *Slice) (string, error) {
	key := fmt.Sprintf("%s/%s", fud.remoteDir, sliceInfo.FileName)
	updateId, exist := fud.awsUploadIdSet.getUploadId(key)
	if !exist {
		//TODO create upload
		// save create id
	}
	//TODO upload
	// save etag
	// check hash
	print(updateId)
	panic("implement me")
}

func (fud *AWSS3FileUploaderDriver) Hash() FileHash {
	return &Md5Base64FileHash{}
}

var awsUploadIdRunning sync2.AtomicInt32

const AWSUploadIdFile = ".fu_aws_upload_id"

type awsUploadIdSet struct {
	status          map[string]*uploadItem
	rwLock          *sync.RWMutex
	awsUploadIdFile string
}

type uploadItem struct {
	UploadId string           `json:"upload_id"`
	ETag     map[int64]string `json:"e_tag"`
}

func loadAWSUploadId(workDir string) (*awsUploadIdSet, error) {
	if !awsUploadIdRunning.CompareAndSwap(0, 1) {
		return nil, errors.New("awsUploadIdSet is already running")
	}
	awsUploadId := awsUploadIdSet{rwLock: new(sync.RWMutex)}
	awsUploadId.awsUploadIdFile = filepath.Join(workDir, AWSUploadIdFile)
	if _, err := os.Stat(awsUploadId.awsUploadIdFile); err == nil {
		// checkPointFile is exist
		jsonBytes, err := ioutil.ReadFile(awsUploadId.awsUploadIdFile)
		if err != nil {
			return nil, errors.Annotate(err, "error thrown during read checkPointFile file")
		}
		if err := json.Unmarshal(jsonBytes, &awsUploadId.status); err != nil {
			return nil, errors.Annotate(err, "error thrown during unmarshal json")
		}
	} else {
		awsUploadId.status = make(map[string]*uploadItem)
	}
	return &awsUploadId, nil
}

func (us *awsUploadIdSet) putUploadId(key string, uploadId string) error {
	us.rwLock.Lock()
	defer us.rwLock.Unlock()
	item, exist := us.status[key]
	if !exist {
		item = &uploadItem{}
		us.status[key] = item
	}
	item.UploadId = uploadId
	if err := us.save(); err != nil {
		return errors.Annotate(err, "save status failed")
	}
	return nil
}

func (us *awsUploadIdSet) putETag(key string, index int64, eTag string) error {
	us.rwLock.Lock()
	defer us.rwLock.Unlock()
	item, exist := us.status[key]
	if !exist {
		return errors.Errorf("key `%s` isn't exist in awsUploadIdFile", key)
	}
	item.ETag[index] = eTag
	if err := us.save(); err != nil {
		return errors.Annotate(err, "save status failed")
	}
	return nil
}

func (us *awsUploadIdSet) getUploadId(key string) (string, bool) {
	us.rwLock.RLock()
	defer us.rwLock.RUnlock()
	item, exist := us.status[key]
	if !exist {
		return "", false
	}
	return item.UploadId, true
}

func (us *awsUploadIdSet) save() error {
	jsonBytes, err := json.Marshal(us.status)
	if err != nil {
		return errors.Annotate(err, "error thrown during marshaling json")
	}
	awsUploadIdFile, err := os.OpenFile(us.awsUploadIdFile, os.O_CREATE|os.O_RDWR|os.O_SYNC, 0666)
	if err != nil {
		return errors.Annotate(err, "error thrown during open awsUploadIdFile file")
	}
	defer awsUploadIdFile.Close()
	_, err = awsUploadIdFile.Write(jsonBytes)
	if err != nil {
		return errors.Annotate(err, "error thrown during write awsUploadIdFile file")
	}
	return nil
}
