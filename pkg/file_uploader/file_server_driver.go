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
	"github.com/pingcap/errors"
	"github.com/siddontang/go/sync2"
	"hash"
	"io"
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
	Complete(path string) (string, error)
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
	workDir        string
}

func NewAWSS3FileUploaderDriver(accessKeyID string, secretAccessKey string, bucketRegion string, bucketName string, workDir string, remoteDir string) (*AWSS3FileUploaderDriver, error) {
	awsUs, err := loadAWSUploadId(workDir)
	if err != nil {
		return nil, errors.Annotatef(err, "can't load AWSUploadIdSet")
	}
	creds := credentials.NewStaticCredentials(accessKeyID, secretAccessKey, "")
	_, err = creds.Get()
	if err != nil {
		return nil, errors.Annotatef(err, "invalid AWSAccessKey")
	}
	cfg := aws.NewConfig().WithRegion(bucketRegion).WithCredentials(creds)
	session, err := session.NewSession()
	if err != nil {
		return nil, errors.Annotatef(err, "create aws session failed")
	}
	s3 := s3.New(session, cfg)
	return &AWSS3FileUploaderDriver{s3, bucketName, awsUs, remoteDir, workDir}, nil
}

func (fud *AWSS3FileUploaderDriver) Upload(sliceInfo *Slice) (string, error) {
	relFilePath, err := filepath.Rel(fud.workDir, sliceInfo.FilePath)
	key := filepath.Join(fud.remoteDir, relFilePath)
	uploadId, exist := fud.awsUploadIdSet.getUploadId(key)
	if !exist {
		uploadId, err = fud.createUpload(key)
		if err != nil {
			return "", errors.Trace(err)
		}
		err = fud.awsUploadIdSet.putUploadId(key, uploadId)
		if err != nil {
			return "", errors.Trace(err)
		}

	}
	md5, eTag, err := fud.uploadPart(uploadId, key, sliceInfo)
	if err != nil {
		return "", nil
	}
	err = fud.awsUploadIdSet.putETag(key, sliceInfo.Index, eTag)
	if err != nil {
		return "", errors.Trace(err)
	}
	return md5, nil
}

func (fud *AWSS3FileUploaderDriver) createUpload(key string) (string, error) {
	resp, err := fud.s3.CreateMultipartUpload(&s3.CreateMultipartUploadInput{
		Bucket:      aws.String(fud.bucketName),
		Key:         aws.String(key),
		ContentType: aws.String("application/octet-stream"),
	})
	if err != nil {
		return "", errors.Annotate(err, "AWS S3 create multipart upload failure")
	}
	return *resp.UploadId, nil

}

func (fud *AWSS3FileUploaderDriver) uploadPart(uploadId, key string, slice *Slice) (string, string, error) {
	file, err := os.OpenFile(slice.FilePath, os.O_RDONLY, 0444)
	if err != nil {
		return "", "", errors.Trace(err)
	}
	defer file.Close()
	hash := fud.Hash()
	_, err = io.Copy(hash, io.NewSectionReader(file, slice.Offset, slice.Length))
	if err != nil {
		return "", "", errors.Trace(err)
	}
	md5 := hash.String()
	output, err := fud.s3.UploadPart(&s3.UploadPartInput{
		Body:          io.NewSectionReader(file, slice.Offset, slice.Length),
		Bucket:        aws.String(fud.bucketName),
		Key:           aws.String(key),
		PartNumber:    aws.Int64(slice.Index),
		UploadId:      aws.String(uploadId),
		ContentLength: aws.Int64(slice.Length),
		ContentMD5:    aws.String(md5),
	})
	if err != nil {
		return "", "", errors.Trace(err)
	}
	return md5, *output.ETag, nil
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
