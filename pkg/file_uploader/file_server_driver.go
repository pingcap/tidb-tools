package file_uploader

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pingcap/errors"
	"github.com/prometheus/common/log"
	"github.com/siddontang/go/sync2"
)

// FileHash used to calculate files Hash.
type FileHash interface {
	hash.Hash
	fmt.Stringer
}

// FileUploaderDriver is file server driver which can slice upload file.
type FileUploaderDriver interface {
	// Upload one slice.
	// returns the hash of this slice.
	Upload(sliceInfo *Slice) (string, error)
	// Hash returns an instance of FileHash interface, which hash algorithm is same with file server.
	Hash() FileHash
	// Complete tells the file server that all slices of the file at `path` are already uploaded.
	// Generally, file server will merge slices at this time.
	Complete(path string) (string, error)
	// Close tells the file server that all files are uploaded.
	Close() error
}

// Md5FileHash calculates md5 hash of input stream.
type Md5FileHash struct {
	h hash.Hash
}

// NewMd5FileHash creates a new md5 calculator.
func NewMd5FileHash() *Md5FileHash {
	return &Md5FileHash{md5.New()}
}

// Write implements Hash interface.
func (fh *Md5FileHash) Write(p []byte) (n int, err error) {
	return fh.h.Write(p)
}

// Sum implements Hash interface.
func (fh *Md5FileHash) Sum(b []byte) []byte {
	return fh.h.Sum(b)
}

// Reset implements Hash interface.
func (fh *Md5FileHash) Reset() {
	fh.h.Reset()
}

// Size implements Hash interface.
func (fh *Md5FileHash) Size() int {
	return fh.h.Size()
}

// BlockSize implements Hash interface.
func (fh *Md5FileHash) BlockSize() int {
	return fh.h.BlockSize()
}

// BlockSize implements Stringer interface.
func (fh *Md5FileHash) String() string {
	return hex.EncodeToString(fh.Sum(nil))
}

// AWSS3FileUploaderDriver is a uploader driver of AWS S3, using the official s3 sdk.
// AWS Official S3 SDK: https://aws.amazon.com/cn/sdk-for-go/
type AWSS3FileUploaderDriver struct {
	s3             *s3.S3
	bucketName     string
	awsUploadIdSet *awsUploadIdSet
	remoteDir      string
	workDir        string
}

// NewAWSS3FileUploaderDriver create a new `AWSS3FileUploaderDriver` instance.
// remoteDir should compliance with the S3 key format rule.
// AWS S3 Key format, see: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
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

	// AWS S3 Key format, see: https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
	s3KeyExp := regexp.MustCompile(`^([a-zA-Z0-9!\-_.*'()]+\/)+$`)
	if !s3KeyExp.MatchString(remoteDir) {
		return nil, errors.Errorf("remote dir format error or contains illegal characters")
	}
	s3 := s3.New(session, cfg)
	return &AWSS3FileUploaderDriver{s3, bucketName, awsUs, remoteDir, workDir}, nil
}

// Upload implements FileUploaderDriver interface.
func (fud *AWSS3FileUploaderDriver) Upload(sliceInfo *Slice) (string, error) {
	relFilePath, err := filepath.Rel(fud.workDir, sliceInfo.FilePath)
	if err != nil {
		return "", errors.Trace(err)
	}
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
	eTag, err := fud.uploadPart(uploadId, key, sliceInfo)
	if err != nil {
		return "", nil
	}
	err = fud.awsUploadIdSet.putETag(key, sliceInfo.Index, eTag)
	if err != nil {
		return "", errors.Trace(err)
	}
	return eTag, nil
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

func (fud *AWSS3FileUploaderDriver) uploadPart(uploadId, key string, slice *Slice) (string, error) {
	file, err := os.OpenFile(slice.FilePath, os.O_RDONLY, 0444)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer file.Close()
	hash := fud.Hash()
	_, err = io.Copy(hash, io.NewSectionReader(file, slice.Offset, slice.Length))
	if err != nil {
		return "", errors.Trace(err)
	}
	md5 := hash.String()
	output, err := fud.s3.UploadPart(&s3.UploadPartInput{
		Body:          io.NewSectionReader(file, slice.Offset, slice.Length),
		Bucket:        aws.String(fud.bucketName),
		Key:           aws.String(key),
		PartNumber:    aws.Int64(slice.Index + 1),
		UploadId:      aws.String(uploadId),
		ContentLength: aws.Int64(slice.Length),
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	if *output.ETag != fmt.Sprintf(`"%s"`, md5) {
		return "", errors.Errorf("hash check failure, slice: %#v", slice)
	}
	return md5, nil
}

func (fud *AWSS3FileUploaderDriver) completeUpload(uploadId, key string, parts []*s3.CompletedPart) (string, error) {
	_, err := fud.s3.CompleteMultipartUpload(&s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(fud.bucketName),
		Key:      aws.String(key),
		UploadId: aws.String(uploadId),
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	output, err := fud.s3.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(fud.bucketName),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", errors.Trace(err)
	}
	return *output.ETag, nil
}

// Hash implements FileUploaderDriver interface.
func (fud *AWSS3FileUploaderDriver) Hash() FileHash {
	return NewMd5FileHash()
}

// Complete implements FileUploaderDriver interface.
func (fud *AWSS3FileUploaderDriver) Complete(path string) (string, error) {
	relFilePath, err := filepath.Rel(fud.workDir, path)
	if err != nil {
		return "", errors.Trace(err)
	}
	key := filepath.Join(fud.remoteDir, relFilePath)
	uploadId, exist := fud.awsUploadIdSet.getUploadId(key)
	if !exist {
		return "", errors.Errorf("key %s not exist", key)
	}
	parts := fud.awsUploadIdSet.getCompletedParts(key)
	hash, err := fud.completeUpload(uploadId, key, parts)
	if err != nil {
		return "", errors.Trace(err)
	}
	return hash, nil
}

// Close implements FileUploaderDriver interface.
func (fud *AWSS3FileUploaderDriver) Close() error {
	output, err := fud.s3.ListMultipartUploads(&s3.ListMultipartUploadsInput{
		Bucket: aws.String(fud.bucketName),
		Prefix: aws.String(fud.remoteDir),
	})
	if err != nil {
		return errors.Trace(err)
	}
	for _, multipartUpload := range output.Uploads {
		_, err = fud.s3.AbortMultipartUpload(&s3.AbortMultipartUploadInput{
			Bucket:   aws.String(fud.bucketName),
			Key:      multipartUpload.Key,
			UploadId: multipartUpload.UploadId,
		})
	}
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("abort %d multipart upload", len(output.Uploads))
	return nil
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
	if item.ETag == nil {
		item.ETag = make(map[int64]string)
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

type completedParts []*s3.CompletedPart

func (s completedParts) Len() int {
	return len(s)
}
func (s completedParts) Less(i, j int) bool {
	return *s[i].PartNumber < *s[j].PartNumber
}

func (s completedParts) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (us *awsUploadIdSet) getCompletedParts(key string) []*s3.CompletedPart {
	us.rwLock.RLock()
	defer us.rwLock.RUnlock()
	item, exist := us.status[key]
	result := make([]*s3.CompletedPart, 0, len(item.ETag))
	if !exist {
		return result
	}
	for index, eTag := range item.ETag {
		result = append(result, &s3.CompletedPart{ETag: aws.String(eTag), PartNumber: aws.Int64(index + 1)})
	}
	sort.Sort(completedParts(result))
	return result
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
