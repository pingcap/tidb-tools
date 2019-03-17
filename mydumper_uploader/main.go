package main

import (
	"flag"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/file_uploader"
)

var (
	mydumper           = flag.String("mydumper", "mydumper", "MyDumper executable file path")
	mydumperArgs       = flag.String("mydumperargs", "--help", "MyDumper args")
	awsAccessKeyId     = flag.String("awsaccesskeyid", "", "AWS S3 Access Key ID")
	awsSecretAccessKey = flag.String("awssecretaccesskey", "", "AWS S3 Secret Access Key")
	awsBucketRegion    = flag.String("awsbucketregion", "", "AWS S3 Bucket Region")
	awsBucketName      = flag.String("awsbucketname", "", "AWS S3 Bucket Name")
	workDir            = flag.String("workdir", "", "Work Dir")
	remoteDir          = flag.String("remotedir", "", "Remote Dir")
	workerNum          = flag.Int("worker", 1, "Worker Thread Number")
	sliceSize          = flag.Int64("slice", 16*1024*1024, "Upload File Slice Size(byte)")
	uploaderArgs       []string
)

func main() {
	flag.Parse()
	cmd := execMydumper(strings.TrimSpace(*mydumperArgs))
	time.Sleep(1 * time.Second)
	driver, err := file_uploader.NewAWSS3FileUploaderDriver(
		strings.TrimSpace(*awsAccessKeyId),
		strings.TrimSpace(*awsSecretAccessKey),
		strings.TrimSpace(*awsBucketRegion),
		strings.TrimSpace(*awsBucketName),
		strings.TrimSpace(*workDir),
		strings.TrimSpace(*remoteDir))

	if err != nil {
		log.Fatalf("AWS S3 Driver create failure, err: %#v", err)
		os.Exit(-1)
	}
	uploader := file_uploader.NewFileUploader(*workDir, *workerNum, *sliceSize, driver)
	err = cmd.Wait()
	if err != nil {
		log.Errorf("Exec Mydumper failure %s", err.Error())
		os.Exit(-1)
	}
	log.Info("mydumper exited.")
	uploader.WaitAndClose()
}

func execMydumper(args string) *exec.Cmd {
	cmd := exec.Command(*mydumper, strings.Split(args, " ")...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		log.Errorf("Exec Mydumper failure %s", err.Error())
		os.Exit(-1)
	}
	return cmd
}
