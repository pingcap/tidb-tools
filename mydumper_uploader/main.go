package main

import (
	"flag"
	"fmt"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-tools/pkg/file_uploader"
	"os"
	"os/exec"
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
	workerNum          = flag.Int("worker", 8, "Worker Thread Number")
	sliceSize          = flag.Int64("slice", 100*1024*1024, "Upload File Slice Size(byte)")
	uploaderArgs       []string
)

func main() {
	defaultUsage := flag.Usage
	flag.Usage = func() {
		defaultUsage()
		fmt.Fprint(flag.CommandLine.Output(), "Usage of Mydumper:\n")
		execMydumper("--help")
	}
	flag.Parse()
	driver, err := file_uploader.NewAWSS3FileUploaderDriver(*awsAccessKeyId, *awsSecretAccessKey, *awsBucketRegion,
		*awsBucketName, *workDir, *remoteDir)
	if err != nil {
		log.Fatalf("AWS S3 Driver create failure, err: %#v", err)
		os.Exit(-1)
	}
	uploader := file_uploader.NewFileUploader("", *workerNum, *sliceSize, driver)
	execMydumper(*mydumperArgs)
	uploader.WaitAndClose()
}

func execMydumper(args string) {
	cmd := exec.Command(*mydumper, args)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err := cmd.Start()
	if err != nil {
		log.Errorf("Exec Mydumper failure %s", err.Error())
		os.Exit(-1)
	}
	err = cmd.Wait()
	if err != nil {
		log.Errorf("Exec Mydumper failure %s", err.Error())
		os.Exit(-1)
	}
}
