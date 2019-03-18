## MydumperUploader

MydumperUploader can upload the data exported by `mydumper` to AWS S3, and support breakpoints.

## How to use

```
Usage of mydumper_uploader:
  -awsaccesskeyid string
    	AWS S3 Access Key ID
  -awsbucketname string
    	AWS S3 Bucket Name
  -awsbucketregion string
    	AWS S3 Bucket Region
  -awssecretaccesskey string
    	AWS S3 Secret Access Key
  -mydumper string
    	MyDumper executable file path (default "mydumper")
  -mydumperargs string
    	MyDumper args (default "--help")
  -remotedir string
    	Remote Dir
  -restore
    	Restore from unfinished work.
  -slice int
    	Upload File Slice Size(byte) (default 16777216)
  -workdir string
    	Work Dir
  -worker int
    	Worker Thread Number (default 1)
```

## Example

If we want to back up data in the database(eg. mysql, tidb) to AWS S3 Storage, we can execute the following command. 
Note that the parameter of mydumper `-o` should be equal with the value of `-workdir`:

```
$ mydumper_uploader -awsaccesskeyid AKXXX -awsbucketname xxx-bucket \
-awsbucketregion cn-northwest-1 -awssecretaccesskey xxxx \
-mydumper /bin/mydumper \
-mydumperargs "-B test -o /home/backup/dump_data1 -u root -h 127.0.0.1 -p passxxxx" \
-workdir /home/backup/dump_data1 -remotedir tidb_data/dump/ -worker 4 -slice 16777216 \
```

If the program crashed by some reasons, we can execute the following command to restore the upload process. 
Note that `mydumper` does not support breakpoint, therefore the upload process can only be resumed after the `mydumper` completed.
The parameters `-awsaccesskeyid`, `-awsbucketname`, `-awsbucketregion`, `-awssecretaccesskey`, `-workdir`, `-remotedir`, 
`-slice` should be equal with those of the previous value:

```
$ mydumper_uploader -awsaccesskeyid AKXXX -awsbucketname xxx-bucket \
-awsbucketregion cn-northwest-1 -awssecretaccesskey xxxx \
-workdir /home/backup/dump_data1 -remotedir tidb_data/dump/ -worker 4 -slice 16777216 -restore
```

## License
Apache 2.0 license. See the [LICENSE](../LICENSE) file for details.

