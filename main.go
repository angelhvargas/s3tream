package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	pb "gopkg.in/cheggaaa/pb.v1"
)

type Aws struct {
	mu                    sync.Mutex
	AWS_ACCESS_KEY_ID     string
	AWS_SECRET_ACCESS_KEY string
	AWS_REGION            string
}

type progressWriter struct {
	writer  io.WriterAt
	size    int64
	bar     *pb.ProgressBar
	display bool
}

func (pw *progressWriter) init(s3ObjectSize int64) {
	if pw.display {
		pw.bar = pb.StartNew(int(s3ObjectSize))
		pw.bar.ShowSpeed = true
		pw.bar.Format("[=>_]")
		pw.bar.SetUnits(pb.U_BYTES_DEC)
	}
}

func (pw *progressWriter) WriteAt(p []byte, off int64) (int, error) {
	if pw.display {
		pw.bar.Add64(int64(len(p)))
	}
	return pw.writer.WriteAt(p, off)
}

func (pw *progressWriter) finish() {
	if pw.display {
		pw.bar.Finish()
	}
}

func newAws() *Aws {
	return &Aws{
		AWS_ACCESS_KEY_ID:     os.Getenv("AWS_ACCESS_KEY_ID"),
		AWS_SECRET_ACCESS_KEY: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		AWS_REGION:            os.Getenv("AWS_REGION"),
	}
}

func (a *Aws) DownloadS3ParallelByParts(bucket, item, path string, displayProgressBar bool) {

	file, err := os.Create(filepath.Join(path, item))
	if err != nil {
		fmt.Printf("Error in downloading from file: %v \n", err)
		os.Exit(1)
	}

	defer file.Close()

	sess, _ := session.NewSession(&aws.Config{
		Region:      aws.String(a.AWS_REGION),
		Credentials: credentials.NewSharedCredentials("", "default"),
	})

	// Get the object size
	s3ObjectSize := a.GetS3ObjectSize(bucket, item)

	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // fetch 64MB per frame/part
		d.Concurrency = 6
	})

	writer := &progressWriter{writer: file, size: s3ObjectSize}
	writer.display = displayProgressBar
	writer.init(s3ObjectSize)

	numBytes, err := downloader.Download(writer,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(item),
		})
	if err != nil {
		fmt.Printf("Error in downloading from file: %v \n", err)
		os.Exit(1)
	}
	writer.finish()
	fmt.Println("Download completed", file.Name(), numBytes, "bytes")
}

func (a *Aws) GetS3ObjectSize(bucket, item string) int64 {
	sess, _ := session.NewSession(&aws.Config{
		Region: aws.String(a.AWS_REGION), Credentials: credentials.NewSharedCredentials("", "default")},
	)

	svc := s3.New(sess)
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(item),
	}

	result, err := svc.HeadObject(input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			fmt.Println("Error getting size of file", aerr)
		} else {
			fmt.Println("Error getting size of file", err)
		}
		os.Exit(1)
	}
	return *result.ContentLength
}

func main() {

	a := newAws()

	if len(os.Args) < 3 {
		fmt.Printf(
			`s3_downloader
-------------
Note: the AWS Region must be set via env: AWS_REGION
Usage:
This program needs the following values as positional arguments: [BUCKET] [ITEM] [FILE_PATH]
`)
		os.Exit(0)
	}

	bucket := os.Args[1]
	if bucket == "" {
		fmt.Println("Bucket positional argument is not present")
		os.Exit(1)
	}

	item := os.Args[2]
	if item == "" {
		fmt.Println("Item positional argument is not present")
		os.Exit(1)
	}

	path := os.Args[3]
	if path == "" {
		fmt.Println("Path positional argument is not present")
		os.Exit(1)
	}

	a.DownloadS3ParallelByParts(bucket, item, path, true)
	os.Exit(0)

}
