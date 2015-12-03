package main

import (
    "fmt"
    "sync"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

const (
  // How many Keys AWS-S3-Api will return in a single request. Note that S3-API
  // has a hard limit of 1000
  // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html
  S3_MAX_KEYS = 1000
  // How many S3-Result-Sets are buffered until the producer blocks
  CHAN_BUFFER_SIZE = 1000
)

var wg sync.WaitGroup

// Finds every element in a S3-Bucket
// Is meant to run in parallel. Puts resultsets into channel *s3.ListObjectsOutput
// Closes this channel when done
// First parameter is an *s3.S3 Object (http://docs.aws.amazon.com/sdk-for-go/api/service/s3/S3.html)
// Second is the S3-Bucket-Name (Note not the S3-Path like s3://bucket_name)
// Third is the result channel of type *s3.ListObjectsOutput
func list_elements(svc *s3.S3, bucket_name string, result_chan chan *s3.ListObjectsOutput) {
  defer wg.Done()
  defer close(result_chan)

  params := &s3.ListObjectsInput{
    Bucket:       aws.String(bucket_name), // Required
    MaxKeys:      aws.Int64(S3_MAX_KEYS),
  }

  err := svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
    result_chan <- page
    return !lastPage
  })

  if err != nil {
    // Print the error, cast err to awserr.Error to get the Code and
    // Message from an error.
    fmt.Println(err.Error())
    return
  }

  fmt.Printf("%s done\n", bucket_name)
}

func main() {
    // Create an S3 service object in the "eu-west-1" region
    // Note that you can also configure your region globally by
    // exporting the AWS_REGION environment variable
    svc := s3.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})

    wg.Add(2)

    result_chan_src := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)
    result_chan_dest := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)

    go list_elements(svc, "knorrtestx1", result_chan_src)
    go list_elements(svc, "knorrtestx2", result_chan_dest)

    wg.Wait()

}
