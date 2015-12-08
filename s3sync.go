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
func list_elements(svc *s3.S3, bucket_name string, chan_s3_chunks chan *s3.ListObjectsOutput) {
  defer wg.Done()
  defer close(chan_s3_chunks)

  params := &s3.ListObjectsInput{
    Bucket:       aws.String(bucket_name), // Required
    MaxKeys:      aws.Int64(S3_MAX_KEYS),
  }

  err := svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
    chan_s3_chunks <- page
    return !lastPage
  })

  if err != nil {
    // Print the error, cast err to awserr.Error to get the Code and
    // Message from an error.
    fmt.Println(err.Error())
    return
  }

  //fmt.Printf("%s done\n", bucket_name)
}

// Need to convert Chungs of Keys, returned from S3-API into lists
func extract_contents(chan_s3_chunks chan *s3.ListObjectsOutput, output_chan chan *s3.Object) {
  defer wg.Done()
  for {
    s3_list_chunk, ok := <- chan_s3_chunks
    if !ok {
      // Channel is closed bei producer
      break
    }

    for _, s3_key := range s3_list_chunk.Contents {
      //fmt.Printf("Key: %v\n", s3_key)
      output_chan <- s3_key
    }

    close(output_chan)

  }
}

// Detects difference between Source- and Destination-Bucket
func find_missing(s3_contents_src, s3_contents_dest chan *s3.Object) {
  defer wg.Done()
  var src, dest *s3.Object

  src = <- s3_contents_src
  dest = <- s3_contents_dest

  for {

    if src == nil && dest == nil {
      // Case A: no elements left in both Buckets
      break

    } else if src != nil && dest != nil {
      // Case B: got an element in both Buckets

      if *src.Key < *dest.Key {
        // src-element is ahead of destination-element. Src-element is missing in Destination-Bucket
        src = <- s3_contents_src

      } else if *src.Key > *dest.Key {
        // src-element is beyond destination-element. Destination-element exists only on Destination-Bucket
        dest = <- s3_contents_dest

      } else {  // src.Key == dest.Key
        // Src and Dest are equal
        src = <- s3_contents_src
        dest = <- s3_contents_dest

      }

    } else if dest == nil {
      // Case C: no element left in Destination-Bucket but in Source-Bucket
      src = <- s3_contents_src

    } else if src == nil {
      // Case D: no elements left in Source-Bucket but in Destination-Bucket
      dest = <- s3_contents_dest

    } else {
      // This means not Case A or not Case B or not Case C or not Case D
      fmt.Printf("This case shouldn't appear. However it did. Src: %v Dest: %v", src, dest)
      // TODO: raise an error. Stop execution
      break

    }
  }
}

func main() {
    // Create an S3 service object in the "eu-west-1" region
    // Note that you can also configure your region globally by
    // exporting the AWS_REGION environment variable
    svc := s3.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})

    wg.Add(5)

    chan_s3_chunks_src := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)
    chan_s3_chunks_dest := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)
    s3_contents_src := make(chan *s3.Object, CHAN_BUFFER_SIZE)
    s3_contents_dest := make(chan *s3.Object, CHAN_BUFFER_SIZE)

    go list_elements(svc, "knorrtestx1", chan_s3_chunks_src)
    go list_elements(svc, "knorrtestx2", chan_s3_chunks_dest)
    go extract_contents(chan_s3_chunks_src, s3_contents_src)
    go extract_contents(chan_s3_chunks_dest, s3_contents_dest)
    go find_missing(s3_contents_src, s3_contents_dest)

    wg.Wait()

}
