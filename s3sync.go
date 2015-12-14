package main

import (
    "fmt"
    "sync"
    "os"

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
  // How many worker processes should do actual sync from Source-Bucket to Destination-Bucket
  CHAN_UPLOAD_WORKER = 5
)

var wg_list sync.WaitGroup
var wg_upload sync.WaitGroup

// Finds every element in a S3-Bucket
// Is meant to run in parallel. Puts resultsets into channel *s3.ListObjectsOutput
// Closes this channel when done
// First parameter is an *s3.S3 Object (http://docs.aws.amazon.com/sdk-for-go/api/service/s3/S3.html)
// Second is the S3-Bucket-Name (Note not the S3-Path like s3://bucket_name)
// Third is the result channel of type *s3.ListObjectsOutput
func receive_s3_chunks(svc *s3.S3, bucket_name string, chan_s3_chunks chan *s3.ListObjectsOutput) {
  defer wg_list.Done()
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

// Need to convert Chunks of Keys from S3-API into a single list of Keys
func extract_contents(chan_s3_chunks chan *s3.ListObjectsOutput, output_chan chan *s3.Object) {
  defer wg_list.Done()
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
  }

  close(output_chan)
}

// Detects difference between Source- and Destination-Bucket
// s3_contents_src: Input-Channel. Contains every element in Source-Bucket.
// s3_contents_dest: Input-Channel. Contains every element in Destination-Bucket
// s3_contents_4_upload: Output-Channel. Receives every Elemente from Source-Bucket
//                       that does not exist in Destination-Bucket. When done this
//                       Channel is closed.
func calculate_diff(s3_contents_src, s3_contents_dest, s3_contents_4_upload chan *s3.Object) {
  defer wg_list.Done()
  defer close(s3_contents_4_upload)
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
        fmt.Printf("Adding %v for upload\n", *src.Key)
        s3_contents_4_upload <- src
        // Continue with next element
        src = <- s3_contents_src

      } else if *src.Key > *dest.Key {
        // src-element is beyond destination-element. Destination-element exists only on Destination-Bucket

        // Continue with next element
        dest = <- s3_contents_dest

      } else {  // src.Key == dest.Key
        // Src and Dest are equal
        // Continue with next element
        src = <- s3_contents_src
        // Continue with next element
        dest = <- s3_contents_dest

      }

    } else if dest == nil {
      // Case C: no element left in Destination-Bucket but in Source-Bucket
      // Continue with next element

      fmt.Printf("Adding %v for upload\n", *src.Key)
      s3_contents_4_upload <- src

      src = <- s3_contents_src

    } else if src == nil {
      // Case D: no elements left in Source-Bucket but in Destination-Bucket
      // Continue with next element
      dest = <- s3_contents_dest

    } else {
      // This means not Case A or not Case B or not Case C or not Case D
      fmt.Printf("This case shouldn't appear. However it did. Src: %v Dest: %v", src, dest)
      // TODO: raise an error. Stop execution
      break

    }
  }
}

// Syncronizes elements received from Channel s3_contents_4_upload with given Destination-Bucket
// This function is able to run in parralel with same Channel
func sync_s3_elements(svc *s3.S3, bucket_name_src, bucket_name_dest string, s3_contents_4_upload chan *s3.Object, worker_id int) {
  defer wg_upload.Done()
  var elem *s3.Object
  var ok bool

  for {
    elem, ok = <- s3_contents_4_upload

    if ok && elem != nil {

      fmt.Printf("Sync from Worker %v: %v\n", worker_id, *elem.Key)
      params := &s3.CopyObjectInput{
      	Bucket:                         aws.String(bucket_name_dest), // Required
      	CopySource:                     aws.String(bucket_name_src + "/" +  *elem.Key),  // Required
      	Key:                            aws.String(*elem.Key),        // Required
      }
      _, err := svc.CopyObject(params)

      if err != nil {
      	// Print the error, cast err to awserr.Error to get the Code and
      	// Message from an error.
      	fmt.Println(err.Error())
      	return
      }

      //fmt.Println(resp)

    } else {
      break
    }
  }
  fmt.Printf("Sync done\n")
}

func main() {

    bucket_name_src := os.Args[1]
    bucket_name_dest := os.Args[2]

    // Create an S3 service object in the "eu-west-1" region
    // Note that you can also configure your region globally by
    // exporting the AWS_REGION environment variable
    svc := s3.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})

    // Channel that contains a list of S3-Chunks
    chan_s3_chunks_src := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)
    // Channel that contains a list of S3-Chunks
    chan_s3_chunks_dest := make(chan *s3.ListObjectsOutput, CHAN_BUFFER_SIZE)
    // List that contains a list of S3-Keys
    s3_contents_src := make(chan *s3.Object, CHAN_BUFFER_SIZE)
    // List that contains a list of S3-Keys
    s3_contents_dest := make(chan *s3.Object, CHAN_BUFFER_SIZE)
    // List that contains a list of S3-Keys that are missing in Destination => need upload to Destination
    s3_contents_4_upload := make(chan *s3.Object, CHAN_UPLOAD_WORKER)

    // Task 1: Read Chunks from S3-Buckets.
    // Chunks are a bunch of S3-Keys that the Bucket contains. A S3-Bucket retuns
    // a maximum of 1.000 keys per Chunk. Both Buckets are read asyncroneuous.
    go receive_s3_chunks(svc, bucket_name_src, chan_s3_chunks_src)
    wg_list.Add(1)
    go receive_s3_chunks(svc, bucket_name_dest, chan_s3_chunks_dest)
    wg_list.Add(1)
    // When the first Chunks arrive we extract their keys and write them into a list.
    // Both Buckets have their own list of S3-Keys
    go extract_contents(chan_s3_chunks_src, s3_contents_src)
    wg_list.Add(1)
    go extract_contents(chan_s3_chunks_dest, s3_contents_dest)
    wg_list.Add(1)
    // When the first elements arrive in the list of S3-Keys we calculate the difference
    // between both Bucket. This job starts as soon as there are elements in one of the lists
    go calculate_diff(s3_contents_src, s3_contents_dest, s3_contents_4_upload)
    wg_list.Add(1)

    // Upload files that are missing on Destination-Bucket
    fmt.Printf("Worker-Count: %v\n", CHAN_UPLOAD_WORKER)
    for i := 0; i < CHAN_UPLOAD_WORKER; i++ {
      fmt.Printf("Starting Worker %v\n", i)
      wg_upload.Add(1)
      go sync_s3_elements(svc, bucket_name_src, bucket_name_dest, s3_contents_4_upload, i)
    }

    wg_list.Wait()
    wg_upload.Wait()

}
