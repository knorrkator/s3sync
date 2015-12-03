package main

import (
    "fmt"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

func main() {
    // Create an S3 service object in the "eu-west-1" region
    // Note that you can also configure your region globally by
    // exporting the AWS_REGION environment variable
    svc := s3.New(session.New(), &aws.Config{Region: aws.String("eu-west-1")})

    params := &s3.ListObjectsInput{
    	Bucket:       aws.String("knorrtestx1"), // Required
    	MaxKeys:      aws.Int64(3),
    }

    pageNum := 0
    err := svc.ListObjectsPages(params, func(page *s3.ListObjectsOutput, lastPage bool) bool {
      pageNum++
      fmt.Println(page)
      return pageNum <= 3
    })

    if err != nil {
    	// Print the error, cast err to awserr.Error to get the Code and
    	// Message from an error.
    	fmt.Println(err.Error())
    	return
    }
}
