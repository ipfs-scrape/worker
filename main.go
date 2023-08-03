package main

import (
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/ipfs-scrape/worker/backend"
	"github.com/ipfs-scrape/worker/processor"
	"github.com/ipfs-scrape/worker/queue"
	"github.com/sirupsen/logrus"
)

func main() {
	dynamodbName, ok := os.LookupEnv("IPFS_DYNAMODB_NAME")
	if !ok {
		logrus.Fatal("IPFS_DYNAMODB_NAME environment variable not set")
	}

	ipfsGatewayURL := os.Getenv("IPFS_GATEWAY_URL")
	if ipfsGatewayURL == "" {
		ipfsGatewayURL = "https://ipfs.io/ipfs"
	}

	ipfsScrapeIntervalStr := os.Getenv("IPFS_SCRAPE_INTERVAL")
	ipfsScrapeInterval, err := time.ParseDuration(ipfsScrapeIntervalStr)
	if err != nil {
		logrus.Warnf("Failed to parse IPFS_SCRAPE_INTERVAL: %s\n %v\n", ipfsScrapeIntervalStr, err)
		ipfsScrapeInterval = 5 * time.Second
	}

	ipfsScrapeConcurrencyStr := os.Getenv("IPFS_SCRAPE_CONCURRENCY")
	ipfsScrapeConcurrency := 1
	if ipfsScrapeConcurrencyStr != "" {
		var err error
		ipfsScrapeConcurrency, err = strconv.Atoi(ipfsScrapeConcurrencyStr)
		if err != nil {
			logrus.Warnf("Failed to convert IPFS_SCRAPE_CONCURRENCY: %s\n to int: %v", ipfsScrapeConcurrencyStr, err)
			ipfsScrapeConcurrency = 5
		}
	}

	// Use the IPFS_DYNAMODB_NAME environment variable
	logrus.Infof("IPFS_DYNAMODB_NAME: %s\n", dynamodbName)
	logrus.Infof("IPFS_GATEWAY_URL: %s\n", ipfsGatewayURL)
	logrus.Infof("IPFS_SCRAPE_INTERVAL: %s\n", ipfsScrapeInterval)
	logrus.Infof("IPFS_SCRAPE_CONCURRENCY: %d\n", ipfsScrapeConcurrency)

	// Create a new session and DynamoDB client
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:   aws.String("us-east-1"),
			Endpoint: aws.String("http://localhost:8000"),
		},
	})

	if err != nil {
		logrus.Fatal(err)
	}

	svc := dynamodb.New(sess)

	// create an instance of our DynamoDBQueue
	dynamoDBQueue, err := queue.NewDynamoDBQueue(dynamodbName, "ipfs", svc)
	if err != nil {
		logrus.Fatal(err)
	}

	// create an instance of out DynamoDBBackend
	dynamoDBBackend, err := backend.NewDynamoDBBackend(dynamodbName, svc)
	if err != nil {
		logrus.Fatal(err)
	}
	// create an instance of our IPFSProcessor
	ipfsProcessor := processor.NewIPFSProcessor(dynamoDBQueue, dynamoDBBackend, ipfsGatewayURL, ipfsScrapeInterval, ipfsScrapeConcurrency)

	// non-blocking start
	ipfsProcessor.Run()
	// block
	ipfsProcessor.Wait()

}
