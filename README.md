# IPFS Scrape Worker

This is a worker that scrapes IPFS hashes and stores the results in a DynamoDB table.

## What does it do right now?

Polls a queue in DynamoDB to execute a process.
Right now the only process is the `IPFSProcessor` which performs the IPFS scrape and stores the Metadata in DynamoDB

```
type Metadata struct {
	ID          string `json:"ID"`
	CID         string `json:"CID"`
	Image       string `json:"Image"`
	Description string `json:"Description"`
	Name        string `json:"Name"`
}
```

## Configuration

The worker is configured using the following environment variables:

- `IPFS_DYNAMODB_NAME`: The name of the DynamoDB table to use.
- `IPFS_GATEWAY_URL`: The URL of the IPFS gateway to use. Defaults to `https://ipfs.io/ipfs`.
- `IPFS_SCRAPE_INTERVAL`: The interval at which to scrape IPFS hashes. Defaults to `5s`.
- `IPFS_SCRAPE_CONCURRENCY`: The number of concurrent scrapes to perform. Defaults to `1`.

## Usage

```
go run main.go
```

## Dependencies

The worker uses the following dependencies:

- `github.com/aws/aws-sdk-go/aws/session`
- `github.com/aws/aws-sdk-go/service/dynamodb`
- `github.com/ipfs-scrape/worker/backend`
- `github.com/ipfs-scrape/worker/processor`
- `github.com/ipfs-scrape/worker/queue`
- `github.com/sirupsen/logrus`

## License

This code is licensed under the MIT License. See the `LICENSE` file for details.
