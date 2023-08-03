# IPFS Scrape Worker

This is a worker that scrapes IPFS content and stores it in a DynamoDB database.

## Prerequisites

- Go 1.16 or later
- Docker
- Docker Compose

## Installation

1. Clone the repository:

```
git clone https://github.com/ipfs-scrape/worker.git

```

The worker will scrape IPFS content and store it in the specified DynamoDB table.

## Configuration

The worker can be configured using environment variables:

- `DYNAMODB_NAME`: the name of the DynamoDB table to use (required).
- `IPFS_GATEWAY_URL`: the URL of the IPFS gateway to use (default: `https://ipfs.io/ipfs`).
- `IPFS_SCRAPE_INTERVAL`: the interval between IPFS scrapes (default: `5s`).
- `IPFS_SCRAPE_CONCURRENCY`: the number of concurrent IPFS scrapes (default: `5`).

## License

This code is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
