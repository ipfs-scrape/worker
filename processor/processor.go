package processor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/ipfs-scrape/worker/backend"
	"github.com/ipfs-scrape/worker/ipfs"
	"github.com/ipfs-scrape/worker/queue"
	"github.com/sirupsen/logrus"
)

// IPFSProcessor is a struct that implements the Processor interface for IPFS.
type IPFSProcessor struct {
	queue       queue.Queue
	backend     backend.Backend
	ipfsGateway string
	logger      *logrus.Entry
	concurrency int

	stopCh chan struct{}
	doneCh chan struct{}

	pollTime time.Duration
}

// NewIPFSProcessor creates a new IPFSProcessor instance with the specified queue, IPFS gateway, and ticker time.
func NewIPFSProcessor(q queue.Queue, b backend.Backend, ipfsGateway string, pollTime time.Duration, concurrency int) *IPFSProcessor {
	return &IPFSProcessor{
		queue:       q,
		backend:     b,
		ipfsGateway: ipfsGateway,
		logger:      logrus.WithField("component", "IPFSProcessor"),
		pollTime:    pollTime,
		concurrency: concurrency,
	}
}

// Run starts the IPFSProcessor and processes items from the queue.
func (p *IPFSProcessor) Run() {
	// Create channels for receiving items from the queue and stopping the processor
	itemCh := make(chan queue.QueueItem)
	p.stopCh = make(chan struct{})
	p.doneCh = make(chan struct{})

	// Start a ticker to periodically check the queue for new items
	ticker := time.NewTicker(p.pollTime)

	// Start a goroutine to receive items from the queue and send them to the item channel
	for i := 0; i < p.concurrency; i++ {
		p.logger.Infof("started processor #%d", i)

		go func() {
			for item := range itemCh {
				p.logger.WithField("ID", item.ID).Info("Processing item")

				err := p.Work(item)
				if err != nil {
					p.logger.WithError(err).Error("Failed to process item")
				} else {
					err = p.queue.Done(item)
					if err != nil {
						p.logger.WithError(err).Error("Failed to mark item as done")
					} else {
						p.logger.WithField("ID", item.ID).Info("Item processed and marked as done")
					}
				}
			}
		}()
	}

	// Start a goroutine to process items from the item channel
	go func() {
		for {
			select {
			case <-ticker.C:
				p.logger.Info("Checking queue for new items")
				item, err := p.queue.GetNextItem()
				if err != nil {
					p.logger.WithError(err).Info("did not get an item from queue")
					continue
				}

				itemCh <- item

			case <-p.stopCh:
				p.logger.Info("Stopping IPFSProcessor")
				close(p.doneCh)
				return
			}
		}
	}()
}
func (p *IPFSProcessor) Work(item queue.QueueItem) error {
	failures := []string{}
	// We assume the Data Payload for this type of work is the json array of CIDs
	if cidsIntf, ok := item.Data["cids"]; ok {
		cids := cidsIntf.([]string)
		for _, cid := range cids {
			metadata, err := p.FetchCID(cid)
			if err != nil {
				p.logger.WithError(err).WithField("CID", cid).Error("Failed to fetch CID metadata")
				failures = append(failures, cid)
				continue
			}

			err = p.backend.Create(metadata)
			if err != nil {
				p.logger.WithError(err).WithField("CID", cid).Error("Failed to create CID in backend")
				failures = append(failures, cid)
				continue
			}

		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("failed to fetch at least CID metadata for:\n%s", strings.Join(failures, "\n"))
	}

	return nil
}

// Wait waits for the IPFSProcessor to stop.
func (p *IPFSProcessor) Wait() {
	<-p.doneCh
}

// Stop stops the IPFSProcessor.
func (p *IPFSProcessor) Stop() {
	close(p.stopCh)
}

// FetchCID fetches the content of the specified CID from the IPFS gateway and returns it as an ipfs.Metadata struct.
func (p *IPFSProcessor) FetchCID(cid string) (ipfs.Metadata, error) {
	// Verify that the IPFS gateway URL is well-formed
	ipfsURL, err := url.Parse(p.ipfsGateway)
	if err != nil {
		return ipfs.Metadata{}, fmt.Errorf("invalid IPFS gateway URL: %s", err)
	}
	if ipfsURL.Scheme == "" {
		return ipfs.Metadata{}, fmt.Errorf("missing protocol in IPFS gateway URL")
	}

	// Construct the URL for the specified CID
	cidURL := fmt.Sprintf("%s/%s", p.ipfsGateway, cid)
	p.logger.WithField("url", cidURL).Info("Fetching CID")

	// Send an HTTP GET request to the CID URL
	resp, err := http.Get(cidURL)
	if err != nil {
		return ipfs.Metadata{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return ipfs.Metadata{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var metadata ipfs.Metadata
	err = json.NewDecoder(resp.Body).Decode(&metadata)
	if err != nil {
		return ipfs.Metadata{}, err
	}

	metadata.ID = cid
	return metadata, nil
}
