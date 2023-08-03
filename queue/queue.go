/*
Lazily drop all this in here.
Refactoring to a more general package can be later or never :)
*/

package queue

import "time"

type Queue interface {
	// AddItem adds an item to the queue.
	AddItem(item QueueItem) error
	// GetNextItem returns the next item in the queue.
	GetNextItem() (QueueItem, error)
	// Done( QueueItem ) marks the item as done.
	Done(item QueueItem) error
}

// QueueItem represents an item in the DynamoDB queue.
type QueueItem struct {
	ID        string         `json:"id"`
	Data      map[string]any `json:"data"`
	CreatedAt int64          `json:"created_at"`
}

// NewQueueItem creates a new QueueItem with the given ID and data.
func NewQueueItem(id string, data map[string]any) QueueItem {
	return QueueItem{
		ID:        id,
		Data:      data,
		CreatedAt: time.Now().Unix(),
	}
}
