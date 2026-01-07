package common

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Event struct {
	ID        string            `json:"id"`
	Type      string            `json:"type"`
	Key       string            `json:"key"` // used for ordering/partitioning/routing
	Attempts  int               `json:"attempts"`
	CreatedAt time.Time         `json:"created_at"`
	Payload   map[string]string `json:"payload"`
}

func NewEvent(eventType, key string, payload map[string]string) Event {
	return Event{
		ID:        uuid.NewString(),
		Type:      eventType,
		Key:       key,
		Attempts:  0,
		CreatedAt: time.Now().UTC(),
		Payload:   payload,
	}
}

func Encode(e Event) ([]byte, error) { return json.Marshal(e) }
func Decode(b []byte) (Event, error) {
	var e Event
	err := json.Unmarshal(b, &e)
	return e, err
}
