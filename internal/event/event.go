package event

import (
	"encoding/json"
	"fmt"
)

// RecentChange is a typed projection of a Wikimedia EventStreams
// "recentchange" event. Only the fields the consumer actually needs
// (for ClickHouse storage and PostgreSQL state upserts) are extracted;
// Raw keeps the original payload for archival storage.
type RecentChange struct {
	Raw json.RawMessage `json:"-"`

	ID        int64  `json:"id"`
	Type      string `json:"type"`
	Namespace int    `json:"namespace"`
	Title     string `json:"title"`
	Comment   string `json:"comment"`
	Timestamp int64  `json:"timestamp"`
	User      string `json:"user"`
	Bot       bool   `json:"bot"`
	Wiki      string `json:"wiki"`
	Revision  struct {
		Old int64 `json:"old"`
		New int64 `json:"new"`
	} `json:"revision"`
}

// Parse decodes and validates a raw recentchange payload. data is copied
// into the returned event's Raw field, so the caller's buffer may be reused.
func Parse(data []byte) (*RecentChange, error) {
	var rc RecentChange
	if err := json.Unmarshal(data, &rc); err != nil {
		return nil, fmt.Errorf("unmarshal recentchange event: %w", err)
	}

	rc.Raw = append(json.RawMessage(nil), data...)

	if err := rc.validate(); err != nil {
		return nil, err
	}

	return &rc, nil
}

func (rc *RecentChange) validate() error {
	if rc.ID <= 0 {
		return fmt.Errorf("missing or invalid required field: id")
	}
	if rc.Type == "" {
		return fmt.Errorf("missing required field: type")
	}
	if rc.Wiki == "" {
		return fmt.Errorf("missing required field: wiki")
	}
	if rc.Title == "" {
		return fmt.Errorf("missing required field: title")
	}
	if rc.Timestamp <= 0 {
		return fmt.Errorf("missing or invalid required field: timestamp")
	}
	return nil
}
