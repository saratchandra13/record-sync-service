package models

import (
	"time"
)

// SyncSource identifies the system that originated a change
type SyncSource string

const (
	SourceInternal SyncSource = "INTERNAL"
	SourceExternal SyncSource = "EXTERNAL"
)

// InternalRecord represents a record in the internal system (System A)
type InternalRecord struct {
	ID          string                 `json:"id"`
	RecordType  string                 `json:"record_type"`
	Attributes  map[string]interface{} `json:"attributes"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
	SyncID      string                 `json:"sync_id,omitempty"` // Tracks sync operations to prevent cycles
	SyncSource  SyncSource             `json:"sync_source,omitempty"`
	ExternalID  string                 `json:"external_id,omitempty"` // Reference to corresponding record in System B
}

// ExternalRecord represents a record in the external system (System B)
type ExternalRecord struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Properties map[string]interface{} `json:"properties"`
	Metadata   struct {
		Created  time.Time  `json:"created"`
		Modified time.Time  `json:"modified"`
		SyncID   string     `json:"sync_id,omitempty"`
		Source   SyncSource `json:"source,omitempty"`
		InternalRef string  `json:"internal_ref,omitempty"` // Reference to corresponding record in System A
	} `json:"metadata"`
}

// SyncOperation represents a sync action to be performed
type SyncOperation string

const (
	Create SyncOperation = "CREATE"
	Update SyncOperation = "UPDATE"
	Delete SyncOperation = "DELETE"
)

// SyncEvent represents a change event that needs to be synchronized
type SyncEvent struct {
	Operation   SyncOperation `json:"operation"`
	Source      SyncSource    `json:"source"`
	SyncID      string        `json:"sync_id"`
	RecordType  string        `json:"record_type"`
	Record      interface{}   `json:"record"` // Can be either InternalRecord or ExternalRecord
	Timestamp   time.Time     `json:"timestamp"`
}
