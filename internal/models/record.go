package models

import (
	"time"
)

// SyncOperation defines the type of operation being performed
type SyncOperation string

const (
	Create SyncOperation = "CREATE"
	Update SyncOperation = "UPDATE"
	Delete SyncOperation = "DELETE"
)

// SyncSource defines the source system for a sync operation
type SyncSource string

const (
	SourceInternal   SyncSource = "INTERNAL"
	SourceSalesforce SyncSource = "SALESFORCE"
)

// SyncMetadata contains tracking information for sync operations
type SyncMetadata struct {
	SyncID    string     `json:"syncId"`    // Unique ID for this sync operation
	Source    SyncSource `json:"source"`    // Source system of this record
	CreatedAt time.Time  `json:"createdAt"` // When this sync metadata was created
	UpdatedAt time.Time  `json:"updatedAt"` // When this sync metadata was last updated
}

// SyncEvent represents a change that needs to be synchronized
type SyncEvent struct {
	Operation  SyncOperation `json:"operation"`  // Type of operation (create, update, delete)
	Source     SyncSource    `json:"source"`     // Source system that originated the change
	SyncID     string        `json:"syncId"`     // Unique ID for tracking this sync event
	RecordType string        `json:"recordType"` // Type of record being synchronized
	Record     interface{}   `json:"record"`     // The actual record data
	Timestamp  time.Time     `json:"timestamp"`  // When the event occurred
}

// InternalCustomer represents a customer record in the internal system
type InternalCustomer struct {
	ID             string       `json:"id"`
	FirstName      string       `json:"firstName"`
	LastName       string       `json:"lastName"`
	Email          string       `json:"email"`
	Phone          string       `json:"phone"`
	BillingAddress string       `json:"billingAddress"`
	Status         string       `json:"status"`
	CreatedDate    time.Time    `json:"createdDate"`
	UpdatedDate    time.Time    `json:"updatedDate"`
	Metadata       SyncMetadata `json:"metadata"`
}

// SalesforceContact represents a contact record in Salesforce
type SalesforceContact struct {
	Id             string       `json:"id"`
	FirstName      string       `json:"firstName"`
	LastName       string       `json:"lastName"`
	Email          string       `json:"email"`
	Phone          string       `json:"phone"`
	MailingAddress string       `json:"mailingAddress"`
	Status         string       `json:"status"`
	CreatedDate    time.Time    `json:"createdDate"`
	LastModified   time.Time    `json:"lastModifiedDate"`
	IsDeleted      bool         `json:"isDeleted"`
	OwnerID        string       `json:"ownerId"`
	Metadata       SyncMetadata `json:"metadata"`
}
