package system

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
)

// SalesforceWebhookPayload represents the payload sent by Salesforce webhooks
type SalesforceWebhookPayload struct {
	EventType string                 `json:"type"`    // CREATE, UPDATE, DELETE
	ObjectID  string                 `json:"id"`      // Salesforce record ID
	Data      models.SalesforceContact `json:"data"`   // The actual record data
	Timestamp time.Time             `json:"timestamp"`
}

// WebhookHandler handles incoming webhooks from Salesforce
type WebhookHandler struct {
	service  SalesforceSystemService
	onChange func(models.SyncEvent)
}

// NewWebhookHandler creates a new webhook handler
func NewWebhookHandler(service SalesforceSystemService, onChange func(models.SyncEvent)) *WebhookHandler {
	return &WebhookHandler{
		service:  service,
		onChange: onChange,
	}
}

// HandleSalesforceWebhook processes incoming webhooks from Salesforce
func (h *WebhookHandler) HandleSalesforceWebhook(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload SalesforceWebhookPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid payload", http.StatusBadRequest)
		return
	}

	// Determine the operation type
	var operation models.SyncOperation
	switch payload.EventType {
	case "CREATE":
		operation = models.Create
	case "UPDATE":
		operation = models.Update
	case "DELETE":
		operation = models.Delete
	default:
		http.Error(w, "Invalid event type", http.StatusBadRequest)
		return
	}

	// Create a sync event
	event := models.SyncEvent{
		Operation:  operation,
		Source:     models.SourceSalesforce,
		SyncID:     payload.Data.Metadata.SyncID,
		RecordType: "customer",
		Record:     payload.Data,
		Timestamp:  payload.Timestamp,
	}

	// Notify the sync engine
	if h.onChange != nil {
		h.onChange(event)
	}

	w.WriteHeader(http.StatusOK)
}
