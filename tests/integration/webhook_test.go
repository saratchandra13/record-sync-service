package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
	"github.com/saratchandra/record-sync-service/internal/sync"
	"github.com/saratchandra/record-sync-service/internal/system"
	"github.com/saratchandra/record-sync-service/internal/transform"
	"github.com/stretchr/testify/assert"
)

func TestSalesforceWebhook(t *testing.T) {
	ctx := context.Background()

	// Initialize transformer registry
	transformerRegistry := transform.NewTransformerRegistry()
	transformerRegistry.RegisterTransformer("SALESFORCE", "INTERNAL", &transform.SalesforceToInternalTransformer{})
	transformerRegistry.RegisterTransformer("INTERNAL", "SALESFORCE", &transform.InternalToSalesforceTransformer{})

	// Create sync engine
	engine := sync.NewSyncEngine(transformerRegistry, []sync.SyncRule{
		{
			RecordType:    "customer",
			Operations:    []models.SyncOperation{models.Create, models.Update},
			Direction:     "bidirectional",
			SourceSystems: []models.SyncSource{models.SourceSalesforce},
			TargetSystems: []models.SyncSource{models.SourceInternal},
			Priority:      1,
			Enabled:       true,
		},
	}, 5)

	// Track sync events
	var syncEvents []models.SyncEvent
	onChange := func(event models.SyncEvent) {
		syncEvents = append(syncEvents, event)
		if err := engine.EnqueueEvent(event); err != nil {
			t.Errorf("Failed to enqueue event: %v", err)
		}
	}

	// Initialize services
	internalService := system.NewInternalSystemService(onChange, "")
	salesforceService := system.NewSalesforceSystemService(nil, "http://localhost:8081", "mock-api-key")

	// Create and register handlers
	internalHandler := system.NewInternalSystemHandler(internalService, transformerRegistry)
	salesforceHandler := system.NewSalesforceSystemHandler(salesforceService, transformerRegistry)

	engine.RegisterHandler(models.SourceInternal, internalHandler)
	engine.RegisterHandler(models.SourceSalesforce, salesforceHandler)

	// Start the engine
	if err := engine.Start(ctx); err != nil {
		t.Fatalf("Failed to start sync engine: %v", err)
	}

	// Create webhook handler
	handler := system.NewWebhookHandler(salesforceService, onChange)

	// Test cases
	tests := []struct {
		name           string
		payload        system.SalesforceWebhookPayload
		expectedStatus int
	}{
		{
			name: "Create Contact",
			payload: system.SalesforceWebhookPayload{
				EventType: "CREATE",
				ObjectID:  "sf-contact-1",
				Data: models.SalesforceContact{
					Id:        "sf-contact-1",
					FirstName: "John",
					LastName:  "Doe",
					Email:     "john.doe@example.com",
					Metadata: models.SyncMetadata{
						SyncID:    "webhook-create-1",
						Source:    models.SourceSalesforce,
						CreatedAt: time.Now(),
						UpdatedAt: time.Now(),
					},
				},
				Timestamp: time.Now(),
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "Update Contact",
			payload: system.SalesforceWebhookPayload{
				EventType: "UPDATE",
				ObjectID:  "sf-contact-1",
				Data: models.SalesforceContact{
					Id:        "sf-contact-1",
					FirstName: "John",
					LastName:  "Doe Updated",
					Email:     "john.doe.updated@example.com",
					Metadata: models.SyncMetadata{
						SyncID:    "webhook-update-1",
						Source:    models.SourceSalesforce,
						CreatedAt: time.Now(),
						UpdatedAt: time.Now(),
					},
				},
				Timestamp: time.Now(),
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Clear sync events from previous test
			syncEvents = nil

			// Create request
			payloadBytes, err := json.Marshal(tc.payload)
			assert.NoError(t, err)

			req := httptest.NewRequest(http.MethodPost, "/webhook/salesforce", bytes.NewBuffer(payloadBytes))
			rec := httptest.NewRecorder()

			// Handle request
			handler.HandleSalesforceWebhook(rec, req)

			// Check response
			assert.Equal(t, tc.expectedStatus, rec.Code)

			if tc.expectedStatus == http.StatusOK {
				// Wait a bit for sync to complete
				time.Sleep(time.Second)

				// Verify event was received and processed
				assert.NotEmpty(t, syncEvents)
				var salesforceEvent models.SyncEvent
				for _, event := range syncEvents {
					if event.Source == models.SourceSalesforce {
						salesforceEvent = event
						break
					}
				}
				assert.Equal(t, tc.payload.Data.Metadata.SyncID, salesforceEvent.SyncID)
				assert.Equal(t, models.SourceSalesforce, salesforceEvent.Source)

				// Verify data was synced to internal service
				internalCustomer, err := internalService.GetCustomer(ctx, tc.payload.Data.Id)
				assert.NoError(t, err)
				assert.Equal(t, tc.payload.Data.FirstName, internalCustomer.FirstName)
				assert.Equal(t, tc.payload.Data.LastName, internalCustomer.LastName)
				assert.Equal(t, tc.payload.Data.Email, internalCustomer.Email)
				assert.Equal(t, models.SourceInternal, internalCustomer.Metadata.Source)
			}
		})
	}
}
