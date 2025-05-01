package integration

import (
	"context"
	"testing"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
	"github.com/saratchandra/record-sync-service/internal/sync"
	"github.com/saratchandra/record-sync-service/internal/system"
	"github.com/saratchandra/record-sync-service/internal/transform"
	"github.com/stretchr/testify/assert"
)

// TestOneWayCRMSync tests the one-way synchronization from internal system to CRM
func TestOneWayCRMSync(t *testing.T) {
	// Create a context with timeout for the test
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Initialize transformer registry
	transformerRegistry := transform.NewTransformerRegistry()

	// Register transformers
	transformerRegistry.RegisterTransformer(string(models.SourceSalesforce), string(models.SourceInternal), &transform.SalesforceToInternalTransformer{})
	transformerRegistry.RegisterTransformer(string(models.SourceInternal), string(models.SourceSalesforce), &transform.InternalToSalesforceTransformer{})

	// Create sync engine with one-way replication rule
	engine := sync.NewSyncEngine(transformerRegistry, []sync.SyncRule{
		{
			RecordType:    "customer",
			Operations:    []models.SyncOperation{models.Create, models.Update, models.Delete},
			Direction:     "oneway",
			SourceSystems: []models.SyncSource{models.SourceInternal}, // Only allow internal as source
			TargetSystems: []models.SyncSource{models.SourceSalesforce}, // Only sync to Salesforce
			Priority:      1,
			Enabled:       true,
		},
	}, 5)

	// Track sync events for verification
	var syncEvents []models.SyncEvent
	onChange := func(event models.SyncEvent) {
		syncEvents = append(syncEvents, event)
		if err := engine.EnqueueEvent(event); err != nil {
			t.Errorf("Failed to enqueue event: %v", err)
		}
	}

	
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

	// Test case 1: Create customer in internal system
	t.Run("Create Customer", func(t *testing.T) {
		customer := models.InternalCustomer{
			ID:        "test-customer-1",
			FirstName: "sarat",
			LastName:  "Chandra",
			Email:     "sarat.chandra@example.com",
			Metadata: models.SyncMetadata{
				SyncID:    "test-create-1",
				Source:    models.SourceInternal,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
		}

		// Create customer in internal system
		createdCustomer, err := internalService.CreateCustomer(ctx, customer)
		assert.NoError(t, err)
		assert.NotNil(t, createdCustomer)
		assert.Equal(t, customer.ID, createdCustomer.ID)

		// Wait for sync to complete
		time.Sleep(2 * time.Second)

		// Verify customer exists in Salesforce
		sfContact, err := salesforceService.GetContact(ctx, customer.ID)
		assert.NoError(t, err)
		assert.NotNil(t, sfContact)
		assert.Equal(t, customer.FirstName, sfContact.FirstName)
		assert.Equal(t, customer.LastName, sfContact.LastName)
		assert.Equal(t, customer.Email, sfContact.Email)
	})

	// Test case 2: Update customer in internal system
	t.Run("Update Customer", func(t *testing.T) {
		updatedCustomer := models.InternalCustomer{
			ID:        "test-customer-1",
			FirstName: "sarat",
			LastName:  "Chandra Updated",
			Email:     "sarat.chandra.updated@example.com",
			Metadata: models.SyncMetadata{
				SyncID:    "test-update-1",
				Source:    models.SourceInternal,
				CreatedAt: time.Now(),
				UpdatedAt: time.Now(),
			},
		}

		err := internalService.UpdateCustomer(ctx, updatedCustomer)
		assert.NoError(t, err)

		// Wait for sync to complete
		time.Sleep(2 * time.Second)

		// Verify customer was updated in Salesforce
		sfContact, err := salesforceService.GetContact(ctx, updatedCustomer.ID)
		assert.NoError(t, err)
		assert.NotNil(t, sfContact)
		assert.Equal(t, updatedCustomer.FirstName, sfContact.FirstName)
		assert.Equal(t, updatedCustomer.LastName, sfContact.LastName)
		assert.Equal(t, updatedCustomer.Email, sfContact.Email)
	})
}
