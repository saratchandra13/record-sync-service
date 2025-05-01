package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
	"github.com/saratchandra/record-sync-service/internal/sync"
	"github.com/saratchandra/record-sync-service/internal/system"
	"github.com/saratchandra/record-sync-service/internal/transform"
)

func main() {
	// Create a context that we can cancel or deadline upto us.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize transformer registry
	transformerRegistry := transform.NewTransformerRegistry()

	// Register transformers using proper source/target constants
	transformerRegistry.RegisterTransformer(string(models.SourceSalesforce), string(models.SourceInternal), &transform.SalesforceToInternalTransformer{})
	transformerRegistry.RegisterTransformer(string(models.SourceInternal), string(models.SourceSalesforce), &transform.InternalToSalesforceTransformer{})

	// Create sync engine first so we can use it in the onChange callback
	engine := sync.NewSyncEngine(transformerRegistry, []sync.SyncRule{
		{
			RecordType:    "customer",
			Operations:    []models.SyncOperation{models.Create, models.Update, models.Delete},
			Direction:     "bidirectional",
			SourceSystems: []models.SyncSource{models.SourceInternal},
			TargetSystems: []models.SyncSource{models.SourceSalesforce},
			Priority:      1,
			Enabled:       true,
		},
	}, 5)

	// Create change event callback that enqueues to the engine
	onChange := func(event models.SyncEvent) {
		log.Printf("Change event received: %v", event)
		if err := engine.EnqueueEvent(event); err != nil {
			log.Printf("Failed to enqueue event: %v", err)
		}
	}

	// Initialize services
	internalService := system.NewInternalSystemService(onChange, "")
	salesforceService := system.NewSalesforceSystemService(onChange, os.Getenv("SALESFORCE_URL"), os.Getenv("SALESFORCE_API_KEY"))

	// Set up webhook handler
	webhookHandler := system.NewWebhookHandler(salesforceService, onChange)
	http.HandleFunc("/webhook/salesforce", webhookHandler.HandleSalesforceWebhook)

	// Start HTTP server for webhooks
	go func() {
		port := os.Getenv("WEBHOOK_PORT")
		if port == "" {
			port = "8082"
		}
		log.Printf("Starting webhook server on port %s", port)
		if err := http.ListenAndServe(":"+port, nil); err != nil {
			log.Printf("Webhook server error: %v", err)
		}
	}()

	// Create and register handlers
	internalHandler := system.NewInternalSystemHandler(internalService, transformerRegistry)
	salesforceHandler := system.NewSalesforceSystemHandler(salesforceService, transformerRegistry)

	engine.RegisterHandler(models.SourceInternal, internalHandler)
	engine.RegisterHandler(models.SourceSalesforce, salesforceHandler)

	// Start the engine
	if err := engine.Start(ctx); err != nil {
		log.Fatalf("Failed to start sync engine: %v", err)
	}

	// Create a test customer to trigger sync
	go func() {
		// Wait a bit for services to start
		time.Sleep(2 * time.Second)

		customer := models.InternalCustomer{
			ID:        "test-customer-1",
			FirstName: "sarat",
			LastName:  "Chandra",
			Email:     "sarat.chandra@example.com",
		}

		log.Println("Creating test customer...")
		createdCustomer, err := internalService.CreateCustomer(ctx, customer)
		if err != nil {
			log.Printf("Failed to create test customer: %v", err)
			return
		}
		log.Printf("Test customer created: %+v", createdCustomer)
	}()

	// Set up signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigChan
	log.Printf("Received signal %v, shutting down...", sig)

	// Cancel the context to initiate shutdown
	cancel()

	// Give some time for graceful shutdown
	log.Println("Shutdown complete")
}
