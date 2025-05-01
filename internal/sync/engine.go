package sync

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
	"github.com/saratchandra/record-sync-service/internal/transform"
)

// SyncRule defines when and how synchronization should occur
type SyncRule struct {
	RecordType      string             // Type of record this rule applies to
	Operations      []models.SyncOperation // Which operations to sync (Create, Update, Delete)
	Direction       string             // "to_external", "to_internal", or "bidirectional"
	Priority        int                // Higher priority rules are processed first
	Enabled         bool               // Whether this rule is active
	Transformations map[string]string  // Special field transformations for this rule
}

// SyncEngine is the core component managing record synchronization
type SyncEngine struct {
	Transformer      *transform.Transformer
	InternalService  InternalSystemService
	ExternalService  ExternalSystemService
	Rules            []SyncRule
	ProcessedEvents  map[string]bool      // Track processed sync IDs to prevent cycles
	EventQueue       chan models.SyncEvent // Channel for sync events
	mutex            sync.RWMutex         // For thread-safe access to shared data
	maxRetries       int                  // Maximum retry attempts for failed syncs
	processingWorkers int                 // Number of workers processing events
}

// InternalSystemService defines the interface for interacting with the internal system
type InternalSystemService interface {
	Create(ctx context.Context, record models.InternalRecord) (models.InternalRecord, error)
	Read(ctx context.Context, id string) (models.InternalRecord, error)
	Update(ctx context.Context, record models.InternalRecord) error
	Delete(ctx context.Context, id string) error
}

// ExternalSystemService defines the interface for interacting with the external system
type ExternalSystemService interface {
	Create(ctx context.Context, record models.ExternalRecord) (models.ExternalRecord, error)
	Read(ctx context.Context, id string) (models.ExternalRecord, error)
	Update(ctx context.Context, record models.ExternalRecord) error
	Delete(ctx context.Context, id string) error
	GetRateLimit() (int, time.Duration) // Returns requests per duration
}

// NewSyncEngine creates a new sync engine with the provided dependencies
func NewSyncEngine(
	transformer *transform.Transformer,
	internalService InternalSystemService,
	externalService ExternalSystemService,
	rules []SyncRule,
	workers int,
) *SyncEngine {
	return &SyncEngine{
		Transformer:      transformer,
		InternalService:  internalService,
		ExternalService:  externalService,
		Rules:            rules,
		ProcessedEvents:  make(map[string]bool),
		EventQueue:       make(chan models.SyncEvent, 10000), // Buffer for spikes
		maxRetries:       3,
		processingWorkers: workers,
	}
}

// Start begins processing sync events
func (e *SyncEngine) Start(ctx context.Context) error {
	log.Println("Starting Sync Engine with", e.processingWorkers, "workers")
	
	// Start workers to process events
	for i := 0; i < e.processingWorkers; i++ {
		go e.processEvents(ctx, i)
	}
	
	return nil
}

// processEvents handles sync events from the queue
func (e *SyncEngine) processEvents(ctx context.Context, workerID int) {
	log.Printf("Worker %d started", workerID)
	
	for {
		select {
		case event := <-e.EventQueue:
			log.Printf("Worker %d processing event: %s - %s", workerID, event.Operation, event.RecordType)
			
			// Check if we've already processed this event
			e.mutex.RLock()
			alreadyProcessed := e.ProcessedEvents[event.SyncID]
			e.mutex.RUnlock()
			
			if alreadyProcessed {
				log.Printf("Skipping already processed event with syncID: %s", event.SyncID)
				continue
			}
			
			// Process the event
			err := e.handleSyncEvent(ctx, event)
			if err != nil {
				log.Printf("Error processing event: %v", err)
				// Here we would typically implement retry logic or dead letter queue
			} else {
				// Mark as processed
				e.mutex.Lock()
				e.ProcessedEvents[event.SyncID] = true
				e.mutex.Unlock()
			}
			
		case <-ctx.Done():
			log.Printf("Worker %d stopping due to context cancellation", workerID)
			return
		}
	}
}

// EnqueueEvent adds a sync event to the processing queue
func (e *SyncEngine) EnqueueEvent(event models.SyncEvent) error {
	// Generate sync ID if not provided
	if event.SyncID == "" {
		event.SyncID = uuid.New().String()
	}
	
	// Validate event
	if event.Operation == "" || event.Source == "" || event.RecordType == "" {
		return errors.New("invalid sync event: missing required fields")
	}
	
	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	
	// Apply rules to determine if this event should be processed
	if !e.shouldProcess(event) {
		log.Printf("Event filtered out by rules: %v", event)
		return nil
	}
	
	// Enqueue the event
	select {
	case e.EventQueue <- event:
		return nil
	default:
		return errors.New("event queue is full, try again later")
	}
}

// shouldProcess determines if an event should be processed based on configured rules
func (e *SyncEngine) shouldProcess(event models.SyncEvent) bool {
	for _, rule := range e.Rules {
		// Skip disabled rules
		if !rule.Enabled {
			continue
		}
		
		// Check if rule applies to this record type
		if rule.RecordType != event.RecordType && rule.RecordType != "*" {
			continue
		}
		
		// Check if operation is supported by this rule
		operationSupported := false
		for _, op := range rule.Operations {
			if op == event.Operation {
				operationSupported = true
				break
			}
		}
		if !operationSupported {
			continue
		}
		
		// Check direction
		if (event.Source == models.SourceInternal && rule.Direction == "to_external") ||
		   (event.Source == models.SourceExternal && rule.Direction == "to_internal") ||
		   rule.Direction == "bidirectional" {
			return true
		}
	}
	
	return false
}

// handleSyncEvent processes a single sync event
func (e *SyncEngine) handleSyncEvent(ctx context.Context, event models.SyncEvent) error {
	log.Printf("Handling event: %s from %s", event.Operation, event.Source)
	
	switch event.Source {
	case models.SourceInternal:
		return e.syncToExternal(ctx, event)
	case models.SourceExternal:
		return e.syncToInternal(ctx, event)
	default:
		return errors.New("unknown event source")
	}
}

// syncToExternal synchronizes a change from the internal system to the external system
func (e *SyncEngine) syncToExternal(ctx context.Context, event models.SyncEvent) error {
	// Type assertion to get internal record
	internalRecord, ok := event.Record.(models.InternalRecord)
	if !ok {
		return errors.New("event record is not an internal record")
	}
	
	// Transform to external format
	externalRecord, err := e.Transformer.InternalToExternal(internalRecord)
	if err != nil {
		return err
	}
	
	// Ensure sync ID is preserved
	externalRecord.Metadata.SyncID = event.SyncID
	externalRecord.Metadata.Source = models.SourceInternal
	
	// Apply the operation to the external system
	switch event.Operation {
	case models.Create:
		_, err = e.ExternalService.Create(ctx, externalRecord)
		return err
	case models.Update:
		return e.ExternalService.Update(ctx, externalRecord)
	case models.Delete:
		return e.ExternalService.Delete(ctx, externalRecord.ID)
	default:
		return errors.New("unsupported operation")
	}
}

// syncToInternal synchronizes a change from the external system to the internal system
func (e *SyncEngine) syncToInternal(ctx context.Context, event models.SyncEvent) error {
	// Type assertion to get external record
	externalRecord, ok := event.Record.(models.ExternalRecord)
	if !ok {
		return errors.New("event record is not an external record")
	}
	
	// Transform to internal format
	internalRecord, err := e.Transformer.ExternalToInternal(externalRecord)
	if err != nil {
		return err
	}
	
	// Ensure sync ID is preserved
	internalRecord.SyncID = event.SyncID
	internalRecord.SyncSource = models.SourceExternal
	
	// Apply the operation to the internal system
	switch event.Operation {
	case models.Create:
		_, err = e.InternalService.Create(ctx, internalRecord)
		return err
	case models.Update:
		return e.InternalService.Update(ctx, internalRecord)
	case models.Delete:
		return e.InternalService.Delete(ctx, internalRecord.ID)
	default:
		return errors.New("unsupported operation")
	}
}
