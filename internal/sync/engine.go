package sync

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
	"github.com/saratchandra/record-sync-service/internal/transform"
)

// SyncRule defines when and how records should be synchronized
type SyncRule struct {
	RecordType    string                 // Type of record this rule applies to (e.g., "customer")
	Operations    []models.SyncOperation // Operations that trigger this rule (create, update, delete)
	Direction     string                 // Direction of sync ("to_internal", "to_salesforce", "bidirectional")
	SourceSystems []models.SyncSource    // Source systems that can trigger this rule
	TargetSystems []models.SyncSource    // Target systems to sync to
	Priority      int                    // Order of execution (lower numbers run first)
	Enabled       bool                   // Whether this rule is active
}

// SyncHandler defines the interface for system-specific sync handlers
type SyncHandler interface {
	HandleCreate(ctx context.Context, event models.SyncEvent) error
	HandleUpdate(ctx context.Context, event models.SyncEvent) error
	HandleDelete(ctx context.Context, event models.SyncEvent) error
}

// SyncEngine orchestrates the synchronization between systems
type SyncEngine struct {
	transformerRegistry *transform.TransformerRegistry
	handlers           map[models.SyncSource]SyncHandler
	Rules             []SyncRule
	ProcessedEvents   map[string]bool     // Track which events we've processed
	EventQueue        chan models.SyncEvent
	mutex             sync.RWMutex
	maxRetries        int
	processingWorkers int
}

// NewSyncEngine creates a new sync engine with the provided dependencies
func NewSyncEngine(
	transformerRegistry *transform.TransformerRegistry,
	rules []SyncRule,
	workers int,
) *SyncEngine {
	return &SyncEngine{
		transformerRegistry: transformerRegistry,
		handlers:           make(map[models.SyncSource]SyncHandler),
		Rules:             rules,
		ProcessedEvents:   make(map[string]bool),
		EventQueue:        make(chan models.SyncEvent, 10000), // iirl this would be a kafka with multiple consumers listening to updates.
		maxRetries:        3,
		processingWorkers: workers,
	}
}

// RegisterHandler registers a sync handler for a specific system
func (e *SyncEngine) RegisterHandler(source models.SyncSource, handler SyncHandler) {
	e.handlers[source] = handler
}

// Start initializes the sync engine and begins processing events
func (e *SyncEngine) Start(ctx context.Context) error {
	log.Printf("Starting Sync Engine with %d workers", e.processingWorkers)

	if len(e.handlers) == 0 {
		return errors.New("no sync handlers registered")
	}

	// Start worker pool
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
			log.Printf("Worker %d processing event: %s - %s from %s", workerID, event.Operation, event.RecordType, event.Source)

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
				// if we cant handle some, we can do a reconcillation by pushing these to a dead letter queue
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
	// Check if we've already processed this event
	e.mutex.RLock()
	alreadyProcessed := e.ProcessedEvents[event.SyncID]
	e.mutex.RUnlock()

	if alreadyProcessed {
		log.Printf("Skipping already enqueued event with syncID: %s", event.SyncID)
		return nil
	}

	// Add to queue
	select {
	case e.EventQueue <- event:
		return nil
	default:
		return errors.New("event queue is full")
	}
}

// IsProcessed checks if an event with this syncID has been processed
func (e *SyncEngine) IsProcessed(syncID string) bool {
	e.mutex.RLock()
	processed := e.ProcessedEvents[syncID]
	e.mutex.RUnlock()
	return processed
}

// ClearProcessedEvents removes processed event history for all events older than the duration
func (e *SyncEngine) ClearProcessedEvents(olderThan time.Duration) int {
	// This would typically check timestamps in a real implementation
	// For demo purposes, we'll just clear all
	e.mutex.Lock()
	count := len(e.ProcessedEvents)
	e.ProcessedEvents = make(map[string]bool)
	e.mutex.Unlock()
	return count
}

// determineTargetSystems evaluates sync rules to determine target systems
func (e *SyncEngine) determineTargetSystems(event models.SyncEvent) ([]models.SyncSource, bool) {
	// Skip if the SyncID indicates a reconciliation event to prevent infinite loops
	if strings.HasPrefix(event.SyncID, "reconcile-") {
		log.Printf("Skipping reconciliation event: %s", event.SyncID)
		return nil, false
	}

	// Find applicable rules
	var matchingRules []SyncRule
	for _, rule := range e.Rules {
		if !rule.Enabled {
			continue // Skip disabled rules
		}

		// Check if rule applies to this record type
		if rule.RecordType != event.RecordType {
			continue
		}

		// Check if rule applies to this operation
		operationMatches := false
		for _, op := range rule.Operations {
			if op == event.Operation {
				operationMatches = true
				break
			}
		}
		if !operationMatches {
			continue
		}

		// Check if rule applies to this source system
		sourceMatches := false
		for _, source := range rule.SourceSystems {
			if source == event.Source {
				sourceMatches = true
				break
			}
		}
		if !sourceMatches {
			continue
		}

		// This rule applies
		matchingRules = append(matchingRules, rule)
	}

	// If no rules match, skip processing
	if len(matchingRules) == 0 {
		log.Printf("No matching rules for event: %v", event)
		return nil, false
	}

	// Sort rules by priority
	sort.Slice(matchingRules, func(i, j int) bool {
		return matchingRules[i].Priority < matchingRules[j].Priority
	})

	// Get target systems from highest priority rule
	topRule := matchingRules[0]
	var targetSystems []models.SyncSource

	// Add all target systems except the source system (to prevent loops)
	for _, target := range topRule.TargetSystems {
		if target != event.Source {
			targetSystems = append(targetSystems, target)
		}
	}

	log.Printf("Determined target systems for event %s: %v", event.SyncID, targetSystems)
	return targetSystems, true
}

// handleSyncEvent processes a single sync event
func (e *SyncEngine) handleSyncEvent(ctx context.Context, event models.SyncEvent) error {
	log.Printf("Handling event: %s from %s", event.Operation, event.Source)

	// Determine target systems for this event
	targetSystems, shouldProcess := e.determineTargetSystems(event)
	if !shouldProcess || len(targetSystems) == 0 {
		log.Printf("No targets for event: %v", event)
		return nil
	}

	// Process sync to each target system
	var lastError error
	for _, targetSystem := range targetSystems {
		handler, exists := e.handlers[targetSystem]
		if !exists {
			log.Printf("No handler registered for target system: %s", targetSystem)
			continue
		}

		// Create a new sync ID for reconciliation to prevent infinite loops
		event.SyncID = fmt.Sprintf("reconcile-%s", event.SyncID)
		
	
		var err error
		switch event.Operation {
		case models.Create:
			err = handler.HandleCreate(ctx, event)
		case models.Update:
			err = handler.HandleUpdate(ctx, event)
		case models.Delete:
			err = handler.HandleDelete(ctx, event)
		default:
			err = fmt.Errorf("unsupported operation: %s", event.Operation)
		}

		if err != nil {
			log.Printf("Error syncing to %s: %v", targetSystem, err)
			lastError = err
		}
	}

	return lastError
}
