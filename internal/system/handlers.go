package system

import (
	"context"
	"errors"
	"fmt"

	"github.com/saratchandra/record-sync-service/internal/models"
	"github.com/saratchandra/record-sync-service/internal/transform"
)

// InternalSystemHandler handles sync operations for the internal system
type InternalSystemHandler struct {
	service            InternalSystemService
	transformerRegistry *transform.TransformerRegistry
}

// NewInternalSystemHandler creates a new internal system handler
func NewInternalSystemHandler(service InternalSystemService, registry *transform.TransformerRegistry) *InternalSystemHandler {
	return &InternalSystemHandler{
		service:            service,
		transformerRegistry: registry,
	}
}

func (h *InternalSystemHandler) transformRecord(event models.SyncEvent) (models.InternalCustomer, error) {
	transformer, exists := h.transformerRegistry.GetTransformer(string(event.Source), string(models.SourceInternal))
	if !exists {
		return models.InternalCustomer{}, fmt.Errorf("no transformer found for %s to internal", event.Source)
	}

	result, err := transformer.TransformToTarget(event.Record, event.SyncID)
	if err != nil {
		return models.InternalCustomer{}, fmt.Errorf("failed to transform record: %w", err)
	}

	customer, ok := result.(models.InternalCustomer)
	if !ok {
		return models.InternalCustomer{}, errors.New("transformed record is not an internal customer")
	}

	return customer, nil
}

func (h *InternalSystemHandler) HandleCreate(ctx context.Context, event models.SyncEvent) error {
	if event.RecordType != "customer" {
		return errors.New("only customer records are supported")
	}

	internalCustomer, err := h.transformRecord(event)
	if err != nil {
		return err
	}

	_, err = h.service.CreateCustomer(ctx, internalCustomer)
	return err
}

func (h *InternalSystemHandler) HandleUpdate(ctx context.Context, event models.SyncEvent) error {
	if event.RecordType != "customer" {
		return errors.New("only customer records are supported")
	}

	internalCustomer, err := h.transformRecord(event)
	if err != nil {
		return err
	}

	return h.service.UpdateCustomer(ctx, internalCustomer)
}

func (h *InternalSystemHandler) HandleDelete(ctx context.Context, event models.SyncEvent) error {
	if event.RecordType != "customer" {
		return errors.New("only customer records are supported")
	}

	internalCustomer, err := h.transformRecord(event)
	if err != nil {
		return err
	}

	return h.service.DeleteCustomer(ctx, internalCustomer.ID)
}

// SalesforceSystemHandler handles sync operations for Salesforce
type SalesforceSystemHandler struct {
	service            SalesforceSystemService
	transformerRegistry *transform.TransformerRegistry
}

// NewSalesforceSystemHandler creates a new Salesforce system handler
func NewSalesforceSystemHandler(service SalesforceSystemService, registry *transform.TransformerRegistry) *SalesforceSystemHandler {
	return &SalesforceSystemHandler{
		service:            service,
		transformerRegistry: registry,
	}
}

func (h *SalesforceSystemHandler) transformRecord(event models.SyncEvent) (models.SalesforceContact, error) {
	transformer, exists := h.transformerRegistry.GetTransformer(string(event.Source), string(models.SourceSalesforce))
	if !exists {
		return models.SalesforceContact{}, fmt.Errorf("no transformer found for %s to salesforce", event.Source)
	}

	result, err := transformer.TransformToTarget(event.Record, event.SyncID)
	if err != nil {
		return models.SalesforceContact{}, fmt.Errorf("failed to transform record: %w", err)
	}

	contact, ok := result.(models.SalesforceContact)
	if !ok {
		return models.SalesforceContact{}, errors.New("transformed record is not a salesforce contact")
	}

	return contact, nil
}

func (h *SalesforceSystemHandler) HandleCreate(ctx context.Context, event models.SyncEvent) error {
	if event.RecordType != "customer" {
		return errors.New("only customer records are supported")
	}


	contact, err := h.transformRecord(event)
	if err != nil {
		return err
	}


	_, err = h.service.CreateContact(ctx, contact)
	return err
}

func (h *SalesforceSystemHandler) HandleUpdate(ctx context.Context, event models.SyncEvent) error {
	if event.RecordType != "customer" {
		return errors.New("only customer records are supported")
	}

	contact, err := h.transformRecord(event)
	if err != nil {
		return err
	}

	return h.service.UpdateContact(ctx, contact)
}

func (h *SalesforceSystemHandler) HandleDelete(ctx context.Context, event models.SyncEvent) error {
	if event.RecordType != "customer" {
		return errors.New("only customer records are supported")
	}

	contact, err := h.transformRecord(event)
	if err != nil {
		return err
	}

	return h.service.DeleteContact(ctx, contact.Id)
}
