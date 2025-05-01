package transform

import (
	"errors"

	"github.com/saratchandra/record-sync-service/internal/models"
)

// Common errors
var (
    ErrInvalidSourceType = errors.New("invalid source record type")
)

// RecordTransformer defines the interface for transforming records between systems
type RecordTransformer interface {
    // TransformToTarget transforms a record from source format to target format
    TransformToTarget(source interface{}, syncID string) (interface{}, error)
}

// TransformerRegistry maintains a map of transformers for different source-target combinations
type TransformerRegistry struct {
    transformers map[string]RecordTransformer
}

// NewTransformerRegistry creates a new transformer registry
func NewTransformerRegistry() *TransformerRegistry {
    return &TransformerRegistry{
        transformers: make(map[string]RecordTransformer),
    }
}

// RegisterTransformer registers a transformer for a specific source-target combination
func (r *TransformerRegistry) RegisterTransformer(sourceSystem, targetSystem string, transformer RecordTransformer) {
    key := r.makeKey(sourceSystem, targetSystem)
    r.transformers[key] = transformer
}

// GetTransformer retrieves a transformer for a specific source-target combination
func (r *TransformerRegistry) GetTransformer(sourceSystem, targetSystem string) (RecordTransformer, bool) {
    key := r.makeKey(sourceSystem, targetSystem)
    transformer, exists := r.transformers[key]
    return transformer, exists
}

// makeKey creates a unique key for a source-target combination
func (r *TransformerRegistry) makeKey(sourceSystem, targetSystem string) string {
    return sourceSystem + "->" + targetSystem
}

// SalesforceToInternalTransformer transforms Salesforce contacts to internal customers
type SalesforceToInternalTransformer struct{}

func (t *SalesforceToInternalTransformer) TransformToTarget(source interface{}, syncID string) (interface{}, error) {
    contact, ok := source.(models.SalesforceContact)
    if !ok {
        return nil, ErrInvalidSourceType
    }
    
    customer := models.InternalCustomer{
        ID:             contact.Id,
        FirstName:      contact.FirstName,
        LastName:       contact.LastName,
        Email:         contact.Email,
        Phone:         contact.Phone,
        BillingAddress: contact.MailingAddress,
        Status:        contact.Status,
        CreatedDate:   contact.CreatedDate,
        UpdatedDate:   contact.LastModified,
        Metadata:      contact.Metadata,
    }
    customer.Metadata.Source = models.SourceInternal
    return customer, nil
}

// InternalToSalesforceTransformer transforms internal customers to Salesforce contacts
type InternalToSalesforceTransformer struct{}

func (t *InternalToSalesforceTransformer) TransformToTarget(source interface{}, syncID string) (interface{}, error) {
    customer, ok := source.(models.InternalCustomer)
    if !ok {
        return nil, ErrInvalidSourceType
    }

    
    return models.SalesforceContact{
        Id:             customer.ID,
        FirstName:      customer.FirstName,
        LastName:       customer.LastName,
        Email:         customer.Email,
        Phone:         customer.Phone,
        MailingAddress: customer.BillingAddress,
        Status:        customer.Status,
        CreatedDate:   customer.CreatedDate,
        LastModified:  customer.UpdatedDate,
        Metadata:      models.SyncMetadata{
            SyncID:     syncID,
            Source:     models.SourceInternal,
            CreatedAt:  customer.CreatedDate,
            UpdatedAt:  customer.UpdatedDate,
        },
    }, nil
}
