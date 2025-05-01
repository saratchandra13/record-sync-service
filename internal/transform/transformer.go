package transform

import (
	"errors"
	"github.com/google/uuid"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
)

// Transformer handles the conversion between internal and external record formats
type Transformer struct {
	// Could include configuration for field mappings, validation rules, etc.
	FieldMappings map[string]map[string]string // Maps record types to field mappings
}

// NewTransformer creates a new transformer with default mappings
func NewTransformer() *Transformer {
	// Initialize with some default field mappings for demonstration
	fieldMappings := make(map[string]map[string]string)
	
	// For Customer type, map internal field names to external field names
	fieldMappings["customer"] = map[string]string{
		"name": "fullName",
		"email": "emailAddress",
		"phone": "phoneNumber",
		"status": "customerStatus",
	}
	
	return &Transformer{
		FieldMappings: fieldMappings,
	}
}

// InternalToExternal converts an internal record to external format
func (t *Transformer) InternalToExternal(internal models.InternalRecord) (models.ExternalRecord, error) {
	var external models.ExternalRecord
	
	// Set basic fields
	external.ID = internal.ExternalID // Use existing external ID if available
	if external.ID == "" { // Otherwise, generate a new one
		external.ID = uuid.New().String()
	}
	
	external.Type = internal.RecordType
	external.Properties = make(map[string]interface{})
	
	// Map fields according to configured mappings
	if mappings, ok := t.FieldMappings[internal.RecordType]; ok {
		for internalField, externalField := range mappings {
			if value, exists := internal.Attributes[internalField]; exists {
				external.Properties[externalField] = value
			}
		}
	} else {
		// If no mappings exist, copy attributes directly
		for key, value := range internal.Attributes {
			external.Properties[key] = value
		}
	}
	
	// Set metadata
	external.Metadata.Created = internal.CreatedAt
	external.Metadata.Modified = internal.UpdatedAt
	external.Metadata.SyncID = internal.SyncID
	external.Metadata.Source = models.SourceInternal
	external.Metadata.InternalRef = internal.ID
	
	return external, nil
}

// ExternalToInternal converts an external record to internal format
func (t *Transformer) ExternalToInternal(external models.ExternalRecord) (models.InternalRecord, error) {
	var internal models.InternalRecord
	
	// Set basic fields
	internal.ID = external.Metadata.InternalRef // Use existing internal ID if available
	if internal.ID == "" { // Otherwise, generate a new one
		internal.ID = uuid.New().String()
	}
	
	internal.RecordType = external.Type
	internal.Attributes = make(map[string]interface{})
	
	// Map fields according to configured mappings (reverse)
	if mappings, ok := t.FieldMappings[external.Type]; ok {
		// Create reverse mapping (external -> internal)
		reverseMappings := make(map[string]string)
		for inField, exField := range mappings {
			reverseMappings[exField] = inField
		}
		
		for externalField, value := range external.Properties {
			if internalField, exists := reverseMappings[externalField]; exists {
				internal.Attributes[internalField] = value
			} else {
				// For fields without mapping, copy directly
				internal.Attributes[externalField] = value
			}
		}
	} else {
		// If no mappings exist, copy properties directly
		for key, value := range external.Properties {
			internal.Attributes[key] = value
		}
	}
	
	// Set metadata fields
	internal.CreatedAt = external.Metadata.Created
	internal.UpdatedAt = external.Metadata.Modified
	internal.SyncID = external.Metadata.SyncID
	internal.SyncSource = models.SourceExternal
	internal.ExternalID = external.ID
	
	return internal, nil
}

// ValidateTransformation checks if a transformation is valid
func (t *Transformer) ValidateTransformation(record interface{}) error {
	switch r := record.(type) {
	case models.InternalRecord:
		// Validate internal record (example validation)
		if r.ID == "" {
			return errors.New("internal record missing ID")
		}
		if r.RecordType == "" {
			return errors.New("internal record missing type")
		}
		return nil
	case models.ExternalRecord:
		// Validate external record (example validation)
		if r.ID == "" {
			return errors.New("external record missing ID")
		}
		if r.Type == "" {
			return errors.New("external record missing type")
		}
		return nil
	default:
		return errors.New("unknown record type for validation")
	}
}
