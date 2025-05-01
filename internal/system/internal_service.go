package system

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/saratchandra/record-sync-service/internal/models"
)

// InternalSystemService defines the interface for the internal system
type InternalSystemService interface {
	CreateCustomer(ctx context.Context, customer models.InternalCustomer) (models.InternalCustomer, error)
	GetCustomer(ctx context.Context, id string) (models.InternalCustomer, error)
	UpdateCustomer(ctx context.Context, customer models.InternalCustomer) error
	DeleteCustomer(ctx context.Context, id string) error
}

// internalSystemServiceImpl implements InternalSystemService
type internalSystemServiceImpl struct {
	customers      map[string]models.InternalCustomer
	customersMutex sync.RWMutex
	OnChange       func(models.SyncEvent)
}

// NewInternalSystemService creates a new internal system service
func NewInternalSystemService(onChange func(models.SyncEvent), _ string) InternalSystemService {
	return &internalSystemServiceImpl{
		customers: make(map[string]models.InternalCustomer),
		OnChange: onChange,
	}
}

// CreateCustomer adds a new customer to the internal system
func (s *internalSystemServiceImpl) CreateCustomer(ctx context.Context, customer models.InternalCustomer) (models.InternalCustomer, error) {
	s.customersMutex.Lock()
	defer s.customersMutex.Unlock()

	if _, exists := s.customers[customer.ID]; exists {
		return models.InternalCustomer{}, errors.New("customer already exists")
	}

	if customer.CreatedDate.IsZero() {
		customer.CreatedDate = time.Now()
	}
	if customer.UpdatedDate.IsZero() {
		customer.UpdatedDate = time.Now()
	}
	customer.Metadata.Source = models.SourceInternal

	s.customers[customer.ID] = customer

	if !strings.HasPrefix(customer.Metadata.SyncID, "reconcile-") && s.OnChange != nil {
		s.OnChange(models.SyncEvent{
			Operation:  models.Create,
			Source:     models.SourceInternal,
			SyncID:     customer.Metadata.SyncID,
			RecordType: "customer",
			Record:     customer,
			Timestamp:  time.Now(),
		})
	}

	return customer, nil
}

// GetCustomer retrieves a customer from the internal system
func (s *internalSystemServiceImpl) GetCustomer(ctx context.Context, id string) (models.InternalCustomer, error) {
	s.customersMutex.RLock()
	defer s.customersMutex.RUnlock()

	customer, exists := s.customers[id]
	if !exists {
		return models.InternalCustomer{}, errors.New("customer not found")
	}

	return customer, nil
}

// UpdateCustomer modifies an existing customer in the internal system
func (s *internalSystemServiceImpl) UpdateCustomer(ctx context.Context, customer models.InternalCustomer) error {
	s.customersMutex.Lock()
	defer s.customersMutex.Unlock()

	existing, exists := s.customers[customer.ID]
	if !exists {
		return errors.New("customer not found")
	}

	customer.CreatedDate = existing.CreatedDate
	customer.UpdatedDate = time.Now()

	s.customers[customer.ID] = customer

	if !strings.HasPrefix(customer.Metadata.SyncID, "reconcile-") && s.OnChange != nil {
		s.OnChange(models.SyncEvent{
			Operation:  models.Update,
			Source:     models.SourceInternal,
			SyncID:     customer.Metadata.SyncID,
			RecordType: "customer",
			Record:     customer,
			Timestamp:  time.Now(),
		})
	}

	return nil
}

// DeleteCustomer removes a customer from the internal system
func (s *internalSystemServiceImpl) DeleteCustomer(ctx context.Context, id string) error {
	s.customersMutex.Lock()
	defer s.customersMutex.Unlock()

	existing, exists := s.customers[id]
	if !exists {
		return errors.New("customer not found")
	}

	delete(s.customers, id)

	if !strings.HasPrefix(existing.Metadata.SyncID, "reconcile-") && s.OnChange != nil {
		s.OnChange(models.SyncEvent{
			Operation:  models.Delete,
			Source:     models.SourceInternal,
			SyncID:     existing.Metadata.SyncID,
			RecordType: "customer",
			Record:     existing,
			Timestamp:  time.Now(),
		})
	}

	return nil
}
