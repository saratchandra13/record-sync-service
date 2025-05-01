package system

import (
	"context"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/saratchandra/record-sync-service/internal/models"
)

// SalesforceSystemService defines the interface for Salesforce integration
type SalesforceSystemService interface {
	CreateContact(ctx context.Context, contact models.SalesforceContact) (models.SalesforceContact, error)
	GetContact(ctx context.Context, id string) (models.SalesforceContact, error)
	UpdateContact(ctx context.Context, contact models.SalesforceContact) error
	DeleteContact(ctx context.Context, id string) error
}

// salesforceSystemServiceImpl implements SalesforceSystemService
type salesforceSystemServiceImpl struct {
	baseURL         string
	client          *http.Client
	contacts        map[string]models.SalesforceContact // Local cache
	contactsMutex   sync.RWMutex
	rateLimit       int
	rateLimitPeriod time.Duration
	lastRequestTime time.Time
	requestCount    int
	requestMutex    sync.Mutex
	apiKey          string
	OnChange        func(models.SyncEvent)
}

// NewSalesforceSystemService creates a new Salesforce service instance
func NewSalesforceSystemService(onChange func(models.SyncEvent), baseURL string, apiKey string) SalesforceSystemService {
	return &salesforceSystemServiceImpl{
		baseURL:         baseURL,
		client:          &http.Client{Timeout: 10 * time.Second},
		contacts:        make(map[string]models.SalesforceContact),
		rateLimit:       100,
		rateLimitPeriod: time.Minute,
		lastRequestTime: time.Now(),
		apiKey:          apiKey,
		OnChange:        onChange,
	}
}

// checkRateLimit enforces the rate limit for external API calls
func (s *salesforceSystemServiceImpl) checkRateLimit() error {
	s.requestMutex.Lock()
	defer s.requestMutex.Unlock()

	now := time.Now()
	
	// Reset counter if we're in a new period
	if now.Sub(s.lastRequestTime) > s.rateLimitPeriod {
		s.requestCount = 0
		s.lastRequestTime = now
	}
	
	// Check if we've exceeded the limit
	if s.requestCount >= s.rateLimit {
		return errors.New("rate limit exceeded for Salesforce API")
	}
	
	// Increment counter
	s.requestCount++
	return nil
}

// CreateContact adds a new contact record to Salesforce
func (s *salesforceSystemServiceImpl) CreateContact(ctx context.Context, contact models.SalesforceContact) (models.SalesforceContact, error) {
	// Check rate limit
	if err := s.checkRateLimit(); err != nil {
		return models.SalesforceContact{}, err
	}

	s.contactsMutex.Lock()
	defer s.contactsMutex.Unlock()
	
	// Check if contact already exists locally
	if _, exists := s.contacts[contact.Id]; exists {
		return models.SalesforceContact{}, errors.New("contact already exists in Salesforce")
	}
	
	// Set timestamps if not provided
	if contact.CreatedDate.IsZero() {
		contact.CreatedDate = time.Now()
	}
	if contact.LastModified.IsZero() {
		contact.LastModified = time.Now()
	}
	
	// Call the mock Salesforce API to create the contact
	jsonData, err := json.Marshal(contact)
	if err != nil {
		return models.SalesforceContact{}, fmt.Errorf("failed to marshal contact: %w", err)
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", fmt.Sprintf("%s/contacts", s.baseURL), bytes.NewBuffer(jsonData))
	if err != nil {
		return models.SalesforceContact{}, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", s.apiKey)
	
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("Warning: Failed to connect to mock Salesforce API: %v", err)
		log.Printf("Falling back to in-memory storage for Salesforce contact")
		// Fall back to local storage if the HTTP call fails
		s.contacts[contact.Id] = contact
		log.Printf("Created Salesforce contact (local fallback): %s", contact.Id)
	} else {
		defer resp.Body.Close()
		
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return models.SalesforceContact{}, fmt.Errorf("failed to read response: %w", err)
		}
		
		if resp.StatusCode != http.StatusCreated {
			return models.SalesforceContact{}, fmt.Errorf("Salesforce API error: %s", string(body))
		}
		
		var createdContact models.SalesforceContact
		if err := json.Unmarshal(body, &createdContact); err != nil {
			return models.SalesforceContact{}, fmt.Errorf("failed to unmarshal response: %w", err)
		}
		
		// Update local cache
		s.contacts[createdContact.Id] = createdContact
		log.Printf("Created Salesforce contact (API): %s", createdContact.Id)
		contact = createdContact
	}
	
	// Only trigger change event if this is not a reconciliation event
	// This allows the sync engine to propagate to all systems except the source
	if !strings.HasPrefix(contact.Metadata.SyncID, "reconcile-") && s.OnChange != nil {
		s.OnChange(models.SyncEvent{
			Operation:  models.Create,
			Source:     models.SourceSalesforce,
			SyncID:     contact.Metadata.SyncID,
			RecordType: "contact",
			Record:     contact,
			Timestamp:  time.Now(),
		})
	}
	
	return contact, nil
}

// GetContact retrieves a contact from Salesforce
func (s *salesforceSystemServiceImpl) GetContact(ctx context.Context, id string) (models.SalesforceContact, error) {
	// Check rate limit
	if err := s.checkRateLimit(); err != nil {
		return models.SalesforceContact{}, err
	}

	s.contactsMutex.RLock()
	defer s.contactsMutex.RUnlock()
	
	// Check local cache first for efficiency
	contact, exists := s.contacts[id]
	if exists {
		return contact, nil
	}
	
	// If not in cache, call the mock Salesforce API
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/contacts/%s", s.baseURL, id), nil)
	if err != nil {
		return models.SalesforceContact{}, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("X-API-Key", s.apiKey)
	
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("Warning: Failed to connect to mock Salesforce API: %v", err)
		return models.SalesforceContact{}, errors.New("contact not found in Salesforce and API unreachable")
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == http.StatusNotFound {
		return models.SalesforceContact{}, errors.New("contact not found in Salesforce")
	}
	
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return models.SalesforceContact{}, fmt.Errorf("failed to read response: %w", err)
	}
	
	if resp.StatusCode != http.StatusOK {
		return models.SalesforceContact{}, fmt.Errorf("Salesforce API error: %s", string(body))
	}
	
	var fetchedContact models.SalesforceContact
	if err := json.Unmarshal(body, &fetchedContact); err != nil {
		return models.SalesforceContact{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}
	
	// Update local cache
	s.contactsMutex.RUnlock()
	s.contactsMutex.Lock()
	s.contacts[fetchedContact.Id] = fetchedContact
	s.contactsMutex.Unlock()
	s.contactsMutex.RLock()
	
	return fetchedContact, nil
}

// UpdateContact modifies an existing contact in Salesforce
func (s *salesforceSystemServiceImpl) UpdateContact(ctx context.Context, contact models.SalesforceContact) error {
	// Check rate limit
	if err := s.checkRateLimit(); err != nil {
		return err
	}

	s.contactsMutex.Lock()
	defer s.contactsMutex.Unlock()
	
	// Check if contact exists locally
	existing, exists := s.contacts[contact.Id]
	if !exists {
		return errors.New("contact not found in Salesforce")
	}
	
	// Preserve creation time
	contact.CreatedDate = existing.CreatedDate
	
	// Update timestamp
	contact.LastModified = time.Now()
	
	// Call the mock Salesforce API to update the contact
	jsonData, err := json.Marshal(contact)
	if err != nil {
		return fmt.Errorf("failed to marshal contact: %w", err)
	}
	
	req, err := http.NewRequestWithContext(ctx, "PUT", fmt.Sprintf("%s/contacts/%s", s.baseURL, contact.Id), bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", s.apiKey)
	
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("Warning: Failed to connect to mock Salesforce API: %v", err)
		log.Printf("Falling back to in-memory storage for Salesforce contact update")
		// Fall back to local storage if the HTTP call fails
		s.contacts[contact.Id] = contact
		log.Printf("Updated Salesforce contact (local fallback): %s", contact.Id)
	} else {
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("Salesforce API error: %s", string(body))
		}
		
		// Update local cache
		s.contacts[contact.Id] = contact
		log.Printf("Updated Salesforce contact (API): %s", contact.Id)
	}
	
	// Only trigger change event if this is not a reconciliation event
	// This allows the sync engine to propagate to all systems except the source
	if !strings.HasPrefix(contact.Metadata.SyncID, "reconcile-") && s.OnChange != nil {
		s.OnChange(models.SyncEvent{
			Operation:  models.Update,
			Source:     models.SourceSalesforce,
			SyncID:     contact.Metadata.SyncID,
			RecordType: "contact",
			Record:     contact,
			Timestamp:  time.Now(),
		})
	}
	
	return nil
}

// DeleteContact removes a contact from Salesforce
func (s *salesforceSystemServiceImpl) DeleteContact(ctx context.Context, id string) error {
	// Check rate limit
	if err := s.checkRateLimit(); err != nil {
		return err
	}

	s.contactsMutex.Lock()
	defer s.contactsMutex.Unlock()
	
	// Check if contact exists
	existing, exists := s.contacts[id]
	if !exists {
		return errors.New("contact not found in Salesforce")
	}
	
	// Call the mock Salesforce API to delete the contact
	req, err := http.NewRequestWithContext(ctx, "DELETE", fmt.Sprintf("%s/contacts/%s", s.baseURL, id), nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("X-API-Key", s.apiKey)
	
	resp, err := s.client.Do(req)
	if err != nil {
		log.Printf("Warning: Failed to connect to mock Salesforce API: %v", err)
		log.Printf("Falling back to in-memory storage for Salesforce contact deletion")
		// Fall back to local storage if the HTTP call fails
		delete(s.contacts, id)
		log.Printf("Deleted Salesforce contact (local fallback): %s", id)
	} else {
		defer resp.Body.Close()
		
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
			body, _ := ioutil.ReadAll(resp.Body)
			return fmt.Errorf("Salesforce API error: %s", string(body))
		}
		
		// Update local cache
		delete(s.contacts, id)
		log.Printf("Deleted Salesforce contact (API): %s", id)
	}
	
	// Only trigger change event if this is not a reconciliation event
	// This allows the sync engine to propagate to all systems except the source
	if !strings.HasPrefix(existing.Metadata.SyncID, "reconcile-") && s.OnChange != nil {
		s.OnChange(models.SyncEvent{
			Operation:  models.Delete,
			Source:     models.SourceSalesforce,
			SyncID:     existing.Metadata.SyncID,
			RecordType: "contact",
			Record:     existing,
			Timestamp:  time.Now(),
		})
	}
	
	return nil
}
