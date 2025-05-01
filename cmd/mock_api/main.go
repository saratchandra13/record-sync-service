package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"github.com/saratchandra/record-sync-service/internal/models"
)

// InMemoryStore provides in-memory storage for mock external services
type InMemoryStore struct {
	contacts      map[string]models.SalesforceContact
	contactsMutex sync.RWMutex
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		contacts: make(map[string]models.SalesforceContact),
	}
}

func main() {
	store := NewInMemoryStore()

	// Create Salesforce API router
	router := mux.NewRouter()
	configureSalesforceRoutes(router, store)

	// Start the server
	log.Println("Starting Mock Salesforce API server...")
	log.Println("Salesforce API server listening on :8081")
	if err := http.ListenAndServe(":8081", router); err != nil {
		log.Fatalf("Salesforce API server failed to start: %v", err)
	}
}

// configureSalesforceRoutes sets up the mock Salesforce API endpoints
func configureSalesforceRoutes(router *mux.Router, store *InMemoryStore) {
	router.HandleFunc("/contacts", store.handleCreateContact).Methods("POST")
	router.HandleFunc("/contacts/{id}", store.handleGetContact).Methods("GET")
	router.HandleFunc("/contacts/{id}", store.handleUpdateContact).Methods("PUT")
	router.HandleFunc("/contacts/{id}", store.handleDeleteContact).Methods("DELETE")
}

func (s *InMemoryStore) handleCreateContact(w http.ResponseWriter, r *http.Request) {
	var contact models.SalesforceContact
	if err := json.NewDecoder(r.Body).Decode(&contact); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.contactsMutex.Lock()
	defer s.contactsMutex.Unlock()

	if _, exists := s.contacts[contact.Id]; exists {
		http.Error(w, "Contact already exists", http.StatusConflict)
		return
	}

	if contact.CreatedDate.IsZero() {
		contact.CreatedDate = time.Now()
	}
	if contact.LastModified.IsZero() {
		contact.LastModified = time.Now()
	}

	s.contacts[contact.Id] = contact

	fmt.Println("We have created a salesforce contact", contact)

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(contact)
}

func (s *InMemoryStore) handleGetContact(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	s.contactsMutex.RLock()
	defer s.contactsMutex.RUnlock()

	contact, exists := s.contacts[id]
	if !exists {
		http.Error(w, "Contact not found", http.StatusNotFound)
		return
	}

	json.NewEncoder(w).Encode(contact)
}

func (s *InMemoryStore) handleUpdateContact(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var contact models.SalesforceContact
	if err := json.NewDecoder(r.Body).Decode(&contact); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.contactsMutex.Lock()
	defer s.contactsMutex.Unlock()

	existing, exists := s.contacts[id]
	if !exists {
		http.Error(w, "Contact not found", http.StatusNotFound)
		return
	}

	contact.CreatedDate = existing.CreatedDate
	contact.LastModified = time.Now()

	s.contacts[id] = contact
	json.NewEncoder(w).Encode(contact)
}

func (s *InMemoryStore) handleDeleteContact(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	s.contactsMutex.Lock()
	defer s.contactsMutex.Unlock()

	if _, exists := s.contacts[id]; !exists {
		http.Error(w, "Contact not found", http.StatusNotFound)
		return
	}

	delete(s.contacts, id)
	w.WriteHeader(http.StatusNoContent)
}
