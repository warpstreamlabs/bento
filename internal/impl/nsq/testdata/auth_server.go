package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// AuthResponse represents the NSQ authentication response format.
// See: https://nsq.io/components/nsqd.html#auth
type AuthResponse struct {
	TTL            int              `json:"ttl"`
	Identity       string           `json:"identity"`
	IdentityURL    string           `json:"identity_url"`
	Authorizations []Authorization  `json:"authorizations"`
}

// Authorization defines permissions for topics and channels.
type Authorization struct {
	Topic       string   `json:"topic"`
	Channels    []string `json:"channels"`
	Permissions []string `json:"permissions"`
}

// ErrorResponse represents an error response for unauthorized requests.
type ErrorResponse struct {
	Error string `json:"error"`
}

const (
	// testSecret is the expected secret for authentication.
	// In a production environment, this would be loaded from environment variables.
	testSecret = "testSecret123"

	// Server configuration
	serverAddr = ":8080"
)

// authHandler handles NSQ authentication requests.
// NSQ sends GET requests with the secret as a query parameter.
// See: https://nsq.io/components/nsqd.html#auth
func authHandler(w http.ResponseWriter, r *http.Request) {
	// NSQ auth protocol uses GET requests with query parameters
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		log.Printf("Authentication failed - method not allowed: %s", r.Method)
		return
	}

	// Extract secret from query parameter (this is how NSQ sends it)
	secret := r.URL.Query().Get("secret")

	// Set response content type
	w.Header().Set("Content-Type", "application/json")

	// Validate the secret
	if secret == "" {
		w.WriteHeader(http.StatusForbidden)
		response := ErrorResponse{Error: "secret parameter required"}
		json.NewEncoder(w).Encode(response)
		log.Printf("Authentication failed - no secret provided")
		return
	}

	if secret != testSecret {
		// Return 403 Forbidden for invalid credentials
		w.WriteHeader(http.StatusForbidden)
		response := ErrorResponse{Error: "unauthorized"}
		json.NewEncoder(w).Encode(response)
		log.Printf("Authentication failed - invalid secret: %s", secret)
		return
	}

	// Return successful authentication response
	w.WriteHeader(http.StatusOK)
	response := AuthResponse{
		TTL:         3600, // Token valid for 1 hour
		Identity:    "test-user",
		IdentityURL: "",
		Authorizations: []Authorization{
			{
				Topic:       ".*", // Allow all topics (regex pattern)
				Channels:    []string{".*"}, // Allow all channels (regex pattern)
				Permissions: []string{"subscribe", "publish"}, // Full permissions
			},
		},
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("Error encoding response: %v", err)
	}

	log.Printf("Authentication successful for identity: %s", response.Identity)
}

// healthHandler provides a simple health check endpoint.
func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK"))
}

func main() {
	// Register handlers
	http.HandleFunc("/", authHandler)
	http.HandleFunc("/health", healthHandler)

	log.Printf("NSQ Auth Server starting on %s", serverAddr)
	log.Printf("Expected secret: %s", testSecret)

	// Start the HTTP server
	if err := http.ListenAndServe(serverAddr, nil); err != nil {
		log.Fatalf("Server failed to start: %v", err)
	}
}
