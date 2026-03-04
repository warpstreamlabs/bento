package kafka

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"fmt"
	"hash"

	"github.com/xdg-go/scram"
	"golang.org/x/crypto/pbkdf2"
)

// SCRAM authentication constants
const (
	scramSaltSize   = 16   // Size of random salt for SCRAM credentials
	scramIterations = 4096 // PBKDF2 iterations (standard for SCRAM)
)

// SASL mechanism typed constants
const (
	saslMechanismPlain       = "PLAIN"
	saslMechanismScramSha256 = "SCRAM-SHA-256"
	saslMechanismScramSha512 = "SCRAM-SHA-512"
)

// SCRAM key derivation labels (RFC 5802)
const (
	scramClientKeyLabel = "Client Key"
	scramServerKeyLabel = "Server Key"
)

// connectionState tracks per-connection authentication state.
// This is used by the Kafka server to track SASL authentication progress.
type connectionState struct {
	authenticated        bool
	scramConversation    *scram.ServerConversation // For SCRAM multi-step auth
	scramMechanism       string                    // Which SASL mechanism is being used
	saslHandshakeVersion int16                     // Version of SaslHandshake used (0 = legacy unframed, 1 = framed)
	expectUnframedSASL   bool                      // True if next bytes should be legacy unframed SASL
}

// saslAuthStatus represents the result of a SASL authentication step.
type saslAuthStatus int

const (
	// saslAuthInProgress indicates SCRAM authentication is continuing (more steps needed)
	saslAuthInProgress saslAuthStatus = iota
	// saslAuthSuccess indicates authentication completed successfully
	saslAuthSuccess
	// saslAuthFailed indicates authentication definitively failed
	saslAuthFailed
)

// PlainCredentials holds username/password pairs for PLAIN authentication.
type PlainCredentials map[string]string

// SCRAMCredentials holds pre-computed SCRAM credentials by username.
type SCRAMCredentials map[string]scram.StoredCredentials

// generateSCRAMCredentials generates SCRAM stored credentials from a plaintext password.
// Generates StoredKey and ServerKey following RFC 5802, compatible with Kafka.
// Username is not part of key derivation per RFC 5802 - it is only used to look up credentials.
func generateSCRAMCredentials(mechanism, password string) (scram.StoredCredentials, error) {
	// Generate a random salt
	salt := make([]byte, scramSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return scram.StoredCredentials{}, fmt.Errorf("failed to generate salt: %w", err)
	}

	// Determine hash function based on mechanism
	var hashFunc func() hash.Hash
	var keyLen int
	switch mechanism {
	case saslMechanismScramSha256:
		hashFunc = sha256.New
		keyLen = sha256.Size
	case saslMechanismScramSha512:
		hashFunc = sha512.New
		keyLen = sha512.Size
	default:
		return scram.StoredCredentials{}, fmt.Errorf("unsupported mechanism: %s", mechanism)
	}

	// Derive SaltedPassword using PBKDF2
	// SaltedPassword = PBKDF2(password, salt, iterations, keyLen)
	saltedPassword := pbkdf2.Key([]byte(password), salt, scramIterations, keyLen, hashFunc)

	// Compute ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKeyHMAC := hmac.New(hashFunc, saltedPassword)
	clientKeyHMAC.Write([]byte(scramClientKeyLabel))
	clientKey := clientKeyHMAC.Sum(nil)

	// Compute StoredKey = Hash(ClientKey)
	storedKeyHash := hashFunc()
	storedKeyHash.Write(clientKey)
	storedKey := storedKeyHash.Sum(nil)

	// Compute ServerKey = HMAC(SaltedPassword, "Server Key")
	serverKeyHMAC := hmac.New(hashFunc, saltedPassword)
	serverKeyHMAC.Write([]byte(scramServerKeyLabel))
	serverKey := serverKeyHMAC.Sum(nil)

	// Return credentials in xdg-go/scram format.
	// Salt must be stored as raw bytes (in the string). The xdg-go SCRAM
	// library Base64-encodes the salt when emitting the server-first message,
	// so the salt is stored as raw bytes in the string.
	return scram.StoredCredentials{
		KeyFactors: scram.KeyFactors{
			Salt:  string(salt),
			Iters: scramIterations,
		},
		StoredKey: storedKey,
		ServerKey: serverKey,
	}, nil
}

// validatePlainCredentials validates PLAIN SASL credentials.
// PLAIN format: [authzid] \0 username \0 password
// Returns (username, valid).
func validatePlainCredentials(authBytes []byte, credentials PlainCredentials) (string, bool) {
	// Split by null bytes
	parts := bytes.Split(authBytes, []byte{0})

	if len(parts) < 3 {
		return "", false
	}

	// parts[0] is authzid (authorization identity), usually empty
	// parts[1] is username (authentication identity)
	// parts[2] is password
	username := string(parts[1])
	password := string(parts[2])

	expectedPassword, exists := credentials[username]
	if !exists {
		return username, false
	}

	// Use constant-time comparison to prevent timing attacks
	if subtle.ConstantTimeCompare([]byte(expectedPassword), []byte(password)) != 1 {
		return username, false
	}

	return username, true
}

// addPlainCredentials adds a username/password pair into the provided PlainCredentials map.
func addPlainCredentials(creds PlainCredentials, username, password string) {
	creds[username] = password
}

// addSCRAMCredentials generates SCRAM stored credentials for the given mechanism and
// password, then inserts them into the provided SCRAMCredentials map under the username.
func addSCRAMCredentials(mechanism string, credsMap SCRAMCredentials, username, password string) error {
	creds, err := generateSCRAMCredentials(mechanism, password)
	if err != nil {
		return err
	}
	credsMap[username] = creds
	return nil
}

// newSCRAMServer creates a new SCRAM server for the specified mechanism.
func newSCRAMServer(mechanism string, credLookup scram.CredentialLookup) (*scram.Server, error) {
	switch mechanism {
	case saslMechanismScramSha256:
		return scram.SHA256.NewServer(credLookup)
	case saslMechanismScramSha512:
		return scram.SHA512.NewServer(credLookup)
	default:
		return nil, fmt.Errorf("unsupported SCRAM mechanism: %s", mechanism)
	}
}

// createSCRAMCredentialLookup creates a credential lookup function for SCRAM authentication.
func createSCRAMCredentialLookup(mechanism string, scram256Creds, scram512Creds SCRAMCredentials) scram.CredentialLookup {
	var credsMap SCRAMCredentials
	if mechanism == saslMechanismScramSha512 {
		credsMap = scram512Creds
	} else {
		credsMap = scram256Creds
	}

	return func(username string) (scram.StoredCredentials, error) {
		creds, ok := credsMap[username]
		if !ok {
			return scram.StoredCredentials{}, fmt.Errorf("user not found: %s", username)
		}
		return creds, nil
	}
}

// SCRAMAuthResult represents the result of a SCRAM authentication step.
type SCRAMAuthResult struct {
	ServerMessage string         // Server's response message to send to client
	AuthStatus    saslAuthStatus // Authentication status (success, failed, or in-progress)
	Error         error          // Non-nil if there was a processing error
}

// Status returns the authentication status from the result.
func (r SCRAMAuthResult) Status() saslAuthStatus {
	return r.AuthStatus
}

// processSCRAMStep processes a SCRAM authentication step.
// This handles the multi-step SCRAM conversation, creating the server conversation
// if needed and processing the client message.
// The state's SCRAMConversation will be initialized if nil.
func processSCRAMStep(clientMessage string, state *connectionState, scram256Creds, scram512Creds SCRAMCredentials) SCRAMAuthResult {
	// If this is the first message (no conversation yet), create one
	if state.scramConversation == nil {
		credLookup := createSCRAMCredentialLookup(state.scramMechanism, scram256Creds, scram512Creds)
		scramServer, err := newSCRAMServer(state.scramMechanism, credLookup)
		if err != nil {
			return SCRAMAuthResult{
				AuthStatus: saslAuthFailed,
				Error:      fmt.Errorf("failed to create SCRAM server: %w", err),
			}
		}
		state.scramConversation = scramServer.NewConversation()
	}

	// Process the client message
	serverMessage, err := state.scramConversation.Step(clientMessage)
	if err != nil {
		return SCRAMAuthResult{
			AuthStatus: saslAuthFailed,
			Error:      fmt.Errorf("authentication failed: %w", err),
		}
	}

	// Check if conversation is complete
	if state.scramConversation.Done() {
		if state.scramConversation.Valid() {
			return SCRAMAuthResult{
				ServerMessage: serverMessage,
				AuthStatus:    saslAuthSuccess,
			}
		}
		return SCRAMAuthResult{
			ServerMessage: serverMessage,
			AuthStatus:    saslAuthFailed,
			Error:         fmt.Errorf("invalid credentials"),
		}
	}

	// Conversation continues (need more steps)
	return SCRAMAuthResult{
		ServerMessage: serverMessage,
		AuthStatus:    saslAuthInProgress,
	}
}
