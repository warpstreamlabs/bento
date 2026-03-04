package kafka

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xdg-go/scram"
)

func TestGenerateSCRAMCredentials_SHA256(t *testing.T) {
	creds, err := generateSCRAMCredentials(saslMechanismScramSha256, "testpassword")
	require.NoError(t, err)

	// Verify credentials structure
	assert.NotEmpty(t, creds.KeyFactors.Salt)
	assert.Equal(t, scramIterations, creds.KeyFactors.Iters)
	assert.Len(t, creds.StoredKey, 32) // SHA-256 produces 32-byte hash
	assert.Len(t, creds.ServerKey, 32)
}

func TestGenerateSCRAMCredentials_SHA512(t *testing.T) {
	creds, err := generateSCRAMCredentials(saslMechanismScramSha512, "testpassword")
	require.NoError(t, err)

	// Verify credentials structure
	assert.NotEmpty(t, creds.KeyFactors.Salt)
	assert.Equal(t, scramIterations, creds.KeyFactors.Iters)
	assert.Len(t, creds.StoredKey, 64) // SHA-512 produces 64-byte hash
	assert.Len(t, creds.ServerKey, 64)
}

func TestGenerateSCRAMCredentials_InvalidMechanism(t *testing.T) {
	_, err := generateSCRAMCredentials("INVALID", "testpassword")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported mechanism")
}

func TestGenerateSCRAMCredentials_DifferentSalts(t *testing.T) {
	// Two calls should generate different salts
	creds1, err := generateSCRAMCredentials(saslMechanismScramSha256, "testpassword")
	require.NoError(t, err)

	creds2, err := generateSCRAMCredentials(saslMechanismScramSha256, "testpassword")
	require.NoError(t, err)

	assert.NotEqual(t, creds1.KeyFactors.Salt, creds2.KeyFactors.Salt)
}

func TestNewSCRAMServer_SHA256(t *testing.T) {
	credLookup := func(username string) (scram.StoredCredentials, error) {
		return scram.StoredCredentials{}, nil
	}

	server, err := newSCRAMServer(saslMechanismScramSha256, credLookup)
	require.NoError(t, err)
	assert.NotNil(t, server)
}

func TestNewSCRAMServer_SHA512(t *testing.T) {
	credLookup := func(username string) (scram.StoredCredentials, error) {
		return scram.StoredCredentials{}, nil
	}

	server, err := newSCRAMServer(saslMechanismScramSha512, credLookup)
	require.NoError(t, err)
	assert.NotNil(t, server)
}

func TestNewSCRAMServer_InvalidMechanism(t *testing.T) {
	credLookup := func(username string) (scram.StoredCredentials, error) {
		return scram.StoredCredentials{}, nil
	}

	_, err := newSCRAMServer("INVALID", credLookup)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported SCRAM mechanism")
}

func TestConnectionState_InitialState(t *testing.T) {
	state := &connectionState{}

	assert.False(t, state.authenticated)
	assert.Nil(t, state.scramConversation)
	assert.Empty(t, state.scramMechanism)
	assert.Equal(t, int16(0), state.saslHandshakeVersion)
	assert.False(t, state.expectUnframedSASL)
}

func TestValidatePlainCredentials_Valid(t *testing.T) {
	creds := PlainCredentials{
		"user1": "password1",
		"user2": "password2",
	}

	// Valid credentials
	username, valid := validatePlainCredentials([]byte("\x00user1\x00password1"), creds)
	assert.True(t, valid)
	assert.Equal(t, "user1", username)

	username, valid = validatePlainCredentials([]byte("\x00user2\x00password2"), creds)
	assert.True(t, valid)
	assert.Equal(t, "user2", username)
}

func TestValidatePlainCredentials_Invalid(t *testing.T) {
	creds := PlainCredentials{
		"user1": "password1",
	}

	// Wrong password
	username, valid := validatePlainCredentials([]byte("\x00user1\x00wrongpassword"), creds)
	assert.False(t, valid)
	assert.Equal(t, "user1", username)

	// Unknown user
	username, valid = validatePlainCredentials([]byte("\x00unknown\x00password"), creds)
	assert.False(t, valid)
	assert.Equal(t, "unknown", username)

	// Malformed (missing parts)
	username, valid = validatePlainCredentials([]byte("user1"), creds)
	assert.False(t, valid)
	assert.Empty(t, username)
}

func TestProcessSCRAMStep_InvalidMechanism(t *testing.T) {
	state := &connectionState{
		scramMechanism: "INVALID",
	}
	scram256 := SCRAMCredentials{}
	scram512 := SCRAMCredentials{}

	result := processSCRAMStep("client-first-message", state, scram256, scram512)
	assert.Equal(t, saslAuthFailed, result.AuthStatus)
	assert.Error(t, result.Error)
	assert.Contains(t, result.Error.Error(), "unsupported SCRAM mechanism")
}

func TestProcessSCRAMStep_UserNotFound(t *testing.T) {
	state := &connectionState{
		scramMechanism: saslMechanismScramSha256,
	}
	scram256 := SCRAMCredentials{}
	scram512 := SCRAMCredentials{}

	// SCRAM client-first message format: n,,n=username,r=client-nonce
	result := processSCRAMStep("n,,n=unknownuser,r=12345", state, scram256, scram512)
	assert.Equal(t, saslAuthFailed, result.AuthStatus)
	assert.Error(t, result.Error)
}

func TestCreateSCRAMCredentialLookup_SHA256(t *testing.T) {
	creds, err := generateSCRAMCredentials(saslMechanismScramSha256, "testpass")
	require.NoError(t, err)

	scram256 := SCRAMCredentials{"testuser": creds}
	scram512 := SCRAMCredentials{}

	lookup := createSCRAMCredentialLookup(saslMechanismScramSha256, scram256, scram512)

	found, err := lookup("testuser")
	require.NoError(t, err)
	assert.Equal(t, creds.StoredKey, found.StoredKey)

	_, err = lookup("unknown")
	assert.Error(t, err)
}

func TestCreateSCRAMCredentialLookup_SHA512(t *testing.T) {
	creds, err := generateSCRAMCredentials(saslMechanismScramSha512, "testpass")
	require.NoError(t, err)

	scram256 := SCRAMCredentials{}
	scram512 := SCRAMCredentials{"testuser": creds}

	lookup := createSCRAMCredentialLookup(saslMechanismScramSha512, scram256, scram512)

	found, err := lookup("testuser")
	require.NoError(t, err)
	assert.Equal(t, creds.StoredKey, found.StoredKey)

	_, err = lookup("unknown")
	assert.Error(t, err)
}
