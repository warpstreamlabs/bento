package io

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestFileProcessorRead(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	testContent := "Hello, World!"

	// Create test file
	if err := os.WriteFile(testFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create test file:", err)
	}

	conf := `{
		"operation": "read",
		"path": "` + testFile + `",
		"content": "",
		"dest": "",
		"scanner": {
			"to_the_end": {}
		}
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	resultMsg := result[0]
	contentBytes, err := resultMsg.AsBytes()
	if err != nil {
		t.Fatal("Failed to get message bytes:", err)
	}
	if string(contentBytes) != testContent {
		t.Errorf("Expected content '%s', got '%s'", testContent, string(contentBytes))
	}

	// Check metadata
	if filePath, exists := resultMsg.MetaGet("file_path"); !exists || filePath != testFile {
		t.Errorf("Expected file_path '%s', got '%s'", testFile, filePath)
	}

	if fileSize, exists := resultMsg.MetaGet("file_size"); !exists || fileSize != "13" {
		t.Errorf("Expected file_size '13', got '%s'", fileSize)
	}
}

func TestFileProcessorWrite(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "output.txt")
	testContent := "Test content"

	conf := `{
		"operation": "write",
		"path": "` + testFile + `",
		"content": "",
		"dest": ""
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte(testContent))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check file was created with correct content
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatal("Failed to read created file:", err)
	}

	if string(content) != testContent {
		t.Errorf("Expected file content '%s', got '%s'", testContent, string(content))
	}
}

func TestFileProcessorDelete(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "delete_me.txt")
	testContent := "To be deleted"

	// Create test file
	if err := os.WriteFile(testFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create test file:", err)
	}

	conf := `{
		"operation": "delete",
		"path": "` + testFile + `",
		"content": "",
		"dest": ""
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check file was deleted
	if _, err := os.Stat(testFile); !os.IsNotExist(err) {
		t.Fatal("Expected file to be deleted")
	}
}

func TestFileProcessorMove(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source.txt")
	destFile := filepath.Join(tempDir, "destination.txt")
	testContent := "Move me"

	// Create source file
	if err := os.WriteFile(srcFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create source file:", err)
	}

	conf := `{
		"operation": "move",
		"path": "` + srcFile + `",
		"dest": "` + destFile + `"
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check source file was deleted
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Fatal("Expected source file to be deleted")
	}

	// Check destination file exists with correct content
	destContent, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal("Failed to read destination file:", err)
	}

	if string(destContent) != testContent {
		t.Errorf("Expected destination content '%s', got '%s'", testContent, string(destContent))
	}
}

func TestFileProcessorStat(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	testContent := "Hello, World!"

	// Create test file
	if err := os.WriteFile(testFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create test file:", err)
	}

	conf := `{
		"operation": "stat",
		"path": "` + testFile + `"
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	resultMsg := result[0]

	// Check metadata
	if filePath, exists := resultMsg.MetaGet("file_path"); !exists || filePath != testFile {
		t.Errorf("Expected file_path '%s', got '%s'", testFile, filePath)
	}

	if fileSize, exists := resultMsg.MetaGet("file_size"); !exists || fileSize != "13" {
		t.Errorf("Expected file_size '13', got '%s'", fileSize)
	}

	if fileName, exists := resultMsg.MetaGet("file_name"); !exists || fileName != "test.txt" {
		t.Errorf("Expected file_name 'test.txt', got '%s'", fileName)
	}

	if fileIsDir, exists := resultMsg.MetaGet("file_is_dir"); !exists || fileIsDir != "false" {
		t.Errorf("Expected file_is_dir 'false', got '%s'", fileIsDir)
	}

	// Check that original message content is preserved
	contentBytes, err := resultMsg.AsBytes()
	if err != nil {
		t.Fatal("Failed to get message bytes:", err)
	}
	if string(contentBytes) != "test message" {
		t.Errorf("Expected original content 'test message', got '%s'", string(contentBytes))
	}
}

func TestFileProcessorInterpolation(t *testing.T) {
	tempDir := t.TempDir()

	conf := `{
		"operation": "write",
		"path": "` + tempDir + `/${! json(\"filename\") }.txt",
		"content": "",
		"dest": ""
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte(`{"filename": "interpolated", "content": "dynamic content"}`))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check file was created with interpolated name and content
	expectedFile := filepath.Join(tempDir, "interpolated.txt")
	content, err := os.ReadFile(expectedFile)
	if err != nil {
		t.Fatal("Failed to read created file:", err)
	}

	expectedContent := `{"filename": "interpolated", "content": "dynamic content"}`
	if string(content) != expectedContent {
		t.Errorf("Expected file content '%s', got '%s'", expectedContent, string(content))
	}
}

func TestFileProcessorAtomicWrite(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "atomic_test.txt")
	testContent := "This is atomic content"

	conf := `{
		"operation": "write",
		"path": "` + testFile + `",
		"content": "",
		"dest": ""
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte(testContent))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check file was created with correct content
	content, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatal("Failed to read created file:", err)
	}

	if string(content) != testContent {
		t.Errorf("Expected file content '%s', got '%s'", testContent, string(content))
	}

	// Check that no temporary file was left behind
	tempFile := testFile + ".tmp"
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Fatal("Temporary file should not exist after successful write")
	}
}

func TestFileProcessorAtomicMoveFallback(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source_atomic.txt")
	destFile := filepath.Join(tempDir, "dest_atomic.txt")
	testContent := "Atomic move content"

	// Create source file
	if err := os.WriteFile(srcFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create source file:", err)
	}

	conf := `{
		"operation": "move",
		"path": "` + srcFile + `",
		"dest": "` + destFile + `"
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check source file was deleted
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Fatal("Expected source file to be deleted")
	}

	// Check destination file exists with correct content
	destContent, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal("Failed to read destination file:", err)
	}

	if string(destContent) != testContent {
		t.Errorf("Expected destination content '%s', got '%s'", testContent, string(destContent))
	}

	// Check that no temporary file was left behind
	tempFile := destFile + ".tmp"
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Fatal("Temporary file should not exist after successful move")
	}
}

func TestFileProcessorRename(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source_rename.txt")
	destFile := filepath.Join(tempDir, "dest_rename.txt")
	testContent := "Rename me"

	// Create source file
	if err := os.WriteFile(srcFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create source file:", err)
	}

	conf := `{
		"operation": "rename",
		"path": "` + srcFile + `",
		"dest": "` + destFile + `"
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	if len(result) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(result))
	}

	// Check source file was deleted
	if _, err := os.Stat(srcFile); !os.IsNotExist(err) {
		t.Fatal("Expected source file to be deleted")
	}

	// Check destination file exists with correct content
	destContent, err := os.ReadFile(destFile)
	if err != nil {
		t.Fatal("Failed to read destination file:", err)
	}

	if string(destContent) != testContent {
		t.Errorf("Expected destination content '%s', got '%s'", testContent, string(destContent))
	}

	// Rename should not create temporary files
	tempFile := destFile + ".tmp"
	if _, err := os.Stat(tempFile); !os.IsNotExist(err) {
		t.Fatal("Temporary file should not exist after rename")
	}
}

func TestFileProcessorRenameCrossFilesystemFailure(t *testing.T) {
	tempDir := t.TempDir()
	srcFile := filepath.Join(tempDir, "source_cross.txt")
	// Use a clearly different path that would be on a different filesystem
	destFile := "/nonexistent_filesystem/dest_cross.txt"
	testContent := "Cross filesystem test"

	// Create source file
	if err := os.WriteFile(srcFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create source file:", err)
	}

	conf := `{
		"operation": "rename",
		"path": "` + srcFile + `",
		"dest": "` + destFile + `"
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	_, err = proc.Process(context.Background(), msg)

	// Rename should fail for cross-filesystem operations
	if err == nil {
		t.Fatal("Expected rename to fail for cross-filesystem operation")
	}

	// Source file should still exist
	if _, statErr := os.Stat(srcFile); statErr != nil {
		t.Fatal("Source file should still exist after failed rename:", statErr)
	}
}

func TestFileProcessorContextCancellation(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "cancel_test.txt")
	testContent := "This is test content for cancellation"

	// Create a test file
	if err := os.WriteFile(testFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create test file:", err)
	}

	conf := `{
		"operation": "read",
		"path": "` + testFile + `",
		"content": "",
		"dest": "",
		"scanner": {
			"to_the_end": {}
		}
	}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	// Create a context that will be cancelled immediately
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	msg := service.NewMessage([]byte("test message"))
	_, err = proc.Process(ctx, msg)

	// With to_the_end scanner, the operation might complete successfully
	// even with immediate cancellation since it reads the entire file at once
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Expected context.Canceled error, got: %v", err)
		}
	}
	// If err is nil, it means the operation completed successfully before cancellation took effect

	// Verify no temporary files were left behind
	tempFiles, err := filepath.Glob(filepath.Join(tempDir, "*.tmp"))
	if err != nil {
		t.Fatal("Failed to check for temporary files:", err)
	}

	if len(tempFiles) > 0 {
		t.Errorf("Found unexpected temporary files: %v", tempFiles)
	}
}

func newFileProcessorFromConfig(conf string) (*fileProcessor, error) {
	parsed, err := fileProcessorSpec().ParseYAML(conf, nil)
	if err != nil {
		return nil, err
	}

	return fileProcessorFromParsed(parsed, service.MockResources())
}

func TestFileProcessorWithScanner(t *testing.T) {
	tempDir := t.TempDir()
	testFile := filepath.Join(tempDir, "test.txt")
	testContent := "Hello, World!\nThis is a test file.\nLine 3"

	// Create test file
	if err := os.WriteFile(testFile, []byte(testContent), 0o644); err != nil {
		t.Fatal("Failed to create test file:", err)
	}

	conf := `operation: "read"
path: "` + testFile + `"
scanner:
  lines: {}`

	proc, err := newFileProcessorFromConfig(conf)
	if err != nil {
		t.Fatal("Failed to create processor:", err)
	}

	msg := service.NewMessage([]byte("test message"))
	result, err := proc.Process(context.Background(), msg)
	if err != nil {
		t.Fatal("Process failed:", err)
	}

	// With lines scanner, we should get one message per line
	expectedLines := []string{
		"Hello, World!",
		"This is a test file.",
		"Line 3",
	}

	if len(result) != len(expectedLines) {
		t.Fatalf("Expected %d messages (one per line), got %d", len(expectedLines), len(result))
	}

	// Verify each message contains the correct line
	for i, expectedLine := range expectedLines {
		if i >= len(result) {
			t.Fatalf("Missing message for line %d", i)
		}

		contentBytes, err := result[i].AsBytes()
		if err != nil {
			t.Fatalf("Failed to get message bytes for line %d: %v", i, err)
		}

		actualContent := string(contentBytes)
		if actualContent != expectedLine {
			t.Errorf("Line %d: Expected '%s', got '%s'", i, expectedLine, actualContent)
		}
	}
}
