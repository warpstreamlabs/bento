package io

import (
	"context"
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
		"content": "` + testContent + `",
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
		"content": "${! json(\"content\") }",
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

	if string(content) != "dynamic content" {
		t.Errorf("Expected file content 'dynamic content', got '%s'", string(content))
	}
}

func newFileProcessorFromConfig(conf string) (*fileProcessor, error) {
	parsed, err := fileProcessorSpec().ParseYAML(conf, nil)
	if err != nil {
		return nil, err
	}

	return fileProcessorFromParsed(parsed, service.MockResources())
}
