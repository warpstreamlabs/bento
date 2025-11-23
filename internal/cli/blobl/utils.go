package blobl

import (
	"encoding/json"
	"fmt"
)

// FormatJSON formats a JSON string with 2-space indentation
func FormatJSON(jsonString string) JSONResponse {
	if jsonString == "" {
		return JSONResponse{
			Success: false,
			Result:  "",
			Error:   "JSON string cannot be empty",
		}
	}

	var parsed any
	if err := json.Unmarshal([]byte(jsonString), &parsed); err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString, // Return original on error
			Error:   fmt.Errorf("invalid JSON: %w", err).Error(),
		}
	}

	formatted, err := json.MarshalIndent(parsed, "", "  ")
	if err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString,
			Error:   fmt.Errorf("failed to format JSON: %w", err).Error(),
		}
	}

	return JSONResponse{
		Success: true,
		Result:  string(formatted),
	}
}

// MinifyJSON compacts a JSON string by removing whitespace
func MinifyJSON(jsonString string) JSONResponse {
	if jsonString == "" {
		return JSONResponse{
			Success: false,
			Result:  "",
			Error:   "JSON string cannot be empty",
		}
	}

	var parsed any
	if err := json.Unmarshal([]byte(jsonString), &parsed); err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString, // Return original on error
			Error:   fmt.Errorf("invalid JSON: %w", err).Error(),
		}
	}

	minified, err := json.Marshal(parsed)
	if err != nil {
		return JSONResponse{
			Success: false,
			Result:  jsonString,
			Error:   fmt.Errorf("failed to minify JSON: %w", err).Error(),
		}
	}

	return JSONResponse{
		Success: true,
		Result:  string(minified),
	}
}

// ValidateJSON checks if a string is valid JSON
func ValidateJSON(jsonString string) ValidationResponse {
	if jsonString == "" {
		return ValidationResponse{
			Valid: false,
			Error: "JSON string cannot be empty",
		}
	}

	var parsed any
	if err := json.Unmarshal([]byte(jsonString), &parsed); err != nil {
		return ValidationResponse{
			Valid: false,
			Error: fmt.Errorf("invalid JSON: %w", err).Error(),
		}
	}

	return ValidationResponse{
		Valid: true,
	}
}
