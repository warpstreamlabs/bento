package blobl

import (
	"encoding/json"
	"fmt"

	"github.com/warpstreamlabs/bento/internal/bloblang"
	"github.com/warpstreamlabs/bento/internal/bloblang/parser"
)

type ExecutionResult struct {
	Result       any `json:"result"`
	MappingError any `json:"mapping_error"`
	ParseError   any `json:"parse_error"`
}

type ValidationResult struct {
	IsValid    bool `json:"is_valid"`
	ParseError any  `json:"parse_error"`
}

type BloblangEnvironment struct {
	env *bloblang.Environment
}

func NewBloblangEnvironment() *BloblangEnvironment {
	return &BloblangEnvironment{
		env: bloblang.GlobalEnvironment(),
	}
}

func (p *BloblangEnvironment) ExecuteMapping(input, mapping string) (*ExecutionResult, error) {
	result := &ExecutionResult{
		Result:       nil,
		MappingError: nil,
		ParseError:   nil,
	}

	if input == "" {
		result.MappingError = "Input JSON string cannot be empty"
		return result, nil
	}

	if mapping == "" {
		result.ParseError = "Mapping string cannot be empty"
		return result, nil
	}

	exec, err := p.env.NewMapping(mapping)
	if err != nil {
		if perr, ok := err.(*parser.Error); ok {
			result.ParseError = fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(mapping)))
		} else {
			result.ParseError = fmt.Sprintf("mapping error: %v", err.Error())
		}
		return result, nil
	}

	execCache := newExecCache()
	output, err := execCache.executeMapping(exec, false, true, []byte(input))
	if err != nil {
		result.MappingError = fmt.Sprintf("execution error: %v", err.Error())
	} else {
		result.Result = output
	}

	return result, nil
}

func (p *BloblangEnvironment) ValidateMapping(mapping string) (*ValidationResult, error) {
	result := &ValidationResult{
		IsValid:    true,
		ParseError: nil,
	}

	if mapping == "" {
		result.IsValid = false
		result.ParseError = "Mapping string cannot be empty"
		return result, nil
	}

	_, err := p.env.NewMapping(mapping)
	if err != nil {
		result.IsValid = false
		if perr, ok := err.(*parser.Error); ok {
			result.ParseError = fmt.Sprintf("failed to parse mapping: %v", perr.ErrorAtPositionStructured("", []rune(mapping)))
		} else {
			result.ParseError = fmt.Sprintf("mapping error: %v", err.Error())
		}
	}

	return result, nil
}

func (p *BloblangEnvironment) GetSyntax() (bloblangSyntax, error) {
	return GenerateBloblangSyntax(p.env)
}

func (p *BloblangEnvironment) ExecuteMappingAsJSON(input, mapping string) (string, error) {
	result, err := p.ExecuteMapping(input, mapping)
	if err != nil {
		return "", err
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("failed to marshal result: %w", err)
	}

	return string(jsonBytes), nil
}
