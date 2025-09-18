package genkit

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/genkit"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/warpstreamlabs/bento/public/bloblang"
	"github.com/warpstreamlabs/bento/public/service"
)

func convertToGenkit(msg *service.Message) (*ai.Message, error) {
	part, err := toGenkitPart(msg)
	if err != nil {
		return nil, err
	}

	meta := make(map[string]any)
	msg.MetaWalk(func(key, value string) error {
		meta[key] = value
		return nil
	})

	return ai.NewMessage(ai.RoleUser, meta, part), nil
}

func convertToBento(resp *ai.Message) ([]*service.Message, error) {
	var messages []*service.Message

	for _, p := range resp.Content {
		msg := service.NewMessage(nil)
		if p.IsCustom() && p.Custom != nil {
			msg.SetStructuredMut(p.Custom)
		} else if p.Text != "" {
			msg.SetBytes([]byte(p.Text))
		} else {
			continue
		}

		if p.Metadata != nil {
			for k, v := range p.Metadata {
				msg.MetaSetMut(k, v)
			}
		}

		messages = append(messages, msg)
	}

	return messages, nil
}
func toGenkitPart(msg *service.Message) (*ai.Part, error) {
	res, err := msg.AsBytes()
	if err != nil {
		return nil, err
	}

	if json.Valid(res) {
		return ai.NewJSONPart(string(res)), nil
	}

	contentType := http.DetectContentType(res)

	if strings.HasPrefix(contentType, "image/") ||
		strings.HasPrefix(contentType, "audio/") ||
		strings.HasPrefix(contentType, "video/") {
		base64Data := base64.StdEncoding.EncodeToString(res)
		dataURL := fmt.Sprintf("data:%s;base64,%s", contentType, base64Data)
		return ai.NewMediaPart(contentType, dataURL), nil
	}

	if strings.HasPrefix(contentType, "text/") {
		return ai.NewTextPart(string(res)), nil
	}

	// Fallback to raw bytes for other binary data
	base64Data := base64.StdEncoding.EncodeToString(res)
	return ai.NewDataPart(base64Data), nil
}

func convertToPartPointers(parts []dotprompt.Part) ([]*ai.Part, error) {
	result := make([]*ai.Part, len(parts))
	for i, part := range parts {
		switch p := part.(type) {
		case *dotprompt.TextPart:
			if p.Text != "" {
				result[i] = ai.NewTextPart(p.Text)
			}
		case *dotprompt.MediaPart:
			result[i] = ai.NewMediaPart(p.Media.ContentType, p.Media.URL)
		}
	}
	return result, nil
}

func convertDotpromptToGenkit(msg dotprompt.Message) (*ai.Message, error) {
	parts, err := convertToPartPointers(msg.Content)
	if err != nil {
		return nil, err
	}
	var role ai.Role
	switch msg.Role {
	case dotprompt.RoleSystem:
		role = ai.RoleSystem
	case dotprompt.RoleUser:
		role = ai.RoleUser
	case dotprompt.RoleModel:
		role = ai.RoleModel
	case dotprompt.RoleTool:
		role = ai.RoleTool
	default:
		return nil, fmt.Errorf("invalid role %s", msg.Role)
	}
	return ai.NewMessage(role, nil, parts...), nil

}

func convertProcessorToTool(g *genkit.Genkit, name, desc string, proc *service.OwnedProcessor) ai.Tool {
	return genkit.DefineTool(g, name, desc,
		func(ctx *ai.ToolContext, input any) (any, error) {
			msg := service.NewMessage(nil)
			switch t := input.(type) {
			case []byte:
				msg.SetBytes(t)
			case string:
				msg.SetBytes([]byte(t))
			default:
				msg.SetStructured(input)
			}

			batch, err := proc.Process(ctx, msg)
			if err != nil {
				return nil, err
			}

			resp := make([]any, 0, len(batch))
			batchErr := batch.WalkWithBatchedErrors(func(i int, m *service.Message) error {
				if err := m.GetError(); err != nil {
					return err
				}

				out, err := m.AsStructuredMut()
				if err != nil {
					return err
				}

				resp = append(resp, out)
				return nil
			})

			if len(resp) == 1 {
				return resp[0], batchErr
			}

			return resp, batchErr

		},
	)
}

func convertBloblangFunctionToTool(name string, exec *bloblang.Executor) ai.Tool {
	return genkit.DefineTool(nil, "weather", "Get current weather for a location",
		func(ctx *ai.ToolContext, input any) (any, error) {
			return exec.Query(input)
		},
	)
}
