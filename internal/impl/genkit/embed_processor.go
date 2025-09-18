package genkit

import (
	"context"
	"fmt"

	"github.com/firebase/genkit/go/ai"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/warpstreamlabs/bento/public/service"
)

type embedderProcessor struct {
	prompt   *service.InterpolatedString
	embedder ai.Embedder
	config   any
}

func newEmbedderProcessor(pconf *service.ParsedConfig, config any, embedder ai.Embedder) (*embedderProcessor, error) {
	var prompt *service.InterpolatedString
	if pconf.Contains(promptField) {
		p, err := pconf.FieldInterpolatedString(promptField)
		if err != nil {
			return nil, err
		}
		prompt = p
	}

	return &embedderProcessor{
		embedder: embedder,
		config:   config,
		prompt:   prompt,
	}, nil
}

func (p *embedderProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	var docs []*ai.Document

	if p.prompt != nil {
		prompt, err := p.prompt.TryString(msg)
		if err != nil {
			return nil, err
		}

		dataArg := &dotprompt.DataArgument{}
		out, _ := msg.AsStructuredMut()
		if data, ok := out.(map[string]any); ok {
			dataArg.Input = data
		}

		dp := dotprompt.NewDotprompt(nil)
		rendered, err := dp.Render(prompt, dataArg, nil)
		if err != nil {
			return nil, err
		}

		for _, m := range rendered.Messages {
			parts, err := convertToPartPointers(m.Content)
			if err != nil {
				return nil, err
			}
			docs = append(docs, &ai.Document{
				Content:  parts,
				Metadata: m.Metadata,
			})
		}
	} else {
		content, err := msg.AsBytes()
		if err != nil {
			return nil, err
		}

		if len(content) == 0 {
			// Return empty result for empty content
			outMsg := msg.Copy()
			outMsg.SetStructuredMut([][]float32{})
			return service.MessageBatch{outMsg}, nil
		}

		parts := []*ai.Part{{Text: string(content)}}
		docs = append(docs, &ai.Document{
			Content: parts,
		})
	}

	metadata := make(map[string]any)
	msg.MetaWalk(func(key, value string) error {
		metadata[key] = value
		return nil
	})

	req := &ai.EmbedRequest{
		Input: docs,
	}

	resp, err := p.embedder.Embed(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to generate embedding: %w", err)
	}

	var embeddings [][]float32
	for _, embedding := range resp.Embeddings {
		embeddings = append(embeddings, embedding.Embedding)
	}

	outMsg := msg.Copy()
	outMsg.SetStructuredMut(embeddings)

	return service.MessageBatch{outMsg}, nil
}

func (p *embedderProcessor) Close(ctx context.Context) error {
	return nil
}
