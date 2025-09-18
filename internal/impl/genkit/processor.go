package genkit

import (
	"context"
	"fmt"
	"strings"

	"github.com/firebase/genkit/go/ai"
	"github.com/firebase/genkit/go/core"
	"github.com/firebase/genkit/go/genkit"
	"github.com/google/dotprompt/go/dotprompt"
	"github.com/warpstreamlabs/bento/public/service"
)

type genkitPromptProcessor struct {
	instance *genkit.Genkit
	model    ai.Model

	config any
	prompt *service.InterpolatedString

	outputSchema map[string]any
	outputFormat string

	tools []ai.ToolRef

	outputInstructions *service.InterpolatedString

	flow *core.Flow[*service.Message, service.MessageBatch, struct{}]
}

func newGenkitProcessor(pconf *service.ParsedConfig, res *service.Resources, config any, model ai.Model) (*genkitPromptProcessor, error) {
	g, err := LoadInstance(res)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve shared genkit instance: %w", err)
	}

	proc := &genkitPromptProcessor{
		instance: g,
		model:    model,
		config:   config,
	}

	proc.prompt, err = pconf.FieldInterpolatedString(promptField)
	if err != nil {
		return nil, err
	}

	proc.outputFormat, err = pconf.FieldString(outputField, outputFormatField)
	if err != nil {
		return nil, err
	}

	if pconf.Contains(outputField, outputInstructionField) {
		proc.outputInstructions, err = pconf.FieldInterpolatedString(outputField, outputInstructionField)
		if err != nil {
			return nil, err
		}
	}

	if pconf.Contains(outputField, outputInstructionField) {
		proc.outputSchema, err = extractSchema(pconf)
		if err != nil {
			return nil, err
		}
	}

	if pconf.Contains(toolsField) {

		toolDefinitions, err := pconf.FieldObjectList(toolsField)
		if err != nil {
			return nil, err
		}

		for _, toolDef := range toolDefinitions {
			ownedProc, err := toolDef.FieldProcessor("processor")
			if err != nil {
				return nil, err
			}

			toolName, err := toolDef.FieldString("name")
			if err != nil {
				return nil, err
			}

			toolDesc, err := toolDef.FieldString("description")
			if err != nil {
				return nil, err
			}

			proc.tools = append(proc.tools, convertProcessorToTool(g, toolName, toolDesc, ownedProc))
		}
	}

	p := res.Path()
	flowName := strings.Join(append([]string{"bento", model.Name()}, p...), ".")
	proc.flow = genkit.DefineFlow(g, flowName, proc.process)
	return proc, nil
}

func (p *genkitPromptProcessor) Process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
	return p.flow.Run(ctx, msg)
}

func (p *genkitPromptProcessor) opts() ([]ai.GenerateOption, error) {
	var modelOpts []ai.GenerateOption
	if len(p.tools) > 0 {
		modelOpts = append(modelOpts, ai.WithToolChoice(ai.ToolChoiceAuto), ai.WithTools(p.tools...))
	}

	if p.model == nil {
		return nil, fmt.Errorf("could not retrieve model")
	}
	modelOpts = append(modelOpts, ai.WithModel(p.model))

	if p.config != nil {
		modelOpts = append(modelOpts, ai.WithConfig(p.config))
	}

	if p.outputSchema != nil {
		modelOpts = append(modelOpts, ai.WithOutputType(p.outputSchema))
	}

	modelOpts = append(modelOpts, ai.WithOutputFormat(p.outputFormat))

	return modelOpts, nil
}

func (p *genkitPromptProcessor) process(ctx context.Context, msg *service.Message) (service.MessageBatch, error) {
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
	doc, err := dp.Render(prompt, dataArg, nil)
	if err != nil {
		return nil, err
	}

	modelOpts, err := p.opts()
	if err != nil {
		return nil, err
	}

	var messages []*ai.Message
	for _, m := range doc.Messages {
		genkitMsg, err := convertDotpromptToGenkit(m)
		if err != nil {
			return nil, err
		}
		messages = append(messages, genkitMsg)
	}
	modelOpts = append(modelOpts, ai.WithMessages(messages...))

	if p.outputInstructions != nil {
		instructions, err := p.outputInstructions.TryString(msg)
		if err != nil {
			return nil, err
		}
		modelOpts = append(modelOpts, ai.WithOutputInstructions(instructions))
	}

	resp, err := genkit.Generate(ctx, p.instance, modelOpts...)
	if err != nil {
		return nil, err
	}

	return convertToBento(resp.Message)
}

func (p *genkitPromptProcessor) Close(ctx context.Context) error {
	return nil
}
