package pipeline

import (
	"context"
	"log/slog"
)

// ObserveStage runs per iteration after ToolStage. Drains InjectCh,
// accumulates final content when no tool calls, tracks block replies.
// Does NOT implement StageWithResult — never controls flow.
type ObserveStage struct {
	deps *PipelineDeps
}

// NewObserveStage creates an ObserveStage.
func NewObserveStage(deps *PipelineDeps) *ObserveStage {
	return &ObserveStage{deps: deps}
}

func (s *ObserveStage) Name() string { return "observe" }

// Execute drains injected messages, accumulates final content + block replies.
func (s *ObserveStage) Execute(_ context.Context, state *RunState) error {
	// 1. Drain InjectCh (non-blocking) — messages from tool side effects, subagent results
	if s.deps.DrainInjectCh != nil {
		for _, msg := range s.deps.DrainInjectCh() {
			state.Messages.AppendPending(msg)
		}
	}

	resp := state.Think.LastResponse
	if resp == nil {
		return nil
	}

	// 2. Track block replies (every response with content counts as a block)
	if resp.Content != "" {
		state.Observe.BlockReplies++
		state.Observe.LastBlockReply = resp.Content
	}

	// 3. Accumulate final content when no tool calls (final answer)
	if len(resp.ToolCalls) == 0 {
		contentPreview := resp.Content
		if len(contentPreview) > 200 {
			contentPreview = contentPreview[:200]
		}
		thinkingPreview := resp.Thinking
		if len(thinkingPreview) > 200 {
			thinkingPreview = thinkingPreview[:200]
		}
		slog.Warn("pipeline observe: capturing final response",
			"run_id", state.RunID,
			"phase", resp.Phase,
			"finish_reason", resp.FinishReason,
			"content_len", len(resp.Content),
			"content_preview", contentPreview,
			"thinking_len", len(resp.Thinking),
			"thinking_preview", thinkingPreview,
			"tool_calls", len(resp.ToolCalls),
		)
		state.Observe.FinalContent = resp.Content
		state.Observe.FinalThinking = resp.Thinking
	}


