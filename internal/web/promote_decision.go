package web

import (
	"encoding/json"

	"strand/internal/promote"
)

// promoteDecision là phần trang giải thích một lần promote dừng với exit 10: lý do, việc nên làm và giá trị
// sequence gợi ý (điền vào override_sequences bằng một nút, người vận hành vẫn xem lại trước khi chạy).
type promoteDecision struct {
	Reason       string                `json:"reason"`
	Title        string                `json:"title"`
	Explain      string                `json:"explain"`
	Options      []string              `json:"options"`
	Fenced       bool                  `json:"fenced"`
	Summary      string                `json:"failover_summary,omitempty"`
	Message      string                `json:"message,omitempty"`
	Sequences    []decisionSeq         `json:"suggested_sequences,omitempty"`
	OverrideJSON string                `json:"override_json,omitempty"`
	Failover     bool                  `json:"failover_offered"` // Failover là một lựa chọn hợp lý cho lý do này
	Steps        []promote.PromoteStep `json:"completed_steps"`
}

type decisionSeq struct {
	Name  string `json:"name"`
	Value int64  `json:"value"`
}

func newPromoteDecision(d promote.Decision) *promoteDecision {
	v := &promoteDecision{Reason: d.Reason, Fenced: d.Fenced, Summary: d.FailoverSummary, Message: d.Message, Steps: d.CompletedSteps}
	switch d.Reason {
	case promote.DecisionFenceFailed:
		v.Title = "The old source could not be fenced"
		v.Explain = "Strand could not make the old source read-only (it is unreachable, or no fence DSN was given). " +
			"Not seeing the source does not mean it is down: opening the target for writes now could leave two databases taking writes."
		v.Options = []string{
			"Fix the connection to the source and run Promote again (safest: nothing is lost).",
			"Or isolate the old source another way (power it off, block application traffic), then use Failover. Changes the target has not applied yet are lost.",
		}
		v.Failover = true
	case promote.DecisionSourceLostAfterFence:
		v.Title = "The source went away after it was fenced"
		v.Explain = "The old source is read-only, but Strand lost the connection before it could show that the target applied the rest of the log. " +
			"The target may be missing the last transactions."
		v.Options = []string{
			"Wait for the source to come back, then run Promote again: it resumes from this step and loses nothing.",
			"Or, if the source is gone for good, use Failover. Transactions the target has not applied yet are lost.",
		}
		v.Failover = true
	case promote.DecisionIndependentSequences:
		v.Title = "Sequence values need your approval"
		v.Explain = "The source was fenced and the target has the data, but some sequences that are not tied to a column could not be read from the source. " +
			"Strand suggests values from the last sync plus a safety margin."
		v.Options = []string{
			"Check the suggested values (raise them if the application used more since the last sync), put them in Override sequences, then run Promote again.",
		}
	default:
		v.Title = "Promote needs a decision"
		v.Explain = "Read the CLI report below."
	}
	if len(d.SuggestedSequences) > 0 {
		for _, n := range promote.SortedSequenceNames(d.SuggestedSequences) {
			v.Sequences = append(v.Sequences, decisionSeq{Name: n, Value: d.SuggestedSequences[n]})
		}
		b, _ := json.Marshal(d.SuggestedSequences) // map: khóa đã sắp xếp
		v.OverrideJSON = string(b)
		if d.Reason != promote.DecisionIndependentSequences {
			v.Options = append(v.Options, "Either way, independent sequences need values: use the suggested ones below (Override sequences) when you run again.")
		}
	}
	return v
}
