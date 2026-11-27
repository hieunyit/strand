package promote

import (
	"encoding/json"
	"strings"
)

// Lý do promote dừng chờ người vận hành (exit 10). Lưu trong Progress.DecisionReason và in ra dòng
// DecisionLinePrefix để Web hiện đúng lựa chọn cho từng trường hợp.
const (
	// DecisionFenceFailed: không fence được source cũ (không truy cập được hoặc thiếu --fence).
	DecisionFenceFailed = "fence_failed"
	// DecisionSourceLostAfterFence: đã fence, nhưng source mất kết nối trước khi chứng minh được target
	// đã apply hết log còn lại.
	DecisionSourceLostAfterFence = "source_lost_after_fence"
	// DecisionIndependentSequences: sequence độc lập không đọc được từ source sau fence.
	DecisionIndependentSequences = "independent_sequences"
)

// DecisionLinePrefix mở đầu dòng JSON (một dòng) mà `strand promote` in ra stdout khi dừng với exit 10.
const DecisionLinePrefix = "STRAND_PROMOTE_DECISION "

// Decision là bản máy đọc được của lần dừng exit 10. Không chứa DSN hay bí mật.
type Decision struct {
	Reason             string           `json:"reason"`
	Message            string           `json:"message,omitempty"`
	Fenced             bool             `json:"fenced"`
	CurrentStep        PromoteStep      `json:"current_step,omitempty"`
	CompletedSteps     []PromoteStep    `json:"completed_steps"`
	FailoverSummary    string           `json:"failover_summary,omitempty"`
	SuggestedSequences map[string]int64 `json:"suggested_sequences,omitempty"`
}

// DecisionFrom dựng Decision từ tiến trình đã lưu; message là thông báo lỗi exit 10.
func DecisionFrom(p *Progress, message string) Decision {
	d := Decision{
		Reason:             p.DecisionReason,
		Message:            message,
		Fenced:             p.IsStepCompleted(StepFenceSource),
		CurrentStep:        p.CurrentStep,
		CompletedSteps:     append([]PromoteStep{}, p.CompletedSteps...),
		FailoverSummary:    p.FailoverSummary,
		SuggestedSequences: p.SuggestedSequences,
	}
	if len(d.SuggestedSequences) == 0 {
		d.SuggestedSequences = nil
	}
	return d
}

// Line trả dòng DecisionLinePrefix + JSON (không xuống dòng bên trong).
func (d Decision) Line() string {
	b, _ := json.Marshal(d)
	return DecisionLinePrefix + string(b)
}

// ParseDecision tìm dòng quyết định cuối cùng trong output của `strand promote`.
func ParseDecision(out string) (Decision, bool) {
	for {
		i := strings.LastIndex(out, DecisionLinePrefix)
		if i < 0 {
			return Decision{}, false
		}
		line := out[i+len(DecisionLinePrefix):]
		if j := strings.IndexByte(line, '\n'); j >= 0 {
			line = line[:j]
		}
		var d Decision
		if json.Unmarshal([]byte(strings.TrimSpace(line)), &d) == nil && d.Reason != "" {
			return d, true
		}
		out = out[:i]
	}
}
