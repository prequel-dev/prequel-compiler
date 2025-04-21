package pqerr

import (
	"errors"
	"fmt"
)

type Pos struct{ Line, Col int }

type HasPos interface{ GetPos() Pos }
type HasRule interface {
	RuleId() string
	RuleHash() string
	CreId() string
}

type Error struct {
	Pos      Pos    // line / column
	RuleId   string // rule‑ID (may be empty)
	RuleHash string // rule‑hash (may be empty)
	CreId    string // cre‑ID (may be empty)
	Msg      string // optional extra text
	Err      error  // wrapped sentinel or nested error
}

func (e *Error) Error() string {
	txt := fmt.Sprintf("line=%d, col=%d", e.Pos.Line, e.Pos.Col)
	if e.CreId != "" {
		txt += fmt.Sprintf(", cre_id=%s", e.GetCreId())
	}
	if e.RuleId != "" {
		txt += fmt.Sprintf(", rule_id=%s", e.GetRuleId())
	}
	if e.RuleHash != "" {
		txt += fmt.Sprintf(", rule_hash=%s", e.GetRuleHash())
	}
	if e.Msg != "" {
		txt += ": " + e.Msg
	}
	if e.Err != nil {
		txt += ": " + e.Err.Error()
	}
	return txt
}

func (e *Error) Unwrap() error       { return e.Err }
func (e *Error) GetRuleId() string   { return e.RuleId }
func (e *Error) GetRuleHash() string { return e.RuleHash }
func (e *Error) GetCreId() string    { return e.CreId }
func (e *Error) GetPos() Pos         { return e.Pos }

func Wrap(pos Pos, ruleId, ruleHash, creId string, err error, msg ...string) error {
	if err == nil {
		return nil
	}
	var m string
	if len(msg) > 0 {
		m = msg[0]
	}
	return &Error{
		Pos:      pos,
		RuleId:   ruleId,
		RuleHash: ruleHash,
		CreId:    creId,
		Msg:      m,
		Err:      err,
	}
}

func PosOf(err error) (Pos, bool) {
	var hp HasPos
	if errors.As(err, &hp) {
		return hp.GetPos(), true
	}
	return Pos{}, false
}
