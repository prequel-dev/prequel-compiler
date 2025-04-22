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
	File() string
}

type Error struct {
	Pos      Pos    // line / column
	RuleId   string // rule‑ID (may be empty)
	RuleHash string // rule‑hash (may be empty)
	CreId    string // cre‑ID (may be empty)
	Msg      string // optional extra text
	File     string // file name
	Err      error  // wrapped sentinel or nested error
}

func (e *Error) Error() string {
	msg := e.Msg
	if msg == "" && e.Err != nil {
		msg = e.Err.Error()
	}
	if msg == "" {
		msg = "error"
	}
	if e.Msg != "" && e.Err != nil {
		msg += ": " + e.Err.Error()
	}

	meta := fmt.Sprintf("line=%d, col=%d", e.Pos.Line, e.Pos.Col)

	if id := e.GetCreId(); id != "" {
		meta += fmt.Sprintf(", cre_id=%s", id)
	}
	if id := e.GetRuleId(); id != "" {
		meta += fmt.Sprintf(", rule_id=%s", id)
	}
	if h := e.GetRuleHash(); h != "" {
		meta += fmt.Sprintf(", rule_hash=%s", h)
	}
	if f := e.GetFile(); f != "" {
		meta += fmt.Sprintf(", file=%s", f)
	}

	return fmt.Sprintf("err=\"%s\", %s", msg, meta)
}

func (e *Error) Unwrap() error       { return e.Err }
func (e *Error) GetRuleId() string   { return e.RuleId }
func (e *Error) GetRuleHash() string { return e.RuleHash }
func (e *Error) GetCreId() string    { return e.CreId }
func (e *Error) GetPos() Pos         { return e.Pos }
func (e *Error) GetFile() string     { return e.File }

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

func WithFile(err error, file string) error {
	var perr *Error
	if errors.As(err, &perr) {
		if perr.File == "" {
			perr.File = file
		}
	}
	return err
}
