package ast

import (
	"errors"
	"testing"

	"github.com/goccy/go-yaml/token"
)

func TestErrRule_ErrorAndUnwrap(t *testing.T) {
	meta := AstMetadataT{Id: "id1", Hash: "h1"}
	origErr := errors.New("something bad")
	e := ErrRule{Meta: meta, Err: origErr}

	msg := e.Error()
	if want := "rule error id=id1 hash=h1: something bad"; msg != want {
		t.Errorf("ErrRule.Error() = %q, want %q", msg, want)
	}

	if !errors.Is(e, origErr) {
		t.Errorf("ErrRule.Unwrap() did not return the original error")
	}
}

func TestParseError_OffsetLineColumn(t *testing.T) {
	tok := &token.Token{
		Position: &token.Position{
			Offset: 42,
			Line:   7,
			Column: 13,
		},
	}
	e := ParseError{token: tok, err: errors.New("parse fail")}

	if got := e.Offset(); got != 42 {
		t.Errorf("ParseError.Offset() = %d, want 42", got)
	}
	if got := e.Line(); got != 7 {
		t.Errorf("ParseError.Line() = %d, want 7", got)
	}
	if got := e.Column(); got != 13 {
		t.Errorf("ParseError.Column() = %d, want 13", got)
	}
}

func TestParseError_OffsetLineColumn_NilToken(t *testing.T) {
	e := ParseError{token: nil, err: errors.New("parse fail")}
	if got := e.Offset(); got != 0 {
		t.Errorf("ParseError.Offset() with nil token = %d, want 0", got)
	}
	if got := e.Line(); got != 0 {
		t.Errorf("ParseError.Line() with nil token = %d, want 0", got)
	}
	if got := e.Column(); got != 0 {
		t.Errorf("ParseError.Column() with nil token = %d, want 0", got)
	}
}

func TestParseError_ErrorAndUnwrap(t *testing.T) {
	origErr := errors.New("parse fail")
	e := ParseError{err: origErr}
	if got := e.Error(); got != "parse fail" {
		t.Errorf("ParseError.Error() = %q, want %q", got, "parse fail")
	}
	if !errors.Is(e, origErr) {
		t.Errorf("ParseError.Unwrap() did not return the original error")
	}
}

// Note: Format() just calls yaml.FormatErrorWithToken, which is a passthrough.
// You can check that it returns a non-empty string.
func TestParseError_Format(t *testing.T) {
	tok := &token.Token{
		Position: &token.Position{
			Offset: 1,
			Line:   2,
			Column: 3,
		},
	}
	e := ParseError{token: tok, err: errors.New("parse fail")}
	out := e.Format(false, false)
	if out == "" {
		t.Errorf("ParseError.Format() returned empty string")
	}
}
