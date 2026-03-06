package ast

import (
	"errors"
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/token"
)

var (
	ErrBadAnchor          = errors.New("anchor value is out of range")
	ErrBadExtractName     = errors.New("extract name is not valid")
	ErrBadField           = errors.New("field is not valid")
	ErrBadGen             = errors.New("gen is not valid")
	ErrBadHash            = errors.New("hash is not valid")
	ErrBadIdentifier      = errors.New("identifier is not valid")
	ErrBadJq              = errors.New("invalid jq expression")
	ErrBadKind            = errors.New("kind is not valid")
	ErrBadPromQL          = errors.New("invalid PromQL query")
	ErrBadRegex           = errors.New("invalid regex pattern")
	ErrBadScriptCode      = errors.New("script code is not valid")
	ErrBadScriptLang      = errors.New("script language is not valid")
	ErrBadSeverity        = errors.New("severity is not valid")
	ErrDupeExtractName    = errors.New("duplicate extract name")
	ErrMaxDepthExceeded   = errors.New("maximum depth exceeded")
	ErrMaxRankExceeded    = errors.New("maximum rank exceeded")
	ErrMissingEvent       = errors.New("event is required when using field terms")
	ErrMissingKey         = errors.New("missing key")
	ErrMissingOrigin      = errors.New("missing origin")
	ErrMissingScriptInput = errors.New("script input is required")
	ErrMissingSource      = errors.New("source is required in event")
	ErrMissingTerm        = errors.New("at least one term is required")
	ErrMissingWindow      = errors.New("window is required when using multiple terms")
	ErrMissingVersion     = errors.New("compiler version is required in strict mode")
	ErrMultipleOrigin     = errors.New("multiple origin events are not allowed")
	ErrNegateCount        = errors.New("negate fields cannot have count > 1")
	ErrOverflow           = errors.New("value overflow")
	ErrShortSequence      = errors.New("sequence must have at least 2 terms")
	ErrTermRedefined      = errors.New("term previously defined")
	ErrTermTypeConflict   = errors.New("term type conflicts with previous term type")
	ErrUnexpectedKey      = errors.New("unexpected key")
	ErrUnexpectedType     = errors.New("unexpected type")
	ErrUndefinedAnchor    = errors.New("undefined anchor")
	ErrUnknownNodeType    = errors.New("unknown node type")
	ErrWindowNegative     = errors.New("window duration cannot be negative")
	ErrZeroCount          = errors.New("count value must be a positive integer")
)

type ErrRule struct {
	Meta AstMetadataT
	Err  error
}

func (e ErrRule) Error() string {
	return fmt.Sprintf("rule error id=%s hash=%s: %v", e.Meta.Id, e.Meta.Hash, e.Err)
}

func (e ErrRule) Unwrap() error {
	return e.Err
}

type ParseError struct {
	token *token.Token
	err   error
}

func (e ParseError) Offset() int {
	if e.token == nil {
		return 0
	}
	return e.token.Position.Offset
}

func (e ParseError) Line() int {
	if e.token == nil {
		return 0
	}
	return e.token.Position.Line
}

func (e ParseError) Column() int {
	if e.token == nil {
		return 0
	}
	return e.token.Position.Column
}

func (e ParseError) Unwrap() error {
	return e.err
}

func (e ParseError) Error() string {
	return e.err.Error()
}

func (e ParseError) Format(colored, inclSource bool) string {
	return yaml.FormatErrorWithToken(e.err.Error(), e.token, colored, inclSource)
}
