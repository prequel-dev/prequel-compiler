package compiler

import (
	"context"
	"errors"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
	lm "github.com/prequel-dev/prequel-logmatch/pkg/match"
	"github.com/rs/zerolog/log"
)

var (
	ErrExpectedReteMatcher = errors.New("expected rete matcher")
	ErrExpectedJsonMatcher = errors.New("expected jq json matcher")
	ErrExpectedLogMatcher  = errors.New("expected log matcher")
	ErrExpectedCbDetect    = errors.New("expected detect callback")
	ErrInvalidCbArgs       = errors.New("invalid callback arguments")
	ErrNotFound            = errors.New("not found")
)

type MatchParamsT struct {
	Address       *ast.AstNodeAddressT
	ParentAddress *ast.AstNodeAddressT
	Origin        bool
}

type AssertParamsT struct {
	Address *ast.AstNodeAddressT
}

type CallbackT func(ctx context.Context, param any) error

type RuntimeI interface {
	NewCbMatch(params MatchParamsT) CallbackT
	NewCbAssert(params AssertParamsT) CallbackT
}

func GetJqMatcher(obj *ObjT) (lm.MatchFunc, error) {
	var (
		m  lm.MatchFunc
		ok bool
	)

	log.Info().Type("object", obj.Object).Msg("Getting jq matcher")
	if m, ok = obj.Object.(lm.MatchFunc); !ok {
		return nil, ErrExpectedJsonMatcher
	}

	return m, nil
}

func GetLogInverseSeqMatcher(obj *ObjT) (*lm.InverseSeq, error) {
	var (
		m  *lm.InverseSeq
		ok bool
	)

	if m, ok = obj.Object.(*lm.InverseSeq); !ok {
		return nil, ErrExpectedLogMatcher
	}

	return m, nil
}

func GetLogSeqMatcher(obj *ObjT) (*lm.MatchSeq, error) {
	var (
		m  *lm.MatchSeq
		ok bool
	)

	if m, ok = obj.Object.(*lm.MatchSeq); !ok {
		return nil, ErrExpectedLogMatcher
	}

	return m, nil
}

func GetLogSingleMatcher(obj *ObjT) (*lm.MatchSingle, error) {
	var (
		m  *lm.MatchSingle
		ok bool
	)

	log.Info().Type("object", obj.Object).Msg("Getting log single matcher")

	if m, ok = obj.Object.(*lm.MatchSingle); !ok {
		return nil, ErrExpectedLogMatcher
	}

	return m, nil
}

// -----
type NoopRuntime struct{}

func NewNoopRuntime() *NoopRuntime {
	return &NoopRuntime{}
}

func (f *NoopRuntime) NewCbMatch(params MatchParamsT) CallbackT {
	return func(ctx context.Context, param any) error {
		return nil
	}
}

func (f *NoopRuntime) NewCbAssert(params AssertParamsT) CallbackT {
	return func(ctx context.Context, param any) error {
		return nil
	}
}
