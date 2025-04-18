package compiler

import (
	"context"
	"errors"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
	"github.com/prequel-dev/prequel-compiler/pkg/matchz"
	lm "github.com/prequel-dev/prequel-logmatch/pkg/match"
	"github.com/rs/zerolog/log"
)

var (
	ErrExpectedReteMatcher   = errors.New("expected rete matcher")
	ErrExpectedJsonMatcher   = errors.New("expected jq json matcher")
	ErrExpectedJsonMatcherCb = errors.New("expected jq json matcher callback")
	ErrExpectedLogMatcher    = errors.New("expected log matcher")
	ErrExpectedLogMatcherCb  = errors.New("expected log matcher callback")
	ErrExpectedCbDetect      = errors.New("expected detect callback")
	ErrInvalidCbArgs         = errors.New("invalid callback arguments")
	ErrNotFound              = errors.New("not found")
)

type MatchParamsT struct {
	Address       *ast.AstNodeAddressT
	ParentAddress *ast.AstNodeAddressT
	Origin        bool
}

type AssertParamsT struct {
	Address *ast.AstNodeAddressT
}

type CbMatchT func(ctx context.Context, m matchz.HitsT) error
type CbAssertT func(ctx context.Context) error

type RuntimeI interface {
	NewCbMatch(params MatchParamsT) CbMatchT
	NewCbAssert(params AssertParamsT) CbAssertT
	LoadAssertObject(ctx context.Context, obj *ObjT) error
	LoadMachineObject(ctx context.Context, obj *ObjT, userCb any) error
}

func GetJqMatcher(obj *ObjT) (lm.MatchFunc, CbMatchT, error) {
	var (
		cb CbMatchT
		m  lm.MatchFunc
		ok bool
	)

	log.Info().Type("object", obj.Object).Msg("Getting jq matcher")
	if m, ok = obj.Object.(lm.MatchFunc); !ok {
		return nil, nil, ErrExpectedJsonMatcher
	}

	if cb, ok = obj.Cb.Callback.(CbMatchT); !ok {
		return nil, nil, ErrExpectedJsonMatcherCb
	}

	return m, cb, nil
}

func GetLogInverseSeqMatcher(obj *ObjT) (*lm.InverseSeq, CbMatchT, error) {
	var (
		cb CbMatchT
		m  *lm.InverseSeq
		ok bool
	)

	if m, ok = obj.Object.(*lm.InverseSeq); !ok {
		return nil, nil, ErrExpectedLogMatcher
	}

	if cb, ok = obj.Cb.Callback.(CbMatchT); !ok {
		return nil, nil, ErrExpectedLogMatcherCb
	}

	return m, cb, nil
}

func GetLogSeqMatcher(obj *ObjT) (*lm.MatchSeq, CbMatchT, error) {
	var (
		cb CbMatchT
		m  *lm.MatchSeq
		ok bool
	)

	if m, ok = obj.Object.(*lm.MatchSeq); !ok {
		return nil, nil, ErrExpectedLogMatcher
	}

	if cb, ok = obj.Cb.Callback.(CbMatchT); !ok {
		return nil, nil, ErrExpectedLogMatcherCb
	}

	return m, cb, nil
}

func GetLogSingleMatcher(obj *ObjT) (*lm.MatchSingle, CbMatchT, error) {
	var (
		cb CbMatchT
		m  *lm.MatchSingle
		ok bool
	)

	log.Info().Type("object", obj.Object).Msg("Getting log single matcher")

	if m, ok = obj.Object.(*lm.MatchSingle); !ok {
		return nil, nil, ErrExpectedLogMatcher
	}

	if cb, ok = obj.Cb.Callback.(CbMatchT); !ok {
		return nil, nil, ErrExpectedLogMatcherCb
	}

	return m, cb, nil
}

// -----
type NoopRuntime struct{}

func NewNoopRuntime() *NoopRuntime {
	return &NoopRuntime{}
}

func (f *NoopRuntime) NewCbMatch(params MatchParamsT) CbMatchT {
	return func(ctx context.Context, m matchz.HitsT) error {
		return nil
	}
}

func (f *NoopRuntime) NewCbAssert(params AssertParamsT) CbAssertT {
	return func(ctx context.Context) error {
		return nil
	}
}

func (f *NoopRuntime) LoadAssertObject(ctx context.Context, obj *ObjT) error {
	return nil
}

func (f *NoopRuntime) LoadMachineObject(ctx context.Context, obj *ObjT, userCb any) error {
	return nil
}
