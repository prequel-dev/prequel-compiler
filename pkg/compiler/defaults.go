package compiler

import (
	"context"
	"fmt"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
)

var (
	defaultPlugin  = NewDefaultPlugin()
	defaultRuntime = NewNoopRuntime()
)

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

// -----

type DefaultPlugin struct{}

func NewDefaultPlugin() *DefaultPlugin {
	return &DefaultPlugin{}
}

func (p *DefaultPlugin) Compile(runtime RuntimeI, node ast.AstNode) (ObjsT, error) {

	match, ok := node.(*ast.AstMatchLeafT)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrUnsupportedAstType, node)
	}

	obj, err := ObjLogMatcher(runtime, match)
	if err != nil {
		return nil, err
	}

	return ObjsT{obj}, nil
}
