package compiler

import (
	"context"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
)

type MatchParamsT struct {
	Address       ast.AstNodeAddressT
	ParentAddress *ast.AstNodeAddressT
	Origin        bool
}

type AssertParamsT struct {
	Address ast.AstNodeAddressT
}

type CallbackT func(ctx context.Context, param any) error

type RuntimeI interface {
	NewCbMatch(params MatchParamsT) CallbackT
	NewCbAssert(params AssertParamsT) CallbackT
}

func AssertObject[T any](obj *ObjT) (*T, error) {
	m, ok := obj.Object.(*T)
	if !ok {
		return nil, ErrObjectTypeAssertion
	}
	return m, nil
}
