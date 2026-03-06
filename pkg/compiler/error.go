package compiler

import "errors"

var (
	ErrNoFields            = errors.New("no fields")
	ErrUnsupportedNodeType = errors.New("unsupported node type")
	ErrUnsupportedAstType  = errors.New("unsupported AST node type")
	ErrUnsupportedScope    = errors.New("unsupported scope")
	ErrSequenceSingleMatch = errors.New("sequence with single match (use set instead)")
	ErrObjectTypeAssertion = errors.New("object type assertion failed")
)
