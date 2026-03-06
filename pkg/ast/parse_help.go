package ast

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/goccy/go-yaml/ast"
)

func (p *parserT) nodeToMapping(node ast.Node) (*ast.MappingNode, error) {
	if node == nil {
		return nil, fmt.Errorf("%w: expected yaml mapping, got null", ErrUnexpectedType)
	}
	mapping, ok := node.(*ast.MappingNode)
	if !ok {
		err := fmt.Errorf("%w: expected yaml mapping, got %s", ErrUnexpectedType, node.Type())
		return nil, p.wrapError(node, err)
	}
	return mapping, nil
}

func (p *parserT) nodeToSequence(node ast.Node) (*ast.SequenceNode, error) {
	if node == nil {
		return nil, fmt.Errorf("%w: expected yaml sequence, got null", ErrUnexpectedType)
	}
	seq, ok := node.(*ast.SequenceNode)
	if !ok {
		err := fmt.Errorf("%w: expected yaml sequence, got %s", ErrUnexpectedType, node.Type())
		return nil, p.wrapError(node, err)
	}
	return seq, nil
}

func (p *parserT) nodeToString(node ast.Node) (string, error) {
	if node == nil {
		return "", fmt.Errorf("%w: expected yaml string, got null", ErrUnexpectedType)
	}

	var s string

	switch v := node.(type) {
	case *ast.StringNode:
		s = v.Value
	case *ast.LiteralNode:
		if v.Value == nil {
			err := fmt.Errorf("%w: literal node value is null", ErrUnexpectedType)
			return "", p.wrapError(node, err)
		}
		s = v.Value.Value
	default:
		err := fmt.Errorf("%w: expected yaml string, got %s", ErrUnexpectedType, node.Type())
		return "", p.wrapError(node, err)
	}
	return s, nil
}

func (p *parserT) nodeToInt64(node ast.Node) (int64, error) {
	if node == nil {
		return 0, fmt.Errorf("%w: expected yaml integer, got null", ErrUnexpectedType)
	}
	v, ok := node.(*ast.IntegerNode)
	if !ok {
		err := fmt.Errorf("%w: %s", ErrUnexpectedType, node.Type())
		return 0, p.wrapError(node, err)
	}

	ival, ok := v.Value.(int64)
	if !ok {
		err := fmt.Errorf("%w: integer value out of range: %v", ErrUnexpectedType, v.Value)
		return 0, p.wrapError(node, err)
	}

	return ival, nil
}

func (p *parserT) nodeToUint64(node ast.Node) (uint64, error) {
	if node == nil {
		return 0, fmt.Errorf("%w: expected yaml integer, got null", ErrUnexpectedType)
	}
	v, ok := node.(*ast.IntegerNode)
	if !ok {
		err := fmt.Errorf("%w: %s", ErrUnexpectedType, node.Type())
		return 0, p.wrapError(node, err)
	}

	ival, ok := v.Value.(uint64)
	if !ok {
		err := fmt.Errorf("%w: integer value out of range: %v", ErrUnexpectedType, v.Value)
		return 0, p.wrapError(node, err)
	}

	return ival, nil
}

func (p *parserT) nodeToUint(v ast.Node) (uint, error) {
	n, err := p.nodeToUint64(v)
	if err != nil {
		return 0, err
	}
	// if n > math.MaxUint {
	// 	// This is a theoretical limit since uint is typically either 32 or 64 bits depending on the platform,
	// 	// but we enforce it to prevent potential overflow issues when converting from uint64 to uint.
	// 	err := fmt.Errorf("%w: value must be a positive integer", ErrOverflow)
	// 	return 0, p.wrapError(v, err)
	// }
	return uint(n), nil
}

func (p *parserT) nodeToBool(node ast.Node) (bool, error) {
	if node == nil {
		return false, fmt.Errorf("%w: expected yaml boolean, got null", ErrUnexpectedType)
	}
	v, ok := node.(*ast.BoolNode)
	if !ok {
		err := fmt.Errorf("%w: %s", ErrUnexpectedType, node.Type())
		return false, p.wrapError(node, err)
	}
	return v.Value, nil
}

func (p *parserT) nodeToStrs(node ast.Node) ([]string, error) {
	seq, err := p.nodeToSequence(node)
	if err != nil {
		return nil, err
	}

	var strs []string
	for _, v := range seq.Values {
		ss, err := p.nodeToString(v)
		if err != nil {
			return nil, err
		}
		strs = append(strs, ss)
	}

	return strs, nil
}

func (p *parserT) nodeToRegex(node ast.Node) (*regexp.Regexp, error) {
	v, err := p.nodeToString(node)
	if err != nil {
		return nil, p.wrapError(node, err)
	}
	if v == "" {
		err := fmt.Errorf("%w: regex pattern cannot be empty", ErrBadRegex)
		return nil, p.wrapError(node, err)
	}
	exp, err := regexp.Compile(v)
	if err != nil {
		err = errors.Join(ErrBadRegex, err)
		return nil, p.wrapError(node, err)
	}
	return exp, nil
}

func (p *parserT) nodeToJq(node ast.Node) (string, error) {
	v, err := p.nodeToString(node)
	if err != nil {
		return "", p.wrapError(node, err)
	}
	if v == "" {
		err := fmt.Errorf("%w: jq expression cannot be empty", ErrBadJq)
		return "", p.wrapError(node, err)
	}
	if err := p.validateJQ(v); err != nil {
		err := errors.Join(ErrBadJq, err)
		return "", p.wrapError(node, err)
	}

	return v, nil
}

func (p *parserT) nodeToDuration(node ast.Node) (time.Duration, error) {
	v, err := p.nodeToString(node)
	if err != nil {
		return 0, p.wrapError(node, err)
	}

	w, err := time.ParseDuration(v)
	if err != nil {
		err := fmt.Errorf("%w: invalid duration format: %v", ErrUnexpectedType, err)
		return 0, p.wrapError(node, err)
	}

	return w, nil
}

func (p *parserT) nodeToDurationPositive(node ast.Node) (time.Duration, error) {
	dur, err := p.nodeToDuration(node)
	if err != nil {
		return 0, err
	}
	if dur <= 0 {
		err := fmt.Errorf("%w: duration must be positive", ErrUnexpectedType)
		return 0, p.wrapError(node, err)
	}
	return dur, nil
}

func findKey(node ast.Node, key string) ast.Node {
	mapping, ok := node.(*ast.MappingNode)
	if !ok {
		return nil
	}
	for _, v := range mapping.Values {
		if k, ok := v.Key.(*ast.StringNode); ok && k.Value == key {
			return v.Key
		}
	}
	return nil
}
