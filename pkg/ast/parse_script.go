package ast

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
)

// type ParseScriptT struct {
// 	Code     string      `yaml:"code"`
// 	Language string      `yaml:"language,omitempty"` // Assumes 'lua' if empty
// 	Timeout  string      `yaml:"timeout,omitempty"`  // Uses default if empty; expects duration string
// 	Input    *ParseTermT `yaml:"input"`              // Required input
// }

const (
	scriptLua = "lua"
)

func (p *parserT) parseScriptNode(state ruleState, node ast.Node) (*AstScriptT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	child := state.pushNode(AstNodeTypeScript)

	var (
		script = AstScriptT{
			baseAst: baseAst{
				scope:   AstScopeCluster,
				address: *child.addr,
				parent:  state.addr,
			},
		}
	)

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {

		case kwScriptCode:
			script.Code, err = p.parseScriptCode(v.Value)

		case kwScriptLang:
			script.Language, err = p.parseScriptLang(v.Value)

		case kwScriptTimeout:
			script.Timeout, err = p.nodeToDuration(v.Value)

		case kwScriptInput:
			script.Input, err = p.parseScriptInput(child, v.Value)

		default:
			err = p.wrapError(v.Key, ErrUnexpectedKey)
		}

		if err != nil {
			return nil, err
		}
	}

	if script.Input == nil {
		return nil, p.wrapErrorParent(mapping, ErrMissingScriptInput)
	}

	return &script, nil
}

func (p *parserT) parseScriptInput(state ruleState, node ast.Node) (AstNode, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var inputNode AstNode

	for i, v := range mapping.Values {
		if i > 0 {
			err := fmt.Errorf("%w: script input mapping must have exactly one key", ErrUnexpectedKey)
			return nil, p.wrapError(v.Key, err)
		}

		key, ok := v.Key.(*ast.StringNode)
		if !ok {
			err := fmt.Errorf("%w: script input mapping keys must be strings", ErrUnexpectedType)
			return nil, p.wrapError(v.Key, err)
		}

		if inputNode, err = p.parseTermChild(state, key, v.Value, nil); err != nil {
			return nil, err
		}
	}

	if inputNode == nil {
		err := fmt.Errorf("%w: script input mapping must have exactly one key", ErrMissingScriptInput)
		return nil, p.wrapError(node, err)
	}

	return inputNode, nil
}

func (p *parserT) parseScriptLang(node ast.Node) (string, error) {

	s, err := p.nodeToString(node)
	if err != nil {
		return "", err
	}

	switch s {
	case scriptLua:
		// Fall through

	case "":
		if p.strict {
			err = fmt.Errorf("%w: script language cannot be empty", ErrBadScriptLang)
			return "", p.wrapError(node, err)
		}

	default:
		err = fmt.Errorf("%w: unsupported script language: %s", ErrBadScriptLang, s)
		return "", p.wrapError(node, err)
	}

	return s, nil

}

func (p *parserT) parseScriptCode(node ast.Node) (string, error) {

	s, err := p.nodeToString(node)
	if err != nil {
		return "", err
	}

	// Only Lua supported for now, so validate as Lua code
	if err := p.validateLua(s); err != nil {
		return "", p.wrapError(node, fmt.Errorf("%w: invalid Lua code: %w", ErrBadScriptCode, err))
	}

	return s, nil
}
