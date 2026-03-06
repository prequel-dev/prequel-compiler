package ast

import (
	"errors"
	"fmt"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
)

type parserT struct {
	maxGen         uint32
	maxRank        uint32
	maxDepth       uint32
	strict         bool
	root           ast.Node
	validateJQ     ValidatorFunc
	validateLua    ValidatorFunc
	validatePromQL ValidatorFunc
}

func ParseRules(yamlInput []byte, opts ...ParseOpt) ([]AstRuleT, error) {
	o := parseOpts(opts...)

	p := parserT{
		maxGen:         o.maxGen,
		maxRank:        o.maxRank,
		maxDepth:       o.maxDepth,
		strict:         o.strict,
		validateJQ:     o.jqValidator,
		validateLua:    o.luaValidator,
		validatePromQL: o.promQLValidator,
	}

	return p.parse(yamlInput)
}

func (p *parserT) parse(yamlInput []byte) ([]AstRuleT, error) {

	// First parse the YAML into a yaml AST ignoring comments.
	doc, err := parser.ParseBytes(yamlInput, 0)
	if err != nil {
		return nil, err
	}

	// A yaml file can contain multiple documents;
	// iterate across the documents and parse each one separately as a rule document.

	var (
		rules   []AstRuleT
		errList []error
	)

	for _, d := range doc.Docs {
		nRules, err := p.parseDocument(d)

		switch err {
		case nil:
			rules = append(rules, nRules...)
		default:
			errList = append(errList, err)
		}
	}

	return rules, errors.Join(errList...)
}

// A single YAML document should container a map with a single key "rules" that maps to a list of rules.

func (p *parserT) parseDocument(doc *ast.DocumentNode) ([]AstRuleT, error) {

	// Set the root of the document for error reporting purposes.
	p.root = doc.Body
	defer func() {
		p.root = nil
	}()

	mapping, err := p.nodeToMapping(doc.Body)
	if err != nil {
		return nil, err
	}

	var (
		hasRules bool
		rules    []AstRuleT
	)

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {

		case kwRules:
			hasRules = true
			if rules, err = p.parseRulesNode(v.Value); err != nil {
				return nil, err
			}

		default:
			err := fmt.Errorf("unexpected key '%s' in document body", key)
			return nil, p.wrapError(v.Key, err)
		}
	}

	if !hasRules {
		err := fmt.Errorf("%w: %s", ErrMissingKey, kwRules)
		return nil, p.wrapError(mapping, err)
	}

	return rules, nil
}

func (p *parserT) rewriteError(err error) error {

	if yErr, ok := err.(yaml.Error); ok {
		return ParseError{
			token: yErr.GetToken(),
			err:   err,
		}
	}

	return err
}

func (p *parserT) wrapErrorParent(node ast.Node, err error) error {
	parent := ast.Parent(p.root, node)
	return p.wrapError(parent, err)
}

func (p *parserT) wrapError(node ast.Node, err error) error {
	if node == nil {
		return p.rewriteError(err)
	}

	return ParseError{
		token: node.GetToken(),
		err:   err,
	}
}
