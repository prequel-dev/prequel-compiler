package ast

import (
	"errors"
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/prequel-dev/prequel-compiler/pkg/version"
)

func (p *parserT) parseRulesNode(node ast.Node) ([]AstRuleT, error) {

	seq, err := p.nodeToSequence(node)
	if err != nil {
		return nil, err
	}

	var (
		rules   []AstRuleT
		errList []error
	)

	for _, ruleNode := range seq.Values {

		rule, err := p.parseRuleNode(ruleNode)

		switch {
		case err != nil:
			errList = append(errList, err)
		default:
			rules = append(rules, *rule)
		}
	}

	return rules, errors.Join(errList...)
}

// Expects layout of a single mapping node with keys 'metadata', 'cre', and 'rule'
// Optionally there is a version filter; with a required version with an optional "<" prefix.

func (p *parserT) parseRuleNode(node ast.Node) (*AstRuleT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	// Check compiler version filter if present
	if err := p.maybeCheckCompiler(mapping); err != nil {
		return nil, err
	}

	var (
		meta    *AstMetadataT
		cre     *AstCreT
		ruleDom ast.Node
	)

	maybeMeta := func(err error) error {
		if meta == nil {
			return err
		}
		return ErrRule{
			Meta: *meta,
			Err:  err,
		}
	}

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, maybeMeta(err)
		}

		switch key {

		case kwMetadata:
			meta, err = p.parseMetadataNode(v.Value)

		case kwCre:
			cre, err = p.parseCreNode(v.Value)

		case kwRule:
			ruleDom = v.Value

		case kwCompiler:
			// TODO: Handle compiler filter

		default:
			err = p.wrapError(v.Key, ErrUnexpectedKey)
		}

		if err != nil {
			return nil, maybeMeta(err)
		}
	}

	switch {
	case meta == nil:
		err := fmt.Errorf("%w: %s", ErrMissingKey, kwMetadata)
		return nil, p.wrapErrorParent(mapping, err)
	case ruleDom == nil:
		kerr := fmt.Errorf("%w: %s", ErrMissingKey, kwRule)
		return nil, maybeMeta(p.wrapErrorParent(mapping, kerr))
	}

	// Parse the root
	var (
		rootAst   AstNode
		ruleState = newRuleState(meta)
	)

	if rootAst, err = p.parseRootNode(ruleState, ruleDom); err != nil {
		return nil, maybeMeta(err)
	}

	if ruleState.getOrigin() < 1 {
		return nil, maybeMeta(p.wrapErrorParent(ruleDom, ErrMissingOrigin))
	}

	rule := &AstRuleT{
		Cre:      cre,
		Metadata: *meta,
		Root:     rootAst,
	}

	return rule, nil
}

// At the root, expecting:
// type ParseRuleDataT struct {
// 	Sequence *ParseSequenceT `yaml:"sequence,omitempty"`
// 	Set      *ParseSetT      `yaml:"set,omitempty"`
// }

func (p *parserT) parseRootNode(state ruleState, node ast.Node) (AstNode, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var (
		rootNode AstNode
	)

	alreadySet := func(v ast.Node) error {
		if rootNode != nil {
			err := fmt.Errorf("%w: one of '%s' or '%s' expected in rule root", ErrUnexpectedKey, kwSequence, kwSet)
			return p.wrapError(v, err)
		}
		return nil
	}

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}
		switch key {

		case kwSequence:
			if err := alreadySet(v.Key); err != nil {
				return nil, err
			}
			if rootNode, err = p.parseNode(state, AstNodeTypeSeq, v.Value); err != nil {
				return nil, err
			}

		case kwSet:
			if err := alreadySet(v.Key); err != nil {
				return nil, err
			}
			if rootNode, err = p.parseNode(state, AstNodeTypeSet, v.Value); err != nil {
				return nil, err
			}

		default:
			err := fmt.Errorf("%w: only '%s' or '%s' expected in rule root, not '%s'", ErrUnexpectedKey, kwSequence, kwSet, key)
			return nil, p.wrapError(v.Key, err)

		}
	}

	if rootNode == nil {
		err := fmt.Errorf("%w: expected rule root to contain either '%s' or '%s' key", ErrMissingKey, kwSequence, kwSet)
		return nil, p.wrapError(mapping, err)
	}

	return rootNode, nil
}

// Checks that the compiler version, if specified in the rule,
// matches the version of the compiler.

func (p *parserT) allowCompilerVersion(node ast.Node) error {

	versionExp, err := p.nodeToString(node)

	switch {
	case err != nil:
		return err
	case versionExp != "":
		if err := version.AllowVersion(versionExp); err != nil {
			return p.wrapError(node, err)
		}
	case p.strict:
		err := fmt.Errorf("%w: compiler version expression must be specified in strict mode", ErrMissingVersion)
		return p.wrapError(node, err)
	default:
		// allow missing version in non-strict mode
	}

	return nil
}

func (p *parserT) maybeCheckCompiler(mapping *ast.MappingNode) error {

	var (
		metaNode     ast.Node
		compilerNode ast.Node
	)

	// Scan ahead for compiler node and metadata node for error context if version check fails
	for _, v := range mapping.Values {
		key, err := p.nodeToString(v.Key)
		if err != nil {
			continue
		}
		switch key {
		case kwCompiler:
			compilerNode = v.Value
		case kwMetadata:
			metaNode = v.Value
		}
	}

	if compilerNode == nil {
		return nil
	}

	err := p.allowCompilerVersion(compilerNode)
	if err == nil {
		return nil
	}

	// Attempt to parse metadata for error context,
	meta, merr := p.parseMetadataNode(metaNode)

	// If metadata parsing fails, attempt a minimal metadata parse
	if merr != nil {
		if meta, merr = p.parseMetadataNodeMinimal(metaNode); merr != nil {
			// If even the minimal parse fails, return the
			// original version error without metadata context
			return err
		}
	}

	return ErrRule{Meta: *meta, Err: err}
}
