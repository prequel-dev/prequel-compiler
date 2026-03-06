package ast

import (
	"fmt"
	"math"
	"regexp"

	"github.com/goccy/go-yaml/ast"
)

// parseTerms is responsible for parsing a sequence of terms.
//
// Terms can be either line match terms (field nodes) or set/sequence/promql/script terms (child nodes).
// A non-zero negateOffset indicates that these terms are being parsed in the context of a negate clause.
// Terms must be all field nodes or all child nodes; mixing is not allowed.

func (p *parserT) parseTerms(state ruleState, v ast.Node, negateOffset int) ([]*protoTerm, error) {

	// Expects the terms to be defined as a sequence node. If it's not a sequence, this will return an error.
	seq, err := p.nodeToSequence(v)
	if err != nil {
		return nil, err
	}

	var (
		allFields bool
		terms     []*protoTerm
	)

	for i, termNode := range seq.Values {

		// Sanity check on rank; this should be inside the loop since rank is incremented for each term.
		// Note: maxRank is one based, whereas rank is zero based, so we check if rank+1 exceeds maxRank.
		if state.rank >= p.maxRank {
			err := fmt.Errorf("%w: %d", ErrMaxRankExceeded, p.maxRank)
			return nil, p.wrapError(termNode, err)
		}

		// Parse the term, which can be either a leaf node or an inner node.
		// We determine this based on the first term, and then enforce that all subsequent terms are of the same type.
		term, err := p.parseTerm(state, termNode, negateOffset)

		switch {
		case err != nil:
			return nil, err

		case i == 0:
			// First term; determine if this is a field term or a child node term,
			// and set the allFields flag accordingly.
			allFields = term.field != nil

		case allFields && term.field == nil:
			// This term is a child node, but previous terms were field nodes; this is not allowed.
			err := fmt.Errorf("%w: all terms must be field nodes", ErrTermTypeConflict)
			return nil, p.wrapError(termNode, err)

		case !allFields && term.field != nil:
			// This term is a field node, but previous terms were child nodes; this is not allowed.
			err := fmt.Errorf("%w: all terms must be child nodes", ErrTermTypeConflict)
			return nil, p.wrapError(termNode, err)

		default:
			// Term type is consistent with previous terms; continue.
		}

		// Term is valid; add to list
		terms = append(terms, term)

		// Increment the rank for the next term.
		state = state.incRank()
	}

	if p.strict && len(terms) == 0 {
		return nil, p.wrapError(v, ErrMissingTerm)
	}

	return terms, nil
}

// parseTerm is responsible for parsing a single term, which can be either a simple string (field term) or a mapping (field term or child node term).
func (p *parserT) parseTerm(state ruleState, node ast.Node, negateOffset int) (*protoTerm, error) {

	// A term which can be either a simple string or a mapping.
	// If the term is a mapping type, parse it as such. Otherwise, treat it as a simple string term.
	if mapping, ok := node.(*ast.MappingNode); ok {
		return p.parseTermAsMap(state, mapping, negateOffset)
	}

	s, err := p.nodeToString(node)
	if err != nil {
		return nil, err
	}

	return &protoTerm{
		field: &protoField{
			StrValue: s,
		},
	}, nil
}

// parseTermAsMap parses a term that is represented as a YAML mapping.
// This can represent either a field term (line match) or a child node term (set/sequence/promql/script).
//
// A non-zero negateOffset indicates that these terms are being parsed in the context of a negate clause,
// and the offset is used to validate any anchors.
//
//	Layout is as follows:
// 		// AstFieldT fields
//		Field      string            `yaml:"field,omitempty"`
//		StrValue   string            `yaml:"value,omitempty"`
//		JqValue    string            `yaml:"jq,omitempty"`
//		RegexValue string            `yaml:"regex,omitempty"`
//		Count      int               `yaml:"count,omitempty"`
//		Extract    []ParseExtractT   `yaml:"extract,omitempty"`
//
//		// Child terms
//		Set        *ParseSetT        `yaml:"set,omitempty"`
//		Sequence   *ParseSequenceT   `yaml:"sequence,omitempty"`
//		PromQL     *ParsePromQL      `yaml:"promql,omitempty"`
//		Script     *ParseScriptT     `yaml:"script,omitempty"`
//
// 		// Applies to AstField and  Child terms
//		NegateOpts *ParseNegateOptsT `yaml:",inline,omitempty"`
//

func (p *parserT) parseTermAsMap(state ruleState, mapping *ast.MappingNode, negateOffset int) (*protoTerm, error) {

	type childPair struct {
		key *ast.StringNode
		val ast.Node
	}

	var (
		err   error
		child AstNode
		cPair childPair
		field *protoField
		nOpts *AstNegateOptsT
	)

	for _, v := range mapping.Values {

		// Keep the key node around to pass to helper functions; necessary for error wrapping with context.
		key, ok := v.Key.(*ast.StringNode)
		if !ok {
			err := fmt.Errorf("%w: %s", ErrUnexpectedType, v.Key.Type())
			return nil, p.wrapError(v.Key, err)
		}

		switch key.Value {

		case kwSet, kwSequence, kwPromQL, kwScript:
			// Only one child or field allowed; if either is already defined, this is an error.
			if cPair.key != nil || field != nil {
				return nil, p.wrapError(v.Key, ErrTermRedefined)
			}

			// Defer parsing the child node until the end,
			// after we've checked for field terms and negate options.
			// Child node parsing requires negate options to be parsed.
			cPair = childPair{key: key, val: v.Value}

		case kwField, kwValue, kwJq, kwRegex, kwCount, kwExtract:
			// Only one child or field allowed; if child is already defined, this is an error.
			if cPair.key != nil {
				return nil, p.wrapError(v.Key, ErrTermRedefined)
			}
			if field == nil {
				field = &protoField{Count: 1}
			}
			if err := p.parseTermField(key, v.Value, field, negateOffset > 0); err != nil {
				return nil, err
			}

		case kwWindow, kwSlide, kwAnchor, kwAbsolute:
			// Negate options are only allowed if there is a non-zero negate offset,
			// which indicates that we are parsing terms in the context of a negate clause.
			if negateOffset == 0 {
				err := fmt.Errorf("%w: negate options not allowed on positive term", ErrUnexpectedKey)
				return nil, p.wrapError(v.Key, err)
			}
			if nOpts == nil {
				nOpts = &AstNegateOptsT{}
			}
			if err := p.parseNegateOpts(key.Value, v.Value, nOpts, negateOffset); err != nil {
				return nil, err
			}

		default:
			return nil, p.wrapError(v.Key, ErrUnexpectedKey)
		}
	}

	if cPair.key != nil {
		if child, err = p.parseTermChild(state, cPair.key, cPair.val, nOpts); err != nil {
			return nil, err
		}
	}

	if field != nil {
		if err := field.validate(); err != nil {
			return nil, p.wrapErrorParent(mapping, err)
		}
	}

	return &protoTerm{
		negateOpts: nOpts,
		child:      child,
		field:      field,
	}, nil
}

func (p *parserT) parseTermChild(state ruleState, key *ast.StringNode, val ast.Node, nOpts *AstNegateOptsT) (AstNode, error) {

	switch key.Value {

	case kwSet:
		return p.parseNode(state, AstNodeTypeSet, val)

	case kwSequence:
		return p.parseNode(state, AstNodeTypeSeq, val)

	case kwPromQL:
		if nOpts != nil {
			err := fmt.Errorf("%w: negate options cannot be used with %s terms", ErrUnexpectedKey, key.Value)
			return nil, p.wrapError(key, err)
		}
		return p.parsePromQLNode(state, val)

	case kwScript:
		if nOpts != nil {
			err := fmt.Errorf("%w: negate options cannot be used with %s terms", ErrUnexpectedKey, key.Value)
			return nil, p.wrapError(key, err)
		}
		return p.parseScriptNode(state, val)

	default:
		// Should not happen; parseTermAsMap should only call this on expected keys.
		return nil, p.wrapError(key, ErrUnexpectedKey)
	}
}

func (p *parserT) parseTermField(key *ast.StringNode, v ast.Node, match *protoField, allowNegate bool) error {

	var (
		err      error
		hasValue bool
	)

	if match.RegexValue != "" || match.JqValue != "" || match.StrValue != "" {
		hasValue = true
	}

	mkMatchKeyError := func() error {
		kerr := fmt.Errorf("%w: '%s' key cannot be used together with other line match keys", ErrUnexpectedKey, key.Value)
		return p.wrapError(key, kerr)
	}

	switch key.Value {
	case kwField:
		match.Field, err = p.nodeToString(v)

	case kwCount:
		match.Count, err = p.nodeToUint64(v)
		switch {
		case err != nil:
			// fall through
		case match.Count == 0:
			err = p.wrapError(key, ErrZeroCount)
		case allowNegate && match.Count > 1:
			err = p.wrapError(key, ErrNegateCount)
		}

	case kwExtract:
		match.Extract, err = p.parseExtracts(v)

	case kwJq:
		if hasValue {
			err = mkMatchKeyError()
		} else {
			match.JqValue, err = p.nodeToJq(v)
		}

	case kwRegex:
		if hasValue {
			err = mkMatchKeyError()
		} else {
			var exp *regexp.Regexp
			if exp, err = p.nodeToRegex(v); err == nil {
				match.RegexValue = exp.String()
			}
		}

	case kwValue:
		if hasValue {
			err = mkMatchKeyError()
		} else {
			match.StrValue, err = p.nodeToString(v)
			if err == nil && match.StrValue == "" {
				err = p.wrapError(v, fmt.Errorf("%w: value cannot be empty", ErrBadField))
			}
		}

	default:
		// Should not happen; parseTermAsMap should only call this on expected keys.
		err = p.wrapError(key, ErrUnexpectedKey)
	}

	return err
}

func (p *parserT) parseNegateOpts(key string, node ast.Node, opts *AstNegateOptsT, negateOffset int) error {
	var err error

	switch key {
	case kwWindow:
		opts.Window, err = p.nodeToDurationPositive(node)

	case kwSlide:
		opts.Slide, err = p.nodeToDuration(node)

	case kwAnchor:
		var anchorInt uint64
		anchorInt, err = p.nodeToUint64(node)
		switch {
		case err != nil:
			// fall through
		case anchorInt >= uint64(negateOffset):
			nerr := fmt.Errorf("%w: anchor value must be in range of [0, %d)", ErrBadAnchor, negateOffset)
			err = p.wrapError(node, nerr)
		case anchorInt > math.MaxUint8:
			// Anchor is later cast to a uint8 (see match.ResetT.Anchor)
			nerr := fmt.Errorf("%w: anchor value must be less than or equal to %d", ErrBadAnchor, math.MaxUint8)
			err = p.wrapError(node, nerr)
		default:
			opts.Anchor = uint32(anchorInt)
		}

	case kwAbsolute:
		opts.Absolute, err = p.nodeToBool(node)

	default:
		// Should not happen; parseTermAsMap should only call this on expected keys.
		err = fmt.Errorf("%w: %s", ErrUnexpectedKey, key)
	}

	if err != nil {
		ferr := fmt.Errorf("failed to parse negate option '%s': %w", key, err)
		return p.wrapError(node, ferr)
	}
	return nil
}
