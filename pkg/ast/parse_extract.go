package ast

import (
	"fmt"
	"regexp"

	"github.com/goccy/go-yaml/ast"
)

var validateExtractName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)

func (p *parserT) parseExtracts(node ast.Node) ([]AstExtractT, error) {

	seq, err := p.nodeToSequence(node)
	if err != nil {
		return nil, err
	}

	var (
		extracts []AstExtractT
		dupes    = make(map[string]struct{}, len(seq.Values)) // Track extract names to detect duplicates.
	)

	for _, v := range seq.Values {

		extract, err := p.parseExtractNode(v, dupes)
		if err != nil {
			return nil, err
		}

		extracts = append(extracts, *extract)
	}

	if len(extracts) == 0 && p.strict {
		err := fmt.Errorf("%w: 'extract' must contain at least one extract definition", ErrMissingKey)
		return nil, p.wrapError(node, err)
	}

	return extracts, nil
}

func (p *parserT) parseExtractNode(node ast.Node, dupeMap map[string]struct{}) (*AstExtractT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var (
		hasValue bool
		extract  AstExtractT
	)

	checkConflict := func(v ast.Node, key string) error {
		if hasValue {
			err := fmt.Errorf("%w: multiple value keys in extract: %s", ErrUnexpectedKey, key)
			return p.wrapError(v, err)
		}
		hasValue = true
		return nil
	}

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {
		case kwExtractName:
			extract.Name, err = p.parseExtractName(v.Value)

			if err == nil {
				if _, exists := dupeMap[extract.Name]; exists {
					err = fmt.Errorf("%w: '%s' is duplicated", ErrDupeExtractName, extract.Name)
					return nil, p.wrapError(v.Value, err)
				}
				dupeMap[extract.Name] = struct{}{}
			}

		case kwExtractJq:
			if err := checkConflict(v, key); err != nil {
				return nil, err
			}
			extract.JqValue, err = p.nodeToJq(v.Value)

		case kwExtractRegex:
			if err := checkConflict(v, key); err != nil {
				return nil, err
			}
			var exp *regexp.Regexp
			if exp, err = p.nodeToRegex(v.Value); err == nil {
				extract.RegexValue = exp.String()
			}

		default:
			err = p.wrapError(v.Key, ErrUnexpectedKey)
		}

		if err != nil {
			return nil, err
		}
	}

	if extract.Name == "" {
		err := fmt.Errorf("%w: missing required '%s' key in extract definition", ErrMissingKey, kwExtractName)
		return nil, p.wrapErrorParent(node, err)
	}

	return &extract, nil
}

func (p *parserT) parseExtractName(v ast.Node) (string, error) {

	s, err := p.nodeToString(v)
	if err != nil {
		return "", err
	}

	// Ignore strict here; a valid extract name is required for correct operation.
	if !validateExtractName.MatchString(s) {
		err := fmt.Errorf("%w: extract name must start with a letter and contain only letters, numbers, or underscores", ErrBadExtractName)
		return "", p.wrapError(v, err)
	}
	return s, nil

}
