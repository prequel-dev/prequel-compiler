package ast

import (
	"fmt"
	"regexp"

	"github.com/goccy/go-yaml/ast"
)

var validBase58Regex = regexp.MustCompile(`^[1-9A-Za-z]{12,}$`)

func (p *parserT) parseMetadataNode(node ast.Node) (*AstMetadataT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var (
		meta AstMetadataT
	)

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {

		case kwName:
			meta.Name, err = p.nodeToString(v.Value)

		case kwId:
			meta.Id, err = p.parseIdNode(v.Value)

		case kwHash:
			meta.Hash, err = p.parseHash(v.Value)

		case kwGen:
			meta.Gen, err = p.parseGen(v.Value)

		case kwKind:
			meta.Kind, err = p.parseKind(v.Value)

		default:
			if p.strict {
				err = p.wrapError(v.Key, ErrUnexpectedKey)
			}
		}

		if err != nil {
			return nil, err
		}
	}

	if meta.Id == "" {
		err := fmt.Errorf("%w: %s", ErrMissingKey, kwId)
		return nil, p.wrapErrorParent(mapping, err)
	}

	if meta.Hash == "" {
		err := fmt.Errorf("%w: %s", ErrMissingKey, kwHash)
		return nil, p.wrapErrorParent(mapping, err)
	}

	return &meta, nil
}

func (p *parserT) parseMetadataNodeMinimal(node ast.Node) (*AstMetadataT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var meta AstMetadataT

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			continue
		}

		switch key {

		case kwHash:
			meta.Hash, _ = p.parseHash(v.Value)

		case kwId:
			meta.Id, _ = p.parseIdNode(v.Value)
		}
	}

	// Only an error if both id and hash are missing, since the name is not required for diagnostics.
	if meta.Id == "" && meta.Hash == "" {
		err := fmt.Errorf("%w: %s and %s", ErrMissingKey, kwId, kwHash)
		return nil, p.wrapErrorParent(mapping, err)
	}

	return &meta, nil
}

func (p *parserT) parseIdNode(v ast.Node) (string, error) {
	s, err := p.nodeToString(v)
	if err != nil {
		return "", err
	}

	// Expect string to be a randomized 16 byte wide base58 encoded string.
	// Ignore strict here; a valid hash is required for correct operation.
	if !validBase58Regex.MatchString(s) {
		return "", p.wrapError(v, ErrBadIdentifier)
	}
	return s, nil
}

func (p *parserT) parseHash(v ast.Node) (string, error) {
	s, err := p.nodeToString(v)
	if err != nil {
		return "", err
	}
	// Expect base58 encoded sha256 hash.
	// Ignore strict here; a valid hash is required for correct operation.
	if !validBase58Regex.MatchString(s) {
		return "", p.wrapError(v, ErrBadHash)
	}
	return s, nil
}

func (p *parserT) parseGen(v ast.Node) (uint32, error) {
	gen, err := p.nodeToUint64(v)
	if err != nil {
		return 0, err
	}
	// Sanity check generation number; should be a positive integer, and not unreasonably high.
	if gen > uint64(p.maxGen) {
		err := fmt.Errorf("%w: generation value must be a positive integer less or equal to %d", ErrBadGen, p.maxGen)
		return 0, p.wrapError(v, err)
	}

	return uint32(gen), nil
}

func (p *parserT) parseKind(v ast.Node) (string, error) {
	s, err := p.nodeToString(v)
	if err != nil {
		return "", err
	}
	switch s {
	case KindPrequel, KindCustom:
	default:
		if p.strict {
			err := fmt.Errorf("%w: kind value must be either 'prequel' or 'custom'", ErrBadKind)
			return "", p.wrapError(v, err)
		}
	}
	return s, nil
}
