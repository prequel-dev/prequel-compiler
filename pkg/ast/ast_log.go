package ast

import (
	"errors"
	"fmt"
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/pqerr"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/prequel-dev/prequel-logmatch/pkg/match"
	"github.com/rs/zerolog/log"
)

var (
	ErrUnknownField     = errors.New("unknown source field")
	ErrUnknownSrc       = errors.New("unknown source")
	ErrSeqPosConditions = errors.New("sequences require two or more positive conditions")
	ErrMissingScalar    = errors.New("missing string, jq, or regex condition")
)

type AstLogMatcherT struct {
	Event  AstEventT
	Match  []AstFieldT
	Negate []AstFieldT
	Window time.Duration
}

func validateLogSeq(n *parser.NodeT, matches int) error {

	if matches <= 1 {
		log.Error().
			Any("node", n).
			Msg("Sequences require two or more positive conditions")
		return pqerr.Wrap(
			pqerr.Pos{Line: n.Metadata.Pos.Line, Col: n.Metadata.Pos.Col},
			n.Metadata.RuleId,
			n.Metadata.RuleHash,
			n.Metadata.CreId,
			ErrSeqPosConditions,
		)
	}

	if n.Metadata.Window == 0 {
		log.Error().
			Any("node", n).
			Msg("Sequence requires a window")
		return ErrInvalidWindow
	}

	return nil
}

func validateLogSet(n *parser.NodeT, matches int) error {

	// Only one positive condition with a window is not allowed
	if matches == 1 && n.Metadata.Window != 0 {
		log.Error().
			Any("node", n).
			Msg("Windows require two or more positive conditions")
		return ErrInvalidWindow
	}

	// More than one positive condition with no window is not allowed
	if matches > 1 && n.Metadata.Window == 0 {
		log.Error().
			Any("node", n).
			Msg("Window requires two or more positive conditions")
		return ErrInvalidWindow
	}

	return nil
}

func (b *builderT) buildLogMatcherNode(parserNode *parser.NodeT, machineAddress *AstNodeAddressT, termIdx *uint32) (*AstNodeT, error) {

	var (
		matchFields  = make([]AstFieldT, 0)
		negateFields = make([]AstFieldT, 0)
		zlog         = log.With().Any("address", machineAddress).Logger()
		err          error
	)

	for _, child := range parserNode.Children {
		var (
			match *parser.MatcherT
			term  AstFieldT
			src   = parserNode.Metadata.Event.Source
			ok    bool
		)

		// Children are expected to be scalar matcher values
		if match, ok = child.(*parser.MatcherT); !ok {
			zlog.Error().Msg("Expected scalar value")
			return nil, ErrMissingScalar
		}

		// Count match fields and remember values
		for _, field := range match.Match.Fields {
			if field.Count > 1 {
				for i := 0; i < field.Count; i++ {
					if term, err = newMatchTerm(src, field); err != nil {
						zlog.Error().Err(err).Msg("Invalid match field term")
						return nil, err
					}
					matchFields = append(matchFields, term)
				}
			} else {
				if term, err = newMatchTerm(src, field); err != nil {
					zlog.Error().Err(err).Msg("Invalid match field term")
					return nil, err
				}
				matchFields = append(matchFields, term)
			}
		}

		// Count negate fields and remember values
		for _, field := range match.Negate.Fields {
			if field.Count > 1 {
				for range field.Count {
					if term, err = newNegateTerm(src, field); err != nil {
						zlog.Error().Err(err).Msg("Invalid negate field term")
						return nil, err
					}
					negateFields = append(negateFields, term)
				}
			} else {
				if term, err = newNegateTerm(src, field); err != nil {
					zlog.Error().Err(err).Msg("Invalid negate field term")
					return nil, err
				}
				negateFields = append(negateFields, term)
			}
		}
	}

	switch parserNode.Metadata.Type {
	case schema.NodeTypeLogSet:
		if err = validateLogSet(parserNode, len(matchFields)); err != nil {
			return nil, err
		}
	case schema.NodeTypeLogSeq:
		if err = validateLogSeq(parserNode, len(matchFields)); err != nil {
			return nil, err
		}
	default:
		log.Error().
			Any("type", parserNode.Metadata.Type.String()).
			Msg("Invalid node type")
		return nil, ErrInvalidNodeType
	}

	return b.doBuildLogMatcherNode(parserNode, machineAddress, termIdx, matchFields, negateFields)
}

// TODO: remove this once we migrate scope to data sources
func getLogMatchScope(parserNode *parser.NodeT) string {
	if parserNode.Metadata.Event.Source == schema.EventTypeK8s {
		return schema.ScopeCluster
	}
	return schema.ScopeNode
}

func (b *builderT) doBuildLogMatcherNode(parserNode *parser.NodeT, machineAddress *AstNodeAddressT, termIdx *uint32, matchFields []AstFieldT, negateFields []AstFieldT) (*AstNodeT, error) {
	var (
		address   = b.newAstNodeAddress(parserNode.Metadata.RuleHash, parserNode.Metadata.Type.String(), termIdx)
		matchNode = newAstNode(parserNode, parserNode.Metadata.Type, getLogMatchScope(parserNode), machineAddress, address)
	)

	matchNode.Object = &AstLogMatcherT{
		Event: AstEventT{
			Origin: parserNode.Metadata.Event.Origin,
			Source: parserNode.Metadata.Event.Source,
		},
		Match:  matchFields,
		Negate: negateFields,
		Window: parserNode.Metadata.Window,
	}

	return matchNode, nil
}

func knownSrcField(src string, field parser.FieldT) (AstFieldT, error) {
	var (
		t = AstFieldT{
			Field: field.Field,
		}
		f, v string
	)

	switch src {
	case schema.EventTypeK8s:
		switch field.Field {
		case schema.FieldK8sEventReason:
			f = schema.FieldK8sEventReason
			v = field.StrValue
		case schema.FieldK8sEventType:
			f = schema.FieldK8sEventType
			v = field.StrValue
		case schema.FieldK8sEventReasonDetail:
			f = schema.FieldK8sEventReasonDetail
			v = field.StrValue
		default:
			return AstFieldT{}, ErrUnknownField
		}
	default:
		return AstFieldT{}, ErrUnknownSrc
	}

	t.TermValue = match.TermT{
		Type:  match.TermJqJson,
		Value: fmt.Sprintf("select(.%s == \"%s\")", f, v),
	}

	return t, nil
}

func newMatchTerm(src string, field parser.FieldT) (AstFieldT, error) {
	var (
		t     AstFieldT
		count = 0
		err   error
	)

	if t, err = knownSrcField(src, field); err == nil {
		return t, nil
	}

	t = AstFieldT{
		Field: field.Field,
	}

	if field.StrValue != "" {
		t.TermValue = match.TermT{
			Type:  match.TermRaw,
			Value: field.StrValue,
		}
		count++
	}
	if field.JqValue != "" {
		t.TermValue = match.TermT{
			Type:  match.TermJqJson,
			Value: field.JqValue,
		}
		count++
	}
	if field.RegexValue != "" {
		t.TermValue = match.TermT{
			Type:  match.TermRegex,
			Value: field.RegexValue,
		}
		count++
	}

	if count > 1 {
		log.Error().Msg("Only one of str, json, or regex value can be set")
		return AstFieldT{}, ErrInvalidNodeType
	}

	return t, nil
}

func newNegateTerm(src string, field parser.FieldT) (AstFieldT, error) {

	var (
		t   AstFieldT
		err error
	)

	if t, err = newMatchTerm(src, field); err != nil {
		return AstFieldT{}, err
	}

	if field.NegateOpts != nil {
		t.NegateOpts = &AstNegateOptsT{
			Window:   field.NegateOpts.Window,
			Slide:    field.NegateOpts.Slide,
			Anchor:   field.NegateOpts.Anchor,
			Absolute: field.NegateOpts.Absolute,
		}
	}

	return t, nil
}
