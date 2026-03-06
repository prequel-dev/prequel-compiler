package compiler

import (
	"fmt"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
	"github.com/prequel-dev/prequel-logmatch/pkg/match"
	"github.com/rs/zerolog/log"
)

func toLogResets(terms []ast.AstFieldT) []match.ResetT {
	resets := make([]match.ResetT, 0, len(terms))
	for _, term := range terms {

		if term.NegateOpts == nil {
			resets = append(resets, match.ResetT{
				Term: term.TermValue,
			})
			continue
		}

		resets = append(resets, match.ResetT{
			Term:     term.TermValue,
			Window:   term.NegateOpts.Window.Nanoseconds(),
			Slide:    term.NegateOpts.Slide.Nanoseconds(),
			Anchor:   uint8(term.NegateOpts.Anchor),
			Absolute: term.NegateOpts.Absolute,
		})

		log.Debug().Any("reset", resets[len(resets)-1]).Msg("Adding log resets")
	}
	return resets
}

func toLogTerms(fields []ast.AstFieldT) []match.TermT {
	terms := make([]match.TermT, 0, len(fields))
	for _, field := range fields {
		// match interface does not yet support explicit counts, do dupe.
		// TODO: Revise when log matcher supports counts.
		cnt := max(field.Count, 1)
		for range cnt {
			terms = append(terms, field.TermValue)
		}
	}
	return terms
}

func ObjLogMatcher(runtime RuntimeI, node *ast.AstMatchLeafT) (*ObjT, error) {
	var (
		err error
		obj = NewObj(node, ObjTypeMatcher)
	)

	obj.Event.Origin = node.Event.Origin
	obj.Event.Source = node.Event.Source

	params := MatchParamsT{
		Address:       node.Address(),
		ParentAddress: node.Parent(),
		Origin:        node.Event.Origin,
	}

	obj.Cb = runtime.NewCbMatch(params)

	switch node.Type() {
	case ast.AstNodeTypeLogSeq:
		if obj.Object, err = makeLogSeqObjects(node); err != nil {
			return nil, err
		}

	case ast.AstNodeTypeLogSet:

		if obj.Object, err = makeLogSetObjects(node); err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("%w: %s", ErrUnsupportedNodeType, node.Type())
	}

	return obj, nil
}

func makeLogSeqObjects(node *ast.AstMatchLeafT) (any, error) {

	switch {
	case len(node.Negate) > 0:
		return match.NewInverseSeq(
			node.Window.Nanoseconds(),
			toLogTerms(node.Terms),
			toLogResets(node.Negate),
		)

	// A sequence with a single term and no negation should be specified as a set,
	// irregardless of the count. It is not a sequence of events, but a single event
	// that may occur multiple times (on count > 1).
	case len(node.Terms) == 1:
		return nil, ErrSequenceSingleMatch

	default:
		return match.NewMatchSeq(node.Window.Nanoseconds(), toLogTerms(node.Terms)...)
	}
}

func makeLogSetObjects(node *ast.AstMatchLeafT) (any, error) {

	// Expand the terms before checking length as toLogs will
	// expand terms with counts > 1 into multiple terms,
	// which may affect whether we can use a single match or need a set.
	logTrms := toLogTerms(node.Terms)

	switch {
	case len(node.Negate) > 0:
		return match.NewInverseSet(
			node.Window.Nanoseconds(),
			logTrms,
			toLogResets(node.Negate),
		)

	case len(logTrms) == 1:
		return match.NewMatchSingle(logTrms[0])

	default:
		return match.NewMatchSet(node.Window.Nanoseconds(), logTrms...)
	}
}
