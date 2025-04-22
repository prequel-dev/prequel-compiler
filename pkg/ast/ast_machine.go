package ast

import (
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/rs/zerolog/log"
)

type AstSeqMatcherT struct {
	Order        []*AstMetadataT
	Negate       []*AstMetadataT
	Correlations []string
	Window       time.Duration
}

type AstSetMatcherT struct {
	Match        []*AstMetadataT
	Negate       []*AstMetadataT
	Correlations []string
	Window       time.Duration
}

func (b *builderT) buildMachineNode(parserNode *parser.NodeT, parentMachineAddress, machineAddress *AstNodeAddressT, children []*AstNodeT) (*AstNodeT, error) {
	var (
		seqMatcher *AstSeqMatcherT
		setMatcher *AstSetMatcherT
		matchNode  = newAstNode(parserNode, parserNode.Metadata.Type, schema.ScopeCluster, parentMachineAddress, machineAddress)
		err        error
	)

	switch parserNode.Metadata.Type {
	case schema.NodeTypeSeq, schema.NodeTypeLogSeq:
		matchNode.Metadata.Type = schema.NodeTypeSeq
		if seqMatcher, err = buildSeqMatcher(parserNode, children); err != nil {
			return nil, err
		}
		matchNode.Object = seqMatcher
	case schema.NodeTypeSet, schema.NodeTypeLogSet:
		matchNode.Metadata.Type = schema.NodeTypeSet
		if setMatcher, err = buildSetMatcher(parserNode, children); err != nil {
			return nil, err
		}
		matchNode.Object = setMatcher
	default:
		log.Error().
			Str("type", parserNode.Metadata.Type.String()).
			Msg("Invalid node type")
		return nil, ErrInvalidNodeType
	}

	return matchNode, nil
}

// Iterate over children. Create descs and add them to the rule along with correlations
func buildSeqMatcher(n *parser.NodeT, children []*AstNodeT) (*AstSeqMatcherT, error) {
	var (
		sm = &AstSeqMatcherT{
			Correlations: make([]string, 0),
			Window:       n.Metadata.Window,
		}
	)

	if n.Metadata.Correlations != nil {
		sm.Correlations = n.Metadata.Correlations
	}

	sm.Order, sm.Negate = buildTermDescriptors(n, children)

	return sm, nil
}

// Iterate over children. Create descs and add them to the rule along with correlations
func buildSetMatcher(n *parser.NodeT, children []*AstNodeT) (*AstSetMatcherT, error) {

	var (
		sm = &AstSetMatcherT{
			Correlations: make([]string, 0),
			Window:       n.Metadata.Window,
		}
	)

	if n.Metadata.Correlations != nil {
		sm.Correlations = n.Metadata.Correlations
	}

	sm.Match, sm.Negate = buildTermDescriptors(n, children)

	return sm, nil
}

func buildTermDescriptors(parserNode *parser.NodeT, children []*AstNodeT) ([]*AstMetadataT, []*AstMetadataT) {
	var (
		match   = make([]*AstMetadataT, 0)
		negate  = make([]*AstMetadataT, 0)
		descPos int
	)

	for _, child := range children {
		if parserNode.NegIdx > 0 {
			if descPos < parserNode.NegIdx {
				match = append(match, &child.Metadata)
			} else {
				negate = append(negate, &child.Metadata)
			}
		} else {
			match = append(match, &child.Metadata)
		}
		descPos++
	}

	return match, negate
}
