package ast

import (
	"errors"
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/rs/zerolog/log"
)

var (
	ErrInvalidDescriptor = errors.New("invalid descriptor")
)

type AstSeqMatcherT struct {
	Order        []*AstDescriptorT
	Negate       []*AstDescriptorT
	Correlations []string
	Window       time.Duration
}

type AstSetMatcherT struct {
	Match        []*AstDescriptorT
	Negate       []*AstDescriptorT
	Correlations []string
	Window       time.Duration
}

func buildMachineNodes(n *parser.NodeT, children []*AstNodeT, depth, parentMatchId, matchId, termIdx uint32, t AstNodeTypeT) (*AstNodePairT, error) {
	var (
		seqMatcher *AstSeqMatcherT
		setMatcher *AstSetMatcherT
		matchNode  = newAstNode(n, t, schema.ScopeCluster, depth, parentMatchId, matchId, termIdx)
		assertNode = newAstNode(n, NodeTypeDesc, schema.ScopeCluster, depth, parentMatchId, matchId, termIdx)
		err        error
	)

	switch t {
	case NodeTypeSeq:
		if seqMatcher, err = buildSeq(n, children); err != nil {
			return nil, err
		}
		matchNode.Object = seqMatcher
	case NodeTypeSet:
		if setMatcher, err = buildSet(n, children); err != nil {
			return nil, err
		}
		matchNode.Object = setMatcher
	}

	log.Info().
		Uint32("matchId", matchId).
		Uint32("depth", depth).
		Uint32("parentMatchId", parentMatchId).
		Uint32("termIdx", termIdx).
		Msg("Building machine descriptor")

	assertNode.Object = &AstDescriptorT{
		Type:    t,
		MatchId: matchId,
		Depth:   depth,
	}

	return &AstNodePairT{
		Match:      matchNode,
		Descriptor: assertNode,
	}, nil
}

// Iterate over children. Create descs and add them to the rule along with correlations
func buildSeq(n *parser.NodeT, children []*AstNodeT) (*AstSeqMatcherT, error) {

	var (
		sm = &AstSeqMatcherT{
			Order:        make([]*AstDescriptorT, 0),
			Negate:       make([]*AstDescriptorT, 0),
			Correlations: make([]string, 0),
			Window:       n.Metadata.Window,
		}
		descChild = 0
	)

	if n.Metadata.Correlations != nil {
		sm.Correlations = n.Metadata.Correlations
	}

	for _, child := range children {

		var (
			desc *AstDescriptorT
			ok   bool
		)

		if child.Metadata.Type == NodeTypeDesc {

			if desc, ok = child.Object.(*AstDescriptorT); !ok {
				return nil, ErrInvalidDescriptor
			}

			if n.NegIdx > 0 {
				if descChild < n.NegIdx {
					sm.Order = append(sm.Order, desc)
				} else {
					sm.Negate = append(sm.Negate, desc)
				}
			} else {
				sm.Order = append(sm.Order, desc)
			}

			descChild++
		}
	}

	return sm, nil
}

// Iterate over children. Create descs and add them to the rule along with correlations
func buildSet(n *parser.NodeT, children []*AstNodeT) (*AstSetMatcherT, error) {

	var (
		sm = &AstSetMatcherT{
			Match:        make([]*AstDescriptorT, 0),
			Negate:       make([]*AstDescriptorT, 0),
			Correlations: make([]string, 0),
			Window:       n.Metadata.Window,
		}
		descChild = 0
	)

	if n.Metadata.Correlations != nil {
		sm.Correlations = n.Metadata.Correlations
	}

	for _, child := range children {

		var (
			desc *AstDescriptorT
			ok   bool
		)

		if child.Metadata.Type == NodeTypeDesc {

			if desc, ok = child.Object.(*AstDescriptorT); !ok {
				return nil, ErrInvalidDescriptor
			}

			if n.NegIdx > 0 {
				if descChild < n.NegIdx {
					sm.Match = append(sm.Match, desc)
				} else {
					sm.Negate = append(sm.Negate, desc)
				}
			} else {
				sm.Match = append(sm.Match, desc)
			}

			descChild++
		}

		log.Debug().
			Interface("child_node", child).
			Msg("Sequence child")
	}

	return sm, nil
}
