package ast

import (
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/rs/zerolog/log"
)

type AstPromQL struct {
	Expr     string
	For      time.Duration
	Interval time.Duration
	Event    *AstEventT
}

func (b *builderT) buildPromQLNode(parserNode *parser.NodeT, machineAddress *AstNodeAddressT, termIdx *uint32) (*AstNodeT, error) {

	// Expects one child of type ParsePromQL

	if len(parserNode.Children) != 1 {
		log.Error().Int("child_count", len(parserNode.Children)).Msg("PromQL node must have exactly one child")
		return nil, parserNode.WrapError(ErrInvalidNodeType)
	}

	promNode, ok := parserNode.Children[0].(*parser.PromQLT)

	if !ok {
		log.Error().Any("promql", parserNode.Children[0]).Msg("Failed to build PromQL node")
		return nil, parserNode.WrapError(ErrMissingScalar)
	}

	if promNode.Expr == "" {
		log.Error().Msg("PromQL Expr string is empty")
		return nil, parserNode.WrapError(ErrMissingScalar)
	}

	pn := &AstPromQL{
		Expr: promNode.Expr,
	}

	if parserNode.Metadata.Event != nil {
		pn.Event = &AstEventT{
			Source: parserNode.Metadata.Event.Source,
			Origin: parserNode.Metadata.Event.Origin,
		}
	}

	if promNode.Interval != nil {
		pn.Interval = *promNode.Interval
	}

	if promNode.For != nil {
		pn.For = *promNode.For
	}

	var (
		address = b.newAstNodeAddress(parserNode.Metadata.RuleHash, parserNode.Metadata.Type.String(), termIdx)
		node    = newAstNode(parserNode, parserNode.Metadata.Type, schema.ScopeCluster, machineAddress, address)
	)

	node.Object = pn
	return node, nil

}
