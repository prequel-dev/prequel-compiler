package ast

import (
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/rs/zerolog/log"
)

type AstScriptT struct {
	Code     string
	Language string
	Timeout  time.Duration
}

// Build the child Ast nodes for the script.
//
// Script nodes are internal nodes with one input node.
// The parser node for a script contains a ScriptT struct as its first child, followed by one input node.
// Build the the child nodes for the script node by building each of the parser node's children;
// the first child is skipped since it is the script definition, and the remaining child is built as the input to the script node.

func (b *builderT) buildScriptChildren(parserNode *parser.NodeT, machineAddress *AstNodeAddressT) ([]*AstNodeT, error) {

	if len(parserNode.Children) != 2 {
		log.Error().Int("child_count", len(parserNode.Children)).Msg("Script node must have two children")
		return nil, parserNode.WrapError(ErrInvalidNodeType)
	}

	termIdx := uint32(1)

	child := parserNode.Children[1]
	parserChildNode, ok := child.(*parser.NodeT)
	if !ok {
		log.Error().Any("child", child).Msg("Failed to build Script child node")
		return nil, parserNode.WrapError(ErrInvalidNodeType)
	}

	leaf, err := b.buildLeafChild(parserChildNode, machineAddress, &termIdx)

	switch {
	case err != nil:
		return nil, err
	case leaf != nil:
		return []*AstNodeT{leaf}, nil
	default:
		node, err := b.buildTree(parserChildNode, machineAddress, &termIdx)
		if err != nil {
			return nil, err
		}

		return []*AstNodeT{node}, nil
	}
}

// Validate script definitions and build the script node.

func (b *builderT) buildScriptNode(parserNode *parser.NodeT, parentMachineAddress, machineAddress *AstNodeAddressT) (*AstNodeT, error) {

	// Expects exactly two children, the first should be parser.ScriptT, the following is the script input node.

	if len(parserNode.Children) != 2 {
		log.Error().Int("child_count", len(parserNode.Children)).Msg("Script node must have exactly two children")
		return nil, parserNode.WrapError(ErrInvalidNodeType)
	}

	scriptNode, ok := parserNode.Children[0].(*parser.ScriptT)

	if !ok {
		log.Error().Any("script", parserNode.Children[0]).Msg("Failed to build Script node")
		return nil, parserNode.WrapError(ErrMissingScalar)
	}

	if scriptNode.Code == "" {
		log.Error().Msg("Script code string is empty")
		return nil, parserNode.WrapError(ErrMissingScalar)
	}

	pn := &AstScriptT{
		Code:     scriptNode.Code,
		Language: scriptNode.Language,
	}

	if scriptNode.Timeout != nil {
		pn.Timeout = *scriptNode.Timeout
	}

	var (
		node = newAstNode(parserNode, parserNode.Metadata.Type, schema.ScopeCluster, parentMachineAddress, machineAddress)
	)

	node.Object = pn
	return node, nil
}
