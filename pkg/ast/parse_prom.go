package ast

import (
	"errors"
	"fmt"

	"github.com/goccy/go-yaml/ast"
)

func (p *parserT) parsePromQLNode(state ruleState, node ast.Node) (*AstPromT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	child := state.pushNode(AstNodeTypePromQL)

	var (
		prom = AstPromT{
			baseAst: baseAst{
				scope:   AstScopeCluster,
				address: *child.addr,
				parent:  state.addr,
			},
		}
	)

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {

		case kwPromExpr:
			prom.Expr, err = p.parsePromExpr(v.Value)

		case kwPromInterval:
			prom.Interval, err = p.nodeToDurationPositive(v.Value)

		case kwPromFor:
			prom.For, err = p.nodeToDurationPositive(v.Value)

		case kwPromEvent:
			prom.Event, err = p.parseEventNode(child, v.Value)

		default:
			err = p.wrapError(v.Key, ErrUnexpectedKey)
		}

		if err != nil {
			return nil, err
		}
	}

	return &prom, nil
}

func (p *parserT) parsePromExpr(node ast.Node) (string, error) {

	s, err := p.nodeToString(node)
	if err != nil {
		return "", err
	}

	if s == "" {
		err := fmt.Errorf("%w: PromQL expression cannot be empty", ErrBadPromQL)
		return "", p.wrapError(node, err)
	}

	if err := p.validatePromQL(s); err != nil {
		err := errors.Join(ErrBadPromQL, err)
		return "", p.wrapError(node, err)
	}

	return s, nil
}
