package ast

import "fmt"

type WalkFunc func(node AstNode, negateOpts *AstNegateOptsT) error

// Walk rule tree depth first, calling the provided function on each node.
// The negateOpts parameter will be non-nil if the node is being visited as part of a negation,
// and will contain the negate options for that negation.

func (r AstRuleT) Walk(fn WalkFunc) error {
	return _walk(r.Root, nil, fn)
}

func _walk(node AstNode, negateOpts *AstNegateOptsT, fn WalkFunc) error {

	switch n := node.(type) {

	case *AstInnerNodeT:
		return _walkInnerNode(n, negateOpts, fn)

	case *AstScriptT:
		return _walkScriptNode(n, negateOpts, fn)

	case *AstMatchLeafT, *AstPromT:
		if err := fn(node, negateOpts); err != nil {
			return err
		}

	default:
		return fmt.Errorf("%w: %T", ErrUnknownNodeType, node)
	}

	return nil
}

func _walkInnerNode(node *AstInnerNodeT, negateOpts *AstNegateOptsT, fn WalkFunc) error {

	if err := fn(node, negateOpts); err != nil {
		return err
	}

	for _, term := range node.Terms {
		if err := _walk(term.Term, nil, fn); err != nil {
			return err
		}
	}

	for _, term := range node.Negate {
		termNegateOpts := term.NegateOpts
		if termNegateOpts == nil {
			// Stub as an indicator that we're in a negation context, even if there are no options.
			termNegateOpts = &AstNegateOptsT{}
		}
		if err := _walk(term.Term, termNegateOpts, fn); err != nil {
			return err
		}
	}

	return nil
}

func _walkScriptNode(node *AstScriptT, negateOpts *AstNegateOptsT, fn WalkFunc) error {

	if err := fn(node, negateOpts); err != nil {
		return err
	}

	// No negate options allowed on a script node.
	return _walk(node.Input, nil, fn)
}
