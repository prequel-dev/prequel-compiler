package ast

import (
	"errors"
	"testing"
)

func TestAstRuleT_Walk_VisitsAllNodes(t *testing.T) {
	// Build a tree:
	// InnerNode
	//   - Terms: [Leaf1, Script]
	//   - Negate: [Leaf2 (with NegateOpts)]
	leaf1 := &AstMatchLeafT{baseAst: baseAst{scope: AstScopeNode}}
	leaf2 := &AstMatchLeafT{baseAst: baseAst{scope: AstScopeNode}}
	script := &AstScriptT{baseAst: baseAst{scope: AstScopeNode}, Input: leaf2}
	inner := &AstInnerNodeT{
		baseAst: baseAst{scope: AstScopeCluster},
		Terms: []AstTermT{
			{Term: leaf1},
			{Term: script},
		},
		Negate: []AstTermT{
			{Term: leaf2, NegateOpts: &AstNegateOptsT{Window: 1}},
		},
	}
	rule := AstRuleT{Root: inner}

	var visited []AstNode
	var negated []AstNode
	err := rule.Walk(func(node AstNode, nopts *AstNegateOptsT) error {
		visited = append(visited, node)
		if nopts != nil {
			negated = append(negated, node)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Walk returned error: %v", err)
	}
	// Should visit inner, leaf1, script, leaf2 (twice: once as script input, once as negate)
	if len(visited) != 5 {
		t.Errorf("expected 5 nodes visited, got %d", len(visited))
	}
	if len(negated) == 0 {
		t.Errorf("expected at least one node visited with negateOpts")
	}
}

func TestAstRuleT_Walk_ErrorPropagation(t *testing.T) {
	leaf := &AstMatchLeafT{baseAst: baseAst{scope: AstScopeNode}}
	rule := AstRuleT{Root: leaf}
	myErr := errors.New("fail")
	err := rule.Walk(func(node AstNode, nopts *AstNegateOptsT) error {
		return myErr
	})
	if !errors.Is(err, myErr) {
		t.Errorf("Walk did not propagate error, got %v", err)
	}
}

type unknownNode struct{}

func (unknownNode) Type() AstNodeType        { return AstNodeType(99) }
func (unknownNode) Scope() AstScopeT         { return AstScopeT(99) }
func (unknownNode) Address() AstNodeAddressT { return AstNodeAddressT{} }
func (unknownNode) Parent() *AstNodeAddressT { return nil }

func TestAstRuleT_Walk_UnknownNodeType(t *testing.T) {
	rule := AstRuleT{Root: unknownNode{}}
	err := rule.Walk(func(node AstNode, nopts *AstNegateOptsT) error { return nil })
	if err == nil || !errors.Is(err, ErrUnknownNodeType) {
		t.Errorf("Walk did not return ErrUnknownNodeType for unknown node, got %v", err)
	}
}

func Test_walkInnerNode_ErrorPropagation(t *testing.T) {
	leaf := &AstMatchLeafT{baseAst: baseAst{scope: AstScopeNode}}
	inner := &AstInnerNodeT{
		baseAst: baseAst{scope: AstScopeCluster},
		Terms: []AstTermT{
			{Term: leaf},
		},
		Negate: []AstTermT{
			{Term: leaf, NegateOpts: &AstNegateOptsT{}},
		},
	}
	myErr := errors.New("inner error")
	// Error on the node itself
	called := 0
	err := _walkInnerNode(inner, nil, func(node AstNode, nopts *AstNegateOptsT) error {
		called++
		if called == 1 {
			return myErr
		}
		return nil
	})
	if !errors.Is(err, myErr) {
		t.Errorf("_walkInnerNode did not propagate error from fn(node), got %v", err)
	}
	// Error on term
	called = 0
	err = _walkInnerNode(inner, nil, func(node AstNode, nopts *AstNegateOptsT) error {
		called++
		if called == 2 {
			return myErr
		}
		return nil
	})
	if !errors.Is(err, myErr) {
		t.Errorf("_walkInnerNode did not propagate error from term, got %v", err)
	}
	// Error on negate
	called = 0
	err = _walkInnerNode(inner, nil, func(node AstNode, nopts *AstNegateOptsT) error {
		called++
		if called == 3 {
			return myErr
		}
		return nil
	})
	if !errors.Is(err, myErr) {
		t.Errorf("_walkInnerNode did not propagate error from negate, got %v", err)
	}
}

func Test_walkScriptNode_ErrorPropagation(t *testing.T) {
	leaf := &AstMatchLeafT{baseAst: baseAst{scope: AstScopeNode}}
	script := &AstScriptT{baseAst: baseAst{scope: AstScopeNode}, Input: leaf}
	myErr := errors.New("script error")
	// Error on the script node itself
	called := 0
	err := _walkScriptNode(script, nil, func(node AstNode, nopts *AstNegateOptsT) error {
		called++
		if called == 1 {
			return myErr
		}
		return nil
	})
	if !errors.Is(err, myErr) {
		t.Errorf("_walkScriptNode did not propagate error from fn(node), got %v", err)
	}
	// Error on the input node
	called = 0
	err = _walkScriptNode(script, nil, func(node AstNode, nopts *AstNegateOptsT) error {
		called++
		if called == 2 {
			return myErr
		}
		return nil
	})
	if !errors.Is(err, myErr) {
		t.Errorf("_walkScriptNode did not propagate error from input, got %v", err)
	}
}
