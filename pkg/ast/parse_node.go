package ast

import (
	"fmt"
	"time"

	"github.com/goccy/go-yaml/ast"
)

// parseNode is a utility function to parse either a set or sequence node based on the provided type.
// This is used to handle the common logic for parsing both sets and sequences,
// since they share the same structure and keys, with the main difference being the
// node type and validation rules.

func (p *parserT) parseNode(state ruleState, ty AstNodeType, node ast.Node) (AstNode, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	// Push the child node to the state and reset the rank.
	// The parent rank should not affect the rank of a child node.
	child := state.pushNode(ty).setRank(0)

	// Sanity check on child depth; this should be after pushing the child node
	// since that is when the depth is incremented.
	// Note: maxDepth is one based, whereas addr.Depth is zero based, so we check if Depth >= maxDepth.
	if child.addr.Depth >= p.maxDepth {
		err := fmt.Errorf("%w: %d", ErrMaxDepthExceeded, p.maxDepth)
		return nil, p.wrapErrorParent(node, err)
	}

	// Parse the node into a protoNode, which is an intermediate representation
	// that captures the relevant information from the YAML node in a structured way.
	proto, err := p._parseNode(child, ty, mapping)
	if err != nil {
		return nil, err
	}

	// Construct the appropriate AST node (SetNode, SequenceNode, etc) based on the protoNode.
	return p.constructNode(state, child, mapping, proto)
}

// _parseNode is the internal implementation of parseNode which does the actual parsing of the node into a protoNode.
// The node is recursively parsed, generating a protoNode.
// The protoNode is used as an intermediate representation to facilitate validation and transformation.
//
// Expected layout of the node is a mapping with the following optional keys:
// 	Window       string       `yaml:"window,omitempty"`
// 	Correlations []string     `yaml:"correlations,omitempty"`
// 	Event        *ParseEventT `yaml:"event,omitempty"`
// 	Match        []ParseTermT `yaml:"match,omitempty"`
// 	Order        []ParseTermT `yaml:"order,omitempty"`
// 	Negate       []ParseTermT `yaml:"negate,omitempty"`

func (p *parserT) _parseNode(state ruleState, ty AstNodeType, node *ast.MappingNode) (*protoNode, error) {

	var (
		negNode ast.Node
		proto   = protoNode{ty: ty, window: -1} // Default to -1 to indicate no window specified; a window of 0 is valid and means "match events that occur at the same time".}
	)

	for _, v := range node.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {

		case kwEvent:
			if proto.event, err = p.parseEventNode(state, v.Value); err != nil {
				return nil, err
			}

		case kwWindow:
			if proto.window, err = p.parseWindow(v.Value); err != nil {
				return nil, err
			}

		case kwMatch:
			if proto.ty != AstNodeTypeSet {
				err := fmt.Errorf("%w: '%s' key is not allowed in this context", ErrUnexpectedKey, key)
				return nil, p.wrapError(v.Key, err)
			}
			if proto.terms, err = p.parseTerms(state, v.Value, 0); err != nil {
				return nil, err
			}

		case kwOrder:
			if proto.ty != AstNodeTypeSeq {
				err := fmt.Errorf("%w: '%s' key is not allowed in this context", ErrUnexpectedKey, key)
				return nil, p.wrapError(v.Key, err)
			}
			if proto.terms, err = p.parseTerms(state, v.Value, 0); err != nil {
				return nil, err
			}

		case kwNegate:
			// Defer parsing the negate node until the end in case it occurs before the match/order node.
			// This is necessary to get the addressing consistent; ie. negative terms successively addressed
			// after positive terms.
			negNode = v.Value

		case kwCorrelations:
			if proto.correlations, err = p.nodeToStrs(v.Value); err != nil {
				return nil, err
			}

		default:
			err := fmt.Errorf("%w: %s", ErrUnexpectedKey, key)
			return nil, p.wrapError(v.Key, err)
		}
	}

	// Sanity checks
	switch {
	case len(proto.terms) == 0:
		// At least one term has to exist.
		// No terms means there is nothing to match/order, which is not valid.
		// Negate only terms are not allowed.
		return nil, p.wrapErrorParent(node, ErrMissingTerm)
	case ty == AstNodeTypeSeq && len(proto.terms) == 1:
		// A sequence with only one term is not allowed.
		return nil, p.wrapError(findKey(node, kwOrder), ErrShortSequence)
	case proto.window >= 0:
		// Fallthrough; window is defined and valid; zero is allowable.
	case len(proto.terms) > 1 || proto.terms[0].count() > 1:
		// A window is required if there are multiple terms to time bound the match.
		return nil, p.wrapErrorParent(node, ErrMissingWindow)
	default:
		// Windows was not defined on a single term which is allowed.
		// Reset window to 0 for consistency.
		proto.window = 0
	}

	// Process a negate node if it exists.
	// This has been deferred until now to ensure that the match/order terms have been parsed
	// and the rank/offset can be correctly assigned to the negate terms.
	if negNode != nil {
		// Fix up the rank on the state to include the already parsed match/order terms,
		// so that negate terms are ranked after them.
		var (
			err          error
			negateOffset = len(proto.terms)
			negateState  = state.setRank(uint32(negateOffset))
		)
		if proto.negate, err = p.parseTerms(negateState, negNode, negateOffset); err != nil {
			return nil, err
		}
	}

	// Possibly assign origin if not already set.
	p.maybeFixupEventOrigin(proto, state)

	return &proto, nil
}

// Simple rules may not have the origin explicit set in the event.
// This is deprecated behavior; new rules should explicitly specify an origin.
// If not strict and no origin specified, determine if there is exactly
// one leaf term in the rule, and if so, assign origin to that term.

func (p *parserT) maybeFixupEventOrigin(proto protoNode, state ruleState) {

	switch {
	case state.getOrigin() > 0:
		// If origin is already set, do not attempt to infer or assign it.
	case p.strict:
		// In strict mode, origin must be explicitly specified; do not attempt to infer or assign it.
	case state.addr != nil && state.addr.Depth > 0:
		// If this is not the root node, do not attempt to infer or assign origin; it must be set at the root level if applicable.
	case proto.terms[0].field == nil:
		// Either all fields or no fields; if the first term is not a field, do not attempt to infer or assign origin.
	case proto.event == nil:
		// If there is no event, do not attempt to infer or assign origin; origin only applies if there is an event.
	default:
		// Force origin to be true and increment the origin count in the state to reflect this assignment.
		proto.event.Origin = true
		state.incOrigin()
	}
}

// Window is expected to be a duration string, which we parse into a time.Duration.
// A negative duration is not valid, since a window cannot be negative.

func (p *parserT) parseWindow(node ast.Node) (time.Duration, error) {
	window, err := p.nodeToDuration(node)
	if err != nil {
		return 0, err
	}
	if window < 0 {
		return 0, p.wrapError(node, ErrWindowNegative)
	}
	return window, nil
}

// Interpret the prototype and construct the appropriate AST node (SetNode, SequenceNode, etc).

func (p *parserT) constructNode(parent, child ruleState, mapping *ast.MappingNode, proto *protoNode) (AstNode, error) {

	// If there are negate terms, they must either all be field terms or all child node terms,
	// and they must match the type of the match/order terms.
	allFields := proto.terms[0].field != nil

	if len(proto.negate) > 0 {
		if negateAllFields := proto.negate[0].field != nil; allFields != negateAllFields {
			err := fmt.Errorf("%w: positive terms and negate terms must both be either field nodes or child nodes", ErrTermTypeConflict)
			return nil, p.wrapError(p.findNegateTerm(mapping), err)
		}
	}

	// Confirm that the event key is set if required, and not set otherwise.
	switch {
	case allFields && proto.event == nil:
		return nil, p.wrapErrorParent(mapping, ErrMissingEvent)

	case !allFields && proto.event != nil:
		err := fmt.Errorf("%w: an event is not allowed when using child node terms", ErrUnexpectedKey)
		return nil, p.wrapError(findKey(mapping, kwEvent), err)
	}

	// TODO: Validate anchors in negate terms; should be in range of [1, len(terms))

	var node AstNode

	switch {
	case !allFields:
		node = p.constructInnerNode(parent, child, proto)
	default:
		node = p.constructLeafNode(parent, child, proto)
	}

	return node, nil
}

func (p *parserT) constructLeafNode(parent, child ruleState, proto *protoNode) AstNode {

	// A non root leaf node is constructed as a normal leaf node with the appropriate parent and address.
	if child.addr.Depth > 0 {
		return p._constructLeafNode(parent, child, proto)
	}

	// Leaf nodes are not allowed at the root level;
	// they must be contained within an Cluster scoped inner node.
	// This allows the engine to evaluate the match at the cluster level where the publish logic is executing.

	// Construct a placeholder inner node to hold the leaf terms, with the appropriate parent and address.
	// This will assume the original child's address, and the leaf node will be addressed as a child of this placeholder node.
	root := &AstInnerNodeT{
		baseAst: baseAst{address: *child.addr, parent: nil, scope: AstScopeCluster},
	}

	grandChild := child.pushNode(AstNodeTypeSet) // Only sets support 1 term.

	leaf := p._constructLeafNode(child, grandChild, proto)

	term := AstTermT{
		Term: leaf,
	}

	root.Terms = []AstTermT{term}

	return root
}

func (p *parserT) _constructLeafNode(parent, child ruleState, proto *protoNode) *AstMatchLeafT {

	// Translate node type from proto to the appropriate match type; ie. Set -> MatchSet, Seq -> MatchSeq.
	translatedType := proto.ty
	switch proto.ty {
	case AstNodeTypeSet:
		translatedType = AstNodeTypeLogSet
	case AstNodeTypeSeq:
		translatedType = AstNodeTypeLogSeq
	}

	child.addr.Type = translatedType

	return &AstMatchLeafT{
		baseAst:      baseAst{address: *child.addr, parent: parent.addr, scope: AstScopeNode},
		Window:       proto.window,
		Correlations: proto.correlations,
		Terms:        protoTermsToAstFields(proto.terms),
		Negate:       protoTermsToAstFields(proto.negate),
		Event:        *proto.event,
	}
}

func (p *parserT) constructInnerNode(parent, child ruleState, proto *protoNode) AstNode {

	return &AstInnerNodeT{
		baseAst:      baseAst{address: *child.addr, parent: parent.addr, scope: AstScopeCluster},
		Window:       proto.window,
		Correlations: proto.correlations,
		Terms:        protoTermsToAstTerms(proto.terms),
		Negate:       protoTermsToAstTerms(proto.negate),
	}
}

// Given mapping node a level up, find the first negate term node for accurate error reporting on negate term issues.
func (p *parserT) findNegateTerm(mapping *ast.MappingNode) (outNode ast.Node) {
	// Default to the mapping node for error reporting if
	// we cannot find the negate terms for some reason;
	// this should not happen since the negate terms
	// should have already been parsed successfully
	// at this point, but we defensively code against it just in case.
	outNode = mapping

	for _, v := range mapping.Values {
		if k, ok := v.Key.(*ast.StringNode); ok && k.Value == kwNegate {
			outNode = v.Value
			break
		}
	}
	return
}
