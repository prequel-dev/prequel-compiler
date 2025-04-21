package parser

import (
	"errors"
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/pqerr"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"gopkg.in/yaml.v3"
)

var (
	ErrNotSupported  = errors.New("not supported")
	ErrTermNotFound  = errors.New("term not found")
	ErrMissingOrder  = errors.New("sequence missing order")
	ErrMissingMatch  = errors.New("set missing match")
	ErrInvalidWindow = errors.New("invalid window")
)

const (
	docRules  = "rules"
	docRule   = "rule"
	docSeq    = "sequence"
	docSet    = "set"
	docOrder  = "order"
	docWindow = "window"
	docMatch  = "match"
	docNegate = "negate"
	docTerms  = "terms"
)

type TreeT struct {
	Nodes []*NodeT `json:"nodes"`
}

type EventT struct {
	Origin bool   `json:"origin"`
	Source string `json:"source"`
}

type NodeMetadataT struct {
	RuleHash     string           `json:"rule_hash"`
	RuleId       string           `json:"rule_id"`
	CreId        string           `json:"cre_id"`
	Window       time.Duration    `json:"window"`
	Event        *EventT          `json:"event"`
	Type         schema.NodeTypeT `json:"type"`
	Correlations []string         `json:"correlations"`
	NegateOpts   *NegateOptsT     `json:"negate_opts"`
	Pos          pqerr.Pos        `json:"pos"`
}

type NodeT struct {
	Metadata NodeMetadataT `json:"metadata"`
	NegIdx   int           `json:"neg_idx"`
	Children []any         `json:"children"`
}

type NegateOptsT struct {
	Window   time.Duration `json:"window"`
	Slide    time.Duration `json:"slide"`
	Anchor   uint32        `json:"anchor"`
	Absolute bool          `json:"absolute"`
}

type FieldT struct {
	Field      string       `json:"field"`
	StrValue   string       `json:"value"`
	JqValue    string       `json:"jq_value"`
	RegexValue string       `json:"regex_value"`
	Count      int          `json:"count"`
	NegateOpts *NegateOptsT `json:"negate"`
}

type TermsT struct {
	Fields []FieldT `json:"fields"`
}

type MatcherT struct {
	Match  TermsT        `json:"match"`
	Negate TermsT        `json:"negate"`
	Window time.Duration `json:"window"`
}

func newEvent(t *ParseEventT) *EventT {
	return &EventT{
		Source: t.Source,
		Origin: t.Origin,
	}
}

type termsMap map[string]*yaml.Node

func initNode(ruleId, ruleHash string, creId string, yn *yaml.Node) *NodeT {
	return &NodeT{
		Metadata: NodeMetadataT{
			RuleId:   ruleId,
			RuleHash: ruleHash,
			CreId:    creId,
			Pos:      pqerr.Pos{Line: yn.Line, Col: yn.Column},
		},
		NegIdx:   -1,
		Children: make([]any, 0),
	}
}

func seqNodeProps(node *NodeT, seq *ParseSequenceT, order bool, yn *yaml.Node) error {

	node.Metadata.Type = schema.NodeTypeSeq

	if !order {
		return pqerr.Wrap(
			pqerr.Pos{Line: yn.Line, Col: yn.Column},
			node.Metadata.RuleId,
			node.Metadata.RuleHash,
			node.Metadata.CreId,
			ErrMissingOrder,
		)
	}

	if seq.Event != nil {
		node.Metadata.Type = schema.NodeTypeLogSeq
		node.Metadata.Event = newEvent(seq.Event)
	}

	if seq.Window != "" {
		var err error

		if node.Metadata.Window, err = time.ParseDuration(seq.Window); err != nil {
			return pqerr.Wrap(
				pqerr.Pos{Line: yn.Line, Col: yn.Column},
				node.Metadata.RuleId,
				node.Metadata.RuleHash,
				node.Metadata.CreId,
				ErrInvalidWindow,
			)
		}
	}

	if seq.Correlations != nil {
		node.Metadata.Correlations = seq.Correlations
	}

	return nil
}

func setNodeProps(node *NodeT, set *ParseSetT, match bool, yn *yaml.Node) error {

	node.Metadata.Type = schema.NodeTypeSet

	if !match {
		return pqerr.Wrap(
			pqerr.Pos{Line: yn.Line, Col: yn.Column},
			node.Metadata.RuleId,
			node.Metadata.RuleHash,
			node.Metadata.CreId,
			ErrMissingMatch,
		)
	}

	if set.Event != nil {
		node.Metadata.Type = schema.NodeTypeLogSet
		node.Metadata.Event = newEvent(set.Event)
	}

	if set.Window != "" {
		var err error

		winNode, _ := findChild(yn, docWindow)

		if node.Metadata.Window, err = time.ParseDuration(set.Window); err != nil {
			return pqerr.Wrap(
				pqerr.Pos{Line: winNode.Line, Col: winNode.Column},
				node.Metadata.RuleId,
				node.Metadata.RuleHash,
				node.Metadata.CreId,
				ErrInvalidWindow,
			)
		}
	}

	if set.Correlations != nil {
		node.Metadata.Correlations = set.Correlations
	}

	return nil
}

func buildTree(terms map[string]ParseTermT, r ParseRuleT, ruleNode *yaml.Node, termsPositions termsMap) (*NodeT, error) {

	var (
		root *NodeT
		n    *yaml.Node
		ok   bool
	)

	n, ok = findChild(ruleNode, docRule)
	if !ok {
		return nil, errors.New("rule not found")
	}

	switch {
	case r.Rule.Sequence != nil:
		seqNode, _ := findChild(n, docSeq)
		root = initNode(r.Metadata.Id, r.Metadata.Hash, r.Cre.Id, seqNode)
		return buildSequenceTree(root, terms, r, seqNode, termsPositions)
	case r.Rule.Set != nil:
		setNode, _ := findChild(n, docSet)
		root = initNode(r.Metadata.Id, r.Metadata.Hash, r.Cre.Id, setNode)
		return buildSetTree(root, terms, r, setNode, termsPositions)
	default:
		return nil, pqerr.Wrap(
			pqerr.Pos{Line: n.Line, Col: n.Column},
			r.Metadata.Id,
			r.Metadata.Hash,
			r.Cre.Id,
			ErrNotSupported,
		)
	}
}

// buildSequenceTree processes a rule with a Sequence definition.
func buildSequenceTree(root *NodeT, terms map[string]ParseTermT, r ParseRuleT, ruleNode *yaml.Node, termsPositions termsMap) (*NodeT, error) {

	var (
		seq      = r.Rule.Sequence
		orderYn  *yaml.Node
		negateYn *yaml.Node
		ok       bool
	)

	orderYn, ok = findChild(ruleNode, docOrder)
	if !ok {
		return nil, pqerr.Wrap(
			pqerr.Pos{Line: ruleNode.Line, Col: ruleNode.Column},
			root.Metadata.RuleId,
			root.Metadata.RuleHash,
			root.Metadata.CreId,
			ErrMissingOrder,
		)
	}

	// Negate is optional
	negateYn, _ = findChild(ruleNode, docNegate)

	// Build positive children from seq.Order (non-negated)
	// Build negative children from seq.Negate (negated)
	pos, neg, err := buildChildrenGroups(root, terms, seq.Order, seq.Negate, orderYn, negateYn, termsPositions)
	if err != nil {
		return nil, err
	}

	// Apply sequence-specific node properties
	if err := seqNodeProps(root, seq, seq.Order != nil, orderYn); err != nil {
		return nil, err
	}

	// Order positive first, then negatives
	root.Children = append(root.Children, pos...)
	root.Children = append(root.Children, neg...)
	if len(neg) > 0 {
		root.NegIdx = len(pos)
	}

	return root, nil
}

// buildSetTree processes a rule with a Set definition.
func buildSetTree(root *NodeT, terms map[string]ParseTermT, r ParseRuleT, ruleNode *yaml.Node, termsPositions termsMap) (*NodeT, error) {

	var (
		set      = r.Rule.Set
		matchYn  *yaml.Node
		negateYn *yaml.Node
		ok       bool
	)

	matchYn, ok = findChild(ruleNode, docMatch)
	if !ok {
		return nil, pqerr.Wrap(
			pqerr.Pos{Line: ruleNode.Line, Col: ruleNode.Column},
			root.Metadata.RuleId,
			root.Metadata.RuleHash,
			root.Metadata.CreId,
			ErrMissingMatch,
		)
	}

	// Negate is optional
	negateYn, _ = findChild(ruleNode, docNegate)

	pos, neg, err := buildChildrenGroups(root, terms, set.Match, set.Negate, matchYn, negateYn, termsPositions)
	if err != nil {
		return nil, err
	}

	// Apply set-specific node properties
	if err := setNodeProps(root, set, set.Match != nil, ruleNode); err != nil {
		return nil, err
	}

	// Order positive first, then negatives
	root.Children = append(root.Children, pos...)
	root.Children = append(root.Children, neg...)
	if len(neg) > 0 {
		root.NegIdx = len(pos)
	}

	return root, nil
}

// buildChildrenGroups is a helper for building positive/negative children
// in a single pass. The boolean flags specify whether each slice
// is being treated as negated or not.
func buildChildrenGroups(root *NodeT, terms map[string]ParseTermT, matches, negates []ParseTermT, orderYn, negateYn *yaml.Node, termsPositions termsMap) (pos []any, neg []any, err error) {

	pos = []any{}
	neg = []any{}

	if len(matches) > 0 {

		cPos, err := buildChildren(root, terms, matches, false, orderYn, termsPositions)
		if err != nil {
			return nil, nil, err
		}
		pos = append(pos, cPos...)
	}

	if len(negates) > 0 {
		cNeg, err := buildChildren(root, terms, negates, true, negateYn, termsPositions)
		if err != nil {
			return nil, nil, err
		}
		// If double-negatives or other logic is needed, adjust the append here
		neg = append(neg, cNeg...)
	}

	return pos, neg, nil
}

func buildChildren(parent *NodeT, tm map[string]ParseTermT, terms []ParseTermT, parentNegate bool, yn *yaml.Node, termsPositions termsMap) ([]any, error) {
	var (
		children = make([]any, 0)
	)

	for _, term := range terms {
		var (
			node         any
			resolvedTerm ParseTermT
			t            = term
			n            = yn
			ok           bool
			err          error
		)

		if term.StrValue != "" {
			// If the term is not found in the terms map, then use as str value
			if resolvedTerm, ok = tm[term.StrValue]; ok {
				t = resolvedTerm
				if n, ok = termsPositions[term.StrValue]; !ok {
					return nil, pqerr.Wrap(
						pqerr.Pos{Line: yn.Line, Col: yn.Column},
						parent.Metadata.RuleId,
						parent.Metadata.RuleHash,
						parent.Metadata.CreId,
						ErrTermNotFound,
					)
				}

				if term.NegateOpts != nil {
					t.NegateOpts = term.NegateOpts
				}
			}
		}

		if node, err = nodeFromTerm(parent, tm, t, parentNegate, n, termsPositions); err != nil {
			return nil, err
		}

		children = append(children, node)

	}

	return children, nil
}

func nodeFromTerm(parent *NodeT, terms map[string]ParseTermT, term ParseTermT, parentNegate bool, yn *yaml.Node, termsPositions termsMap) (any, error) {

	var (
		node *NodeT
		opts *NegateOptsT
		n    *yaml.Node
		err  error
		ok   bool
	)

	switch {
	case term.Sequence != nil:

		if n, ok = findChild(yn, docSeq); !ok {
			n = yn
		}

		if node, err = buildSequenceNode(parent, terms, term.Sequence, n, termsPositions); err != nil {
			return nil, err
		}

		if term.NegateOpts != nil {
			if opts, err = negateOpts(term); err != nil {
				return nil, err
			}
			node.Metadata.NegateOpts = opts
		}
	case term.Set != nil:

		if n, ok = findChild(yn, docSet); !ok {
			n = yn
		}

		if node, err = buildSetNode(parent, terms, term.Set, n, termsPositions); err != nil {
			return nil, err
		}

		if term.NegateOpts != nil {
			if opts, err = negateOpts(term); err != nil {
				return nil, err
			}
			node.Metadata.NegateOpts = opts
		}
	case term.StrValue != "" || term.JqValue != "" || term.RegexValue != "":
		return parseValue(term, parentNegate)

	default:
		return nil, pqerr.Wrap(
			pqerr.Pos{Line: yn.Line, Col: yn.Column},
			parent.Metadata.RuleId,
			parent.Metadata.RuleHash,
			parent.Metadata.CreId,
			ErrTermNotFound,
		)
	}

	return node, nil
}

func negateOpts(term ParseTermT) (*NegateOptsT, error) {
	var (
		opts = &NegateOptsT{}
		err  error
	)

	if term.NegateOpts.Window != "" {
		if opts.Window, err = time.ParseDuration(term.NegateOpts.Window); err != nil {
			return nil, err
		}
	}

	if term.NegateOpts.Slide != "" {
		if opts.Slide, err = time.ParseDuration(term.NegateOpts.Slide); err != nil {
			return nil, err
		}
	}

	opts.Anchor = term.NegateOpts.Anchor
	opts.Absolute = term.NegateOpts.Absolute

	return opts, nil
}

func buildSequenceNode(parent *NodeT, terms map[string]ParseTermT, seq *ParseSequenceT, yn *yaml.Node, termsPositions termsMap) (*NodeT, error) {
	node := initNode(parent.Metadata.RuleId, parent.Metadata.RuleHash, parent.Metadata.CreId, yn)

	pos, neg, err := buildPosNegChildren(node, terms, seq.Order, seq.Negate, yn, termsPositions)
	if err != nil {
		return nil, err
	}

	// Apply sequence-specific node properties
	if err := seqNodeProps(node, seq, seq.Order != nil, yn); err != nil {
		return nil, err
	}

	node.Children = append(node.Children, pos...)
	node.Children = append(node.Children, neg...)
	if len(neg) > 0 {
		node.NegIdx = len(pos)
	}
	return node, nil
}

func buildSetNode(parent *NodeT, terms map[string]ParseTermT, set *ParseSetT, yn *yaml.Node, termsPositions termsMap) (*NodeT, error) {
	node := initNode(parent.Metadata.RuleId, parent.Metadata.RuleHash, parent.Metadata.CreId, yn)

	pos, neg, err := buildPosNegChildren(node, terms, set.Match, set.Negate, yn, termsPositions)
	if err != nil {
		return nil, err
	}

	// Apply set-specific node properties
	if err := setNodeProps(node, set, set.Match != nil, yn); err != nil {
		return nil, err
	}

	node.Children = append(node.Children, pos...)
	node.Children = append(node.Children, neg...)
	if len(neg) > 0 {
		node.NegIdx = len(pos)
	}
	return node, nil
}

// buildPosNegChildren is a helper for building
// positive and negative children across Sequence and Set
func buildPosNegChildren(node *NodeT, terms map[string]ParseTermT, matches, negates []ParseTermT, yn *yaml.Node, termsPositions termsMap) (pos []any, neg []any, err error) {

	pos, neg = []any{}, []any{}

	if len(matches) > 0 {
		cPos, err := buildChildren(node, terms, matches, false, yn, termsPositions)
		if err != nil {
			return nil, nil, err
		}
		pos = append(pos, cPos...)
	}

	if len(negates) > 0 {
		cNeg, err := buildChildren(node, terms, negates, true, yn, termsPositions)
		if err != nil {
			return nil, nil, err
		}
		neg = append(neg, cNeg...)
	}

	return pos, neg, nil
}

func parseValue(term ParseTermT, negate bool) (*MatcherT, error) {

	var (
		matcher = &MatcherT{}
	)

	switch negate {
	case false:
		matcher.Match.Fields = append(matcher.Match.Fields, FieldT{
			Field:      term.Field,
			StrValue:   term.StrValue,
			JqValue:    term.JqValue,
			RegexValue: term.RegexValue,
			Count:      term.Count,
		})
	case true:

		var (
			err  error
			opts *NegateOptsT
		)

		if term.NegateOpts != nil {
			if opts, err = negateOpts(term); err != nil {
				return nil, err
			}
		}

		matcher.Negate.Fields = append(matcher.Negate.Fields, FieldT{
			Field:      term.Field,
			StrValue:   term.StrValue,
			JqValue:    term.JqValue,
			RegexValue: term.RegexValue,
			Count:      term.Count,
			NegateOpts: opts,
		})
	}

	return matcher, nil
}

func ParseCres(data []byte) (map[string]ParseCreT, error) {
	var (
		config RulesT
		cres   = make(map[string]ParseCreT)
		err    error
	)

	if config, _, err = _parse(data); err != nil {
		return nil, err
	}

	for _, rule := range config.Rules {
		cres[rule.Metadata.Hash] = rule.Cre
	}

	return cres, nil
}

func Parse(data []byte) (*TreeT, error) {

	var (
		docMap         *yaml.Node
		termsNode      *yaml.Node
		config         RulesT
		root           *yaml.Node
		termsPositions termsMap
		err            error
	)

	if config, root, err = _parse(data); err != nil {
		return nil, err
	}

	docMap = root.Content[0]

	rulesRoot, ok := findChild(docMap, docRules)
	if !ok {
		return nil, errors.New("rules not found")
	}

	termsNode, _ = findChild(docMap, docTerms)
	termsPositions = collectTerms(termsNode)

	return parseRules(config.Rules, config.Terms, rulesRoot, termsPositions)
}

func parseRules(rules []ParseRuleT, terms map[string]ParseTermT, rulesRoot *yaml.Node, termsPositions termsMap) (*TreeT, error) {

	var (
		tree = &TreeT{
			Nodes: make([]*NodeT, 0),
		}
	)

	for i, rule := range rules {
		var (
			node     *NodeT
			ruleNode *yaml.Node
			ok       bool
			err      error
		)

		if ruleNode, ok = seqItem(rulesRoot, i); !ok {
			return nil, errors.New("rule not found")
		}

		if node, err = buildTree(terms, rule, ruleNode, termsPositions); err != nil {
			return nil, err
		}

		tree.Nodes = append(tree.Nodes, node)
	}

	return tree, nil
}

func ParseRules(config *RulesT, rulesRoot *yaml.Node, termsPositions termsMap) (*TreeT, error) {
	return parseRules(config.Rules, config.Terms, rulesRoot, termsPositions)
}

func findChild(n *yaml.Node, key string) (*yaml.Node, bool) {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil, false
	}
	for i := 0; i < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		if k.Value == key {
			return v, true
		}
	}
	return nil, false
}

func seqItem(seq *yaml.Node, idx int) (*yaml.Node, bool) {
	if seq == nil || seq.Kind != yaml.SequenceNode || idx < 0 ||
		idx >= len(seq.Content) {
		return nil, false
	}
	return seq.Content[idx], true
}

func collectTerms(doc *yaml.Node) termsMap {
	terms := make(termsMap)
	if doc == nil || doc.Kind != yaml.MappingNode {
		return terms
	}
	for i := 0; i < len(doc.Content); i += 2 {
		key := doc.Content[i] // scalar
		terms[key.Value] = doc.Content[i+1]
	}
	return terms
}
