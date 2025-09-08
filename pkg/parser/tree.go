package parser

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/prequel-dev/prequel-compiler/pkg/pqerr"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

var (
	ErrRuleNotFound     = errors.New("rule not found")
	ErrRuleRootNotFound = errors.New("missing rule section")
	ErrNotSupported     = errors.New("not supported")
	ErrTermNotFound     = errors.New("term not found")
	ErrMissingOrder     = errors.New("'sequence' missing 'order'")
	ErrMissingMatch     = errors.New("'set' missing 'match'")
	ErrInvalidWindow    = errors.New("invalid 'window'")
	ErrTermsMapping     = errors.New("'terms' must be a mapping")
	ErrDuplicateTerm    = errors.New("duplicate term name")
	ErrMissingRuleId    = errors.New("missing rule id")
	ErrMissingRuleHash  = errors.New("missing rule hash")
	ErrMissingCreId     = errors.New("missing cre id")
	ErrInvalidCreId     = errors.New("invalid cre id")
	ErrInvalidRuleId    = errors.New("invalid rule id (must be base58)")
	ErrInvalidRuleHash  = errors.New("invalid rule hash (must be base58)")
	ErrExtractName      = errors.New("invalid extract name (alphanumeric and underscores only)")
)

var (
	validCreIdRegex     = regexp.MustCompile(`^[A-Za-z0-9-]{4,}$`)
	validBase58IdRegex  = regexp.MustCompile(`^[1-9A-Za-z]{12,}$`)
	validateExtractName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
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

type ExtractT struct {
	Name       string `json:"name"`
	JqValue    string `json:"jq_value,omitempty"`
	RegexValue string `json:"regex_value,omitempty"`
}

type FieldT struct {
	Field      string       `json:"field"`
	StrValue   string       `json:"value"`
	JqValue    string       `json:"jq_value"`
	RegexValue string       `json:"regex_value"`
	Count      int          `json:"count"`
	NegateOpts *NegateOptsT `json:"negate"`
	Extract    []ExtractT   `json:"extract,omitempty"`
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

func isValidBase58Id(s string) bool {
	return validBase58IdRegex.MatchString(s)
}

func isValidCreId(s string) bool {
	return validCreIdRegex.MatchString(s)
}

func isValidExtractName(s string) bool {
	return validateExtractName.MatchString(s)
}

func initNode(ruleId, ruleHash string, creId string, yn *yaml.Node) (*NodeT, error) {

	if ruleId == "" {
		return nil, ErrMissingRuleId
	}

	if !isValidBase58Id(ruleId) {
		return nil, ErrInvalidRuleId
	}

	if ruleHash == "" {
		return nil, ErrMissingRuleHash
	}

	if !isValidBase58Id(ruleHash) {
		return nil, ErrInvalidRuleHash
	}

	if creId == "" {
		return nil, ErrMissingCreId
	}

	if !isValidCreId(creId) {
		return nil, ErrInvalidCreId
	}

	return &NodeT{
		Metadata: NodeMetadataT{
			RuleId:   ruleId,
			RuleHash: ruleHash,
			CreId:    creId,
			Pos:      pqerr.Pos{Line: yn.Line, Col: yn.Column},
		},
		NegIdx:   -1,
		Children: make([]any, 0),
	}, nil
}

func seqNodeProps(node *NodeT, seq *ParseSequenceT, order bool, yn *yaml.Node) error {

	node.Metadata.Type = schema.NodeTypeSeq

	if !order {
		return node.WrapError(ErrMissingOrder)
	}

	if seq.Event != nil {
		node.Metadata.Type = schema.NodeTypeLogSeq
		node.Metadata.Event = newEvent(seq.Event)
	}

	if seq.Window != "" {
		var err error

		if winNode, ok := findChild(yn, docWindow); ok {
			node.Metadata.Pos = pqerr.Pos{Line: winNode.Line, Col: winNode.Column}
		}

		if node.Metadata.Window, err = time.ParseDuration(seq.Window); err != nil {
			return node.WrapError(ErrInvalidWindow)
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
		return node.WrapError(ErrMissingMatch)
	}

	if set.Event != nil {
		node.Metadata.Type = schema.NodeTypeLogSet
		node.Metadata.Event = newEvent(set.Event)
	}

	if set.Window != "" {
		var err error

		if winNode, ok := findChild(yn, docWindow); ok {
			node.Metadata.Pos = pqerr.Pos{Line: winNode.Line, Col: winNode.Column}
		}

		if node.Metadata.Window, err = time.ParseDuration(set.Window); err != nil {
			return node.WrapError(ErrInvalidWindow)
		}
	}

	if set.Correlations != nil {
		node.Metadata.Correlations = set.Correlations
	}

	return nil
}

func buildTree(termsT map[string]ParseTermT, r ParseRuleT, ruleNode *yaml.Node, termsY map[string]*yaml.Node) (*NodeT, error) {

	var (
		root *NodeT
		n    *yaml.Node
		ok   bool
		err  error
	)

	n, ok = findChild(ruleNode, docRule)
	if !ok {
		return nil, pqerr.Wrap(
			pqerr.Pos{Line: ruleNode.Line, Col: ruleNode.Column},
			r.Metadata.Id,
			r.Metadata.Hash,
			r.Cre.Id,
			ErrRuleRootNotFound,
		)
	}

	switch {
	case r.Rule.Sequence != nil:
		seqNode, _ := findChild(n, docSeq)
		root, err = initNode(r.Metadata.Id, r.Metadata.Hash, r.Cre.Id, seqNode)
		if err != nil {
			return nil, pqerr.Wrap(
				pqerr.Pos{Line: n.Line, Col: n.Column},
				r.Metadata.Id,
				r.Metadata.Hash,
				r.Cre.Id,
				err,
			)
		}
		return buildSequenceTree(root, termsT, r, seqNode, termsY)
	case r.Rule.Set != nil:
		setNode, _ := findChild(n, docSet)
		root, err = initNode(r.Metadata.Id, r.Metadata.Hash, r.Cre.Id, setNode)
		if err != nil {
			return nil, pqerr.Wrap(
				pqerr.Pos{Line: n.Line, Col: n.Column},
				r.Metadata.Id,
				r.Metadata.Hash,
				r.Cre.Id,
				err,
			)
		}
		return buildSetTree(root, termsT, r, setNode, termsY)
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
func buildSequenceTree(root *NodeT, termsT map[string]ParseTermT, r ParseRuleT, ruleNode *yaml.Node, termsY map[string]*yaml.Node) (*NodeT, error) {

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
			r.Metadata.Id,
			r.Metadata.Hash,
			r.Cre.Id,
			ErrMissingOrder,
		)
	}

	// Negate is optional
	negateYn, _ = findChild(ruleNode, docNegate)

	// Build positive children from seq.Order (non-negated)
	// Build negative children from seq.Negate (negated)
	pos, neg, err := buildChildrenGroups(root, termsT, seq.Order, seq.Negate, orderYn, negateYn, termsY)
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
func buildSetTree(root *NodeT, termsT map[string]ParseTermT, r ParseRuleT, ruleNode *yaml.Node, termsY map[string]*yaml.Node) (*NodeT, error) {

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
			r.Metadata.Id,
			r.Metadata.Hash,
			r.Cre.Id,
			ErrMissingMatch,
		)
	}

	// Negate is optional
	negateYn, _ = findChild(ruleNode, docNegate)

	pos, neg, err := buildChildrenGroups(root, termsT, set.Match, set.Negate, matchYn, negateYn, termsY)
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
func buildChildrenGroups(root *NodeT, termsT map[string]ParseTermT, matches, negates []ParseTermT, orderYn, negateYn *yaml.Node, termsY map[string]*yaml.Node) (pos []any, neg []any, err error) {

	pos = []any{}
	neg = []any{}

	if len(matches) > 0 {

		cPos, err := buildChildren(root, termsT, matches, false, orderYn, termsY)
		if err != nil {
			return nil, nil, err
		}
		pos = append(pos, cPos...)
	}

	if len(negates) > 0 {
		cNeg, err := buildChildren(root, termsT, negates, true, negateYn, termsY)
		if err != nil {
			return nil, nil, err
		}
		// If double-negatives or other logic is needed, adjust the append here
		neg = append(neg, cNeg...)
	}

	return pos, neg, nil
}

func buildChildren(parent *NodeT, tm map[string]ParseTermT, terms []ParseTermT, parentNegate bool, yn *yaml.Node, termsY map[string]*yaml.Node) ([]any, error) {
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
				if n, ok = termsY[term.StrValue]; !ok {
					return nil, parent.WrapError(ErrTermNotFound)
				}

				if term.NegateOpts != nil {
					t.NegateOpts = term.NegateOpts
				}
			}
		}

		if node, err = nodeFromTerm(parent, tm, t, parentNegate, n, termsY); err != nil {
			return nil, err
		}

		children = append(children, node)

	}

	return children, nil
}

func nodeFromTerm(parent *NodeT, termsT map[string]ParseTermT, term ParseTermT, parentNegate bool, yn *yaml.Node, termsY map[string]*yaml.Node) (any, error) {

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

		if node, err = buildSequenceNode(parent, termsT, term.Sequence, n, termsY); err != nil {
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

		if node, err = buildSetNode(parent, termsT, term.Set, n, termsY); err != nil {
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
		parent.Metadata.Pos = pqerr.Pos{Line: yn.Line, Col: yn.Column}
		return nil, parent.WrapError(ErrTermNotFound)
	}

	return node, nil
}

func extractTerms(terms []ParseExtractT) ([]ExtractT, error) {
	var extracts []ExtractT
	for _, term := range terms {

		if !isValidExtractName(term.Name) {
			return nil, ErrExtractName
		}

		extracts = append(extracts, ExtractT{
			Name:       term.Name,
			JqValue:    term.JqValue,
			RegexValue: term.RegexValue,
		})
	}
	return extracts, nil
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

func buildSequenceNode(parent *NodeT, termsT map[string]ParseTermT, seq *ParseSequenceT, yn *yaml.Node, termsY map[string]*yaml.Node) (*NodeT, error) {
	node, err := initNode(parent.Metadata.RuleId, parent.Metadata.RuleHash, parent.Metadata.CreId, yn)
	if err != nil {
		return nil, parent.WrapError(err)
	}

	pos, neg, err := buildPosNegChildren(node, termsT, seq.Order, seq.Negate, yn, termsY)
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

func buildSetNode(parent *NodeT, termsT map[string]ParseTermT, set *ParseSetT, yn *yaml.Node, termsY map[string]*yaml.Node) (*NodeT, error) {
	node, err := initNode(parent.Metadata.RuleId, parent.Metadata.RuleHash, parent.Metadata.CreId, yn)
	if err != nil {
		return nil, parent.WrapError(err)
	}

	pos, neg, err := buildPosNegChildren(node, termsT, set.Match, set.Negate, yn, termsY)
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
func buildPosNegChildren(node *NodeT, termsT map[string]ParseTermT, matches, negates []ParseTermT, yn *yaml.Node, termsY map[string]*yaml.Node) (pos []any, neg []any, err error) {

	pos, neg = []any{}, []any{}

	if len(matches) > 0 {
		cPos, err := buildChildren(node, termsT, matches, false, yn, termsY)
		if err != nil {
			return nil, nil, err
		}
		pos = append(pos, cPos...)
	}

	if len(negates) > 0 {
		cNeg, err := buildChildren(node, termsT, negates, true, yn, termsY)
		if err != nil {
			return nil, nil, err
		}
		neg = append(neg, cNeg...)
	}

	return pos, neg, nil
}

func parseValue(term ParseTermT, negate bool) (*MatcherT, error) {

	var (
		err     error
		matcher = &MatcherT{}
	)

	switch negate {
	case false:
		var extracts []ExtractT
		if len(term.Extract) > 0 {
			if extracts, err = extractTerms(term.Extract); err != nil {
				return nil, err
			}
		}

		matcher.Match.Fields = append(matcher.Match.Fields, FieldT{
			Field:      term.Field,
			StrValue:   term.StrValue,
			JqValue:    term.JqValue,
			RegexValue: term.RegexValue,
			Count:      term.Count,
			Extract:    extracts,
		})
	case true:

		var (
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

func Parse(data []byte, opts ...ParseOptT) (*TreeT, error) {

	var (
		config *RulesT
		err    error
	)

	if config, err = Unmarshal(data); err != nil {
		return nil, err
	}

	return ParseRules(config, opts)
}

func Unmarshal(data []byte) (*RulesT, error) {

	var (
		docMap    *yaml.Node
		termsNode *yaml.Node
		config    RulesT
		root      *yaml.Node
		ok        bool
		err       error
	)

	if config, root, err = _parse(data); err != nil {
		return nil, err
	}

	docMap = root.Content[0]

	config.Root, ok = findChild(docMap, docRules)
	if !ok {
		return nil, errors.New("rules not found")
	}

	termsNode, ok = findChild(docMap, docTerms)
	if ok {
		config.TermsY = collectTermsY(termsNode)
	}

	return &config, nil
}

func Hash(h string) string {
	hash := sha1.Sum([]byte(h))
	return base58.Encode(hash[:])
}

// HashRule to provide a unique identity for the rule.
// The hash is based on the rule's content, excluding previous hash calculations.

func HashRule(rule ParseRuleT) (string, error) {
	rule.Metadata.Hash = "" // Hash is what we are generating here, not semantically important
	return _hashRule(rule)
}

// StableHash to provide a unique stable identity for the rule.  It can be used for dupe detection.
// The hash is based on the rule's content, excluding metadata that is not semantically important.

func StableHash(rule ParseRuleT) (string, error) {

	// Strip out versioning metadata before calculating the stable hash.
	// The versioning metadata is not semantically important for the rule's content,
	// so we can safely ignore it for the purpose of hashing.
	// This is important to ensure that the hash remains consistent across changes
	// that do not affect the rule's content, such as version bumps or metadata changes.

	// The field rule.Metadata.Id is considered part of the rules identity and should be included in the stable hash.
	// Rules can change over time having the following properties:
	// - Metadata.Id: Unique identifier for the rule, which is immutable for the lifetime of the rule.
	// - Metadata.Hash: A hash of the rule's content, which is regenerated on every semantic change.
	// - Metadata.Version: A version string that *should* be incremented on changes, but is not semantically important.
	// - Metadata.Gen: A generation counter that is incremented on every change, but is not semantically important.

	rule.Metadata.Gen = 0      // Gen is bumped on every semantic change, so we don't want it in the hash
	rule.Metadata.Version = "" // Version may be bumped on change, also not semantically important
	return HashRule(rule)
}

func _hashRule(rule ParseRuleT) (string, error) {
	// json.Marshal to produce deterministic output
	jsonBytes, err := json.Marshal(rule)
	if err != nil {
		return "", err
	}

	hash := sha256.Sum256(jsonBytes)

	return base58.Encode(hash[:]), nil
}

func parseRules(rules []ParseRuleT, termsT map[string]ParseTermT, rulesRoot *yaml.Node, termsY map[string]*yaml.Node, opts ...ParseOptT) (*TreeT, error) {

	var (
		o    = parseOpts(opts...)
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
			log.Error().
				Int("index", i).
				Msg("Rule not found")
			return nil, ErrRuleNotFound
		}

		if o.genIds {
			if rule.Metadata.Id == "" {
				rule.Metadata.Id = Hash(rule.Cre.Id)
				log.Warn().
					Str("rule.Metadata.Id", rule.Metadata.Id).
					Str("rule.Cre.Id", rule.Cre.Id).
					Msg("Rule id is empty, generating from cre id")
			}
			if rule.Metadata.Hash == "" {
				if rule.Metadata.Hash, err = HashRule(rule); err != nil {
					return nil, err
				}
				log.Warn().
					Str("rule.Cre.Id", rule.Cre.Id).
					Str("rule.Metadata.Id", rule.Metadata.Id).
					Str("rule.Metadata.Hash", rule.Metadata.Hash).
					Msg("Rule hash is empty, generating from rule data")
			}
		}

		if node, err = buildTree(termsT, rule, ruleNode, termsY); err != nil {
			return nil, err
		}

		tree.Nodes = append(tree.Nodes, node)
	}

	return tree, nil
}

func ParseRules(config *RulesT, opts []ParseOptT) (*TreeT, error) {
	return parseRules(config.Rules, config.TermsT, config.Root, config.TermsY, opts...)
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

func collectTermsY(doc *yaml.Node) map[string]*yaml.Node {
	termsY := make(map[string]*yaml.Node)
	if doc == nil || doc.Kind != yaml.MappingNode {
		return termsY
	}
	for i := 0; i < len(doc.Content); i += 2 {
		key := doc.Content[i] // scalar
		termsY[key.Value] = doc.Content[i+1]
	}
	return termsY
}

func (n *NodeT) WrapError(err error) error {
	return pqerr.Wrap(
		pqerr.Pos{Line: n.Metadata.Pos.Line, Col: n.Metadata.Pos.Col},
		n.Metadata.RuleId,
		n.Metadata.RuleHash,
		n.Metadata.CreId, err)
}

type ParseOptT func(*parseOptsT)

func WithGenIds() func(*parseOptsT) {
	return func(o *parseOptsT) {
		o.genIds = true
	}
}

type parseOptsT struct {
	genIds bool
}

func parseOpts(opts ...ParseOptT) *parseOptsT {
	o := &parseOptsT{}
	for _, opt := range opts {
		opt(o)
	}

	return o
}

func Read(rdr io.Reader, opts ...ParseOptT) (*RulesT, error) {
	var (
		allRules = &RulesT{
			Rules:  make([]ParseRuleT, 0),
			TermsT: make(map[string]ParseTermT),
			TermsY: make(map[string]*yaml.Node),
		}
		root    *yaml.Node
		dupes   = make(map[string]struct{})
		decoder *yaml.Decoder
		o       = parseOpts(opts...)
		ok      bool
	)

	decoder = yaml.NewDecoder(rdr)

LOOP:
	for {
		// 1) grab the raw document (with positions) ---------------------------
		var doc yaml.Node
		if err := decoder.Decode(&doc); err != nil {
			switch err {
			case io.EOF:
				break LOOP
			default:
				log.Error().Err(err).Msg("fail yaml decode")
				return nil, err
			}
		}
		if len(doc.Content) == 0 { // empty document ("---\n")
			continue
		}

		root = doc.Content[0]

		if sec, ok := findChild(root, docSection); ok { // key “section” exists?
			if sec.Kind == yaml.ScalarNode && sec.Value == docVersion {
				// Entire document is a version footer: ignore it and move on
				continue
			}
		}

		allRules.Root, ok = findChild(root, docRules)
		if !ok {
			return nil, errors.New("rules not found")
		}

		// 2) walk keys in that mapping ---------------------------------------
		for i := 0; i < len(root.Content); i += 2 {
			kNode, vNode := root.Content[i], root.Content[i+1]
			switch kNode.Value {

			case "rules":
				var rules []ParseRuleT
				if err := vNode.Decode(&rules); err != nil {
					return nil, err
				}
				if !o.genIds {
					if err := checkDuplicates(rules, dupes); err != nil {
						return nil, err
					}
				}
				allRules.Rules = append(allRules.Rules, rules...)

			case "terms":

				termsTNew, termsYNew, err := parseTermsNode(vNode) // vNode is *yaml.Node for this block
				if err != nil {
					return nil, err
				}

				if allRules.TermsT == nil {
					allRules.TermsT = make(map[string]ParseTermT)
				}

				if err := mergeTerms(allRules.TermsT, allRules.TermsY, termsTNew, termsYNew); err != nil {
					return nil, err
				}
			default:
				// unknown section – ignore or warn
			}
		}
	}

	return allRules, nil
}

func mergeTerms(dst map[string]ParseTermT, dstPos map[string]*yaml.Node, src map[string]ParseTermT, srcPos map[string]*yaml.Node) error {
	for k, v := range src {
		if _, dup := dst[k]; dup {
			return ErrDuplicateTerm
		}
		dst[k] = v
		dstPos[k] = srcPos[k]
	}
	return nil
}

func checkDuplicates(rules []ParseRuleT, seen map[string]struct{}) error {
	for _, r := range rules {
		for _, id := range []string{r.Metadata.Hash, r.Metadata.Id, r.Cre.Id} {
			if _, dup := seen[id]; dup {
				return fmt.Errorf("duplicate id=%s (cre=%s)", id, r.Cre.Id)
			}
			seen[id] = struct{}{}
		}
	}
	return nil
}

func parseTermsNode(n *yaml.Node) (map[string]ParseTermT, map[string]*yaml.Node, error) {
	var m = make(map[string]ParseTermT)
	var p = make(map[string]*yaml.Node)

	if n.Kind != yaml.MappingNode {
		log.Error().Msg("terms node is not a mapping")
		return nil, nil, ErrTermsMapping
	}

	for i := 0; i < len(n.Content); i += 2 {
		kNode, vNode := n.Content[i], n.Content[i+1]

		if _, dup := m[kNode.Value]; dup {
			return nil, nil, ErrDuplicateTerm
		}

		var t ParseTermT
		if err := vNode.Decode(&t); err != nil {
			return nil, nil, err
		}

		m[kNode.Value] = t
		p[kNode.Value] = vNode
	}

	return m, p, nil
}
