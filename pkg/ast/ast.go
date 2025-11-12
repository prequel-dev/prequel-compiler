package ast

import (
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/prequel-dev/prequel-logmatch/pkg/match"
	"github.com/rs/zerolog/log"
)

const (
	AstVersion = 1
)

var (
	ErrInvalidEventType        = errors.New("invalid event type")
	ErrInvalidNodeType         = errors.New("invalid node type")
	ErrRootNodeWithoutEventSrc = errors.New("root node has no event source")
	ErrInvalidWindow           = errors.New("invalid window")
	ErrMissingOrigin           = errors.New("missing origin event")
	ErrInvalidAnchor           = errors.New("invalid negate anchor")
	ErrNoTermIdx               = errors.New("no term idx")
)

type AstT struct {
	Nodes []*AstNodeT `json:"nodes"`
}

type AstNodeAddressT struct {
	Version  string  `json:"version"`   // Version of the address format
	Name     string  `json:"name"`      // Name of the node. Currently using type
	RuleHash string  `json:"rule_hash"` // unique semantic identifier for the rule
	Depth    uint32  `json:"depth"`     // Depth of the node in the rule tree
	NodeId   uint32  `json:"node_id"`   // globally unique identifier for the match in the rule tree
	TermIdx  *uint32 `json:"term_idx"`  // Index of term/condition into parent's conditions. Used for assertion to assign term idx into parent machines
}

type AstNodeT struct {
	Metadata AstMetadataT `json:"metadata"` // Metadata for the node
	Children []*AstNodeT  `json:"children"` // Children of the node
	Object   any          `json:"object"`   // Object for the node (e.g. log matcher, state machine, descriptor, etc.)
}

type AstMetadataT struct {
	Type          schema.NodeTypeT `json:"type"`           // Type of the node
	Address       *AstNodeAddressT `json:"address"`        // Address of this node in the rule tree. Must be globally unique in the tree
	ParentAddress *AstNodeAddressT `json:"parent_address"` // Address of the parent node
	NegateOpts    *AstNegateOptsT  `json:"negate_opts"`    // Optional egate options for the node
	RuleId        string           `json:"rule_id"`        // Consistent identifier for the rule that remains consistent through rule logic changes
	Scope         string           `json:"scope"`          // Scope can be an individual node, a cluster, or a set of clusters
	NegIdx        int              `json:"neg_idx"`        // Index into children where negative conditions begin. Equals -1 if no children or no negative conditions
}

// NegateOptsT contains optional negate settings for the matcher object
type AstNegateOptsT struct {
	Window   time.Duration `json:"window"`
	Slide    time.Duration `json:"slide"`
	Anchor   uint32        `json:"anchor"`
	Absolute bool          `json:"absolute"`
}

type AstExtractT struct {
	Name       string `json:"name"`
	JqValue    string `json:"jq_value,omitempty"`
	RegexValue string `json:"regex_value,omitempty"`
}

type AstFieldT struct {
	Field      string          `json:"field"`
	TermValue  match.TermT     `json:"term_value"`
	NegateOpts *AstNegateOptsT `json:"negate_opts"`
	Extracts   []AstExtractT   `json:"extracts"`
}

type AstEventT struct {
	Origin bool   `json:"origin"`
	Source string `json:"source"`
}

type builderT struct {
	CurrentNodeId uint32
	CurrentDepth  uint32
	HasOrigin     bool
}

func NewBuilder() *builderT {
	return &builderT{
		CurrentNodeId: uint32(0),
		CurrentDepth:  uint32(0),
		HasOrigin:     false,
	}
}

func (b *builderT) descendTree(fn func() error) error {
	b.CurrentDepth++
	defer func() { b.CurrentDepth-- }()
	return fn()
}

func Build(data []byte) (*AstT, error) {
	var (
		parseTree *parser.TreeT
		err       error
	)

	if parseTree, err = parser.Parse(data); err != nil {
		log.Error().Any("err", err).Msg("Parser failed")
		return nil, err
	}

	return BuildTree(parseTree)
}

// Build AST from the given parser node in pre-order DFS traversal
func BuildTree(tree *parser.TreeT) (*AstT, error) {
	var (
		ast = &AstT{
			Nodes: make([]*AstNodeT, 0),
		}
	)

	for _, parserNode := range tree.Nodes {

		var (
			rb      = NewBuilder()
			err     error
			termIdx = uint32(0)
			rule    *AstNodeT
		)

		// Recursively build tree
		if rule, err = rb.buildTree(parserNode, nil, &termIdx); err != nil {
			return nil, err
		}

		if !rb.HasOrigin {
			return nil, parserNode.WrapError(ErrMissingOrigin)
		}

		ast.Nodes = append(ast.Nodes, rule)
	}

	return ast, nil
}

func (b *builderT) buildTree(parserNode *parser.NodeT, parentMachineAddress *AstNodeAddressT, termIdx *uint32) (*AstNodeT, error) {

	var (
		machineMatchNode *AstNodeT
		matchNode        *AstNodeT
		children         = make([]*AstNodeT, 0)
		machineAddress   = b.newAstNodeAddress(parserNode.Metadata.RuleHash, parserNode.Metadata.Type.String(), termIdx)
		err              error
	)

	// Build children (either matcher children or nested machines)
	if parserNode.IsMatcherNode() {
		if matchNode, err = b.buildMatcherChildren(parserNode, machineAddress, termIdx); err != nil {
			return nil, err
		}
		children = append(children, matchNode)
	} else if parserNode.IsPromNode() {
		if matchNode, err = b.buildPromQLNode(parserNode, machineAddress, termIdx); err != nil {
			return nil, err
		}
		children = append(children, matchNode)

	} else {
		if children, err = b.buildMachineChildren(parserNode, machineAddress); err != nil {
			return nil, err
		}
	}

	// Build state machine after recursively building children
	if machineMatchNode, err = b.buildStateMachine(parserNode, parentMachineAddress, machineAddress, children); err != nil {
		return nil, err
	}

	machineMatchNode.Children = append(machineMatchNode.Children, children...)

	return machineMatchNode, nil
}

func (b *builderT) newAstNodeAddress(ruleHash, name string, termIdx *uint32) *AstNodeAddressT {
	var address = &AstNodeAddressT{
		Version:  "v" + strconv.FormatInt(int64(AstVersion), 10),
		Name:     name,
		RuleHash: ruleHash,
		Depth:    b.CurrentDepth,
		NodeId:   b.CurrentNodeId,
		TermIdx:  termIdx,
	}

	b.CurrentNodeId++

	return address
}

func newAstNode(parserNode *parser.NodeT, typ schema.NodeTypeT, scope string, parentAddress, address *AstNodeAddressT) *AstNodeT {
	return &AstNodeT{
		Metadata: AstMetadataT{
			RuleId:        parserNode.Metadata.RuleId,
			Address:       address,
			ParentAddress: parentAddress,
			NegIdx:        parserNode.NegIdx,
			Type:          typ,
			Scope:         scope,
		},
	}
}

func (b *builderT) buildMatcherChildren(parserNode *parser.NodeT, machineAddress *AstNodeAddressT, termIdx *uint32) (*AstNodeT, error) {

	var (
		matchNode *AstNodeT
		err       error
	)

	if parserNode.Metadata.Event == nil {
		return nil, parserNode.WrapError(ErrRootNodeWithoutEventSrc)
	}

	if parserNode.Metadata.Event.Source == "" {
		log.Error().
			Any("address", machineAddress).
			Msg("Event missing source")
		return nil, parserNode.WrapError(ErrInvalidEventType)
	}

	// Implied that the root node has an origin event
	b.HasOrigin = true
	parserNode.Metadata.Event.Origin = true

	err = b.descendTree(func() error {
		if matchNode, err = b.buildMatcherNodes(parserNode, machineAddress, termIdx); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return matchNode, nil
}

func (b *builderT) buildMatcherNodes(parserNode *parser.NodeT, machineAddress *AstNodeAddressT, termIdx *uint32) (*AstNodeT, error) {

	// Validation
	switch parserNode.Metadata.Type {
	case schema.NodeTypeLogSeq:
	case schema.NodeTypeLogSet:
	case schema.NodeTypePromQL:
		return b.buildPromQLNode(parserNode, machineAddress, termIdx)
	default:
		return nil, parserNode.WrapError(ErrInvalidNodeType)
	}

	// We currently only support building log matchers in this package
	return b.buildLogMatcherNode(parserNode, machineAddress, termIdx)
}

func (b *builderT) buildMachineChildren(parserNode *parser.NodeT, machineAddress *AstNodeAddressT) ([]*AstNodeT, error) {

	var (
		children = make([]*AstNodeT, 0)
	)

	for i, child := range parserNode.Children {
		var (
			negateOpts      *parser.NegateOptsT
			termIdx         = uint32(i)
			parserChildNode *parser.NodeT
			matchNode       *AstNodeT
			ok              bool
			err             error
		)

		if parserChildNode, ok = child.(*parser.NodeT); !ok {
			return nil, parserNode.WrapError(ErrInvalidNodeType)
		}

		if parserChildNode.Metadata.NegateOpts != nil {
			negateOpts = parserChildNode.Metadata.NegateOpts

			if negateOpts.Anchor > uint32(len(parserNode.Children)) {
				log.Error().
					Msg("Negate anchor is greater than the number of children")
				return nil, parserNode.WrapError(ErrInvalidAnchor)
			}
		}

		// Process nested state machine
		if parserChildNode.Metadata.Event == nil {
			err = b.descendTree(func() error {
				if matchNode, err = b.buildTree(parserChildNode, machineAddress, &termIdx); err != nil {
					return err
				}
				addNegateOpts(matchNode, negateOpts)
				children = append(children, matchNode)
				return nil
			})
			if err != nil {
				return nil, err
			}
			continue
		}

		// If the child has an event/data source, then it is not a state machine. Build it via buildMatcherNodes

		if parserChildNode.Metadata.Event.Origin {
			b.HasOrigin = true
		}

		if parserChildNode.Metadata.Event.Source == "" {
			log.Error().
				Any("address", machineAddress).
				Msg("Event missing source")
			return nil, parserChildNode.WrapError(ErrInvalidEventType)
		}

		err = b.descendTree(func() error {
			if matchNode, err = b.buildMatcherNodes(parserChildNode, machineAddress, &termIdx); err != nil {
				return err
			}
			addNegateOpts(matchNode, negateOpts)
			children = append(children, matchNode)
			return nil
		})
		if err != nil {
			return nil, err
		}

	}

	return children, nil
}

func addNegateOpts(assert *AstNodeT, negateOpts *parser.NegateOptsT) {
	if negateOpts == nil {
		return
	}

	assert.Metadata.NegateOpts = &AstNegateOptsT{
		Window:   negateOpts.Window,
		Slide:    negateOpts.Slide,
		Anchor:   negateOpts.Anchor,
		Absolute: negateOpts.Absolute,
	}
}

func (b *builderT) buildStateMachine(parserNode *parser.NodeT, parentMachineAddress *AstNodeAddressT, machineAddress *AstNodeAddressT, children []*AstNodeT) (*AstNodeT, error) {

	switch parserNode.Metadata.Type {
	case schema.NodeTypeSeq, schema.NodeTypeLogSeq:
		if parserNode.Metadata.Window == 0 {
			log.Error().
				Any("address", machineAddress).
				Msg("Window is required for sequences")
			return nil, parserNode.WrapError(ErrInvalidWindow)
		}
	case schema.NodeTypeSet, schema.NodeTypeLogSet, schema.NodeTypePromQL:
	default:
		log.Error().
			Any("address", machineAddress).
			Str("type", parserNode.Metadata.Type.String()).
			Msg("Invalid node type")
		return nil, parserNode.WrapError(ErrInvalidNodeType)
	}

	return b.buildMachineNode(parserNode, parentMachineAddress, machineAddress, children)
}

func (a *AstNodeAddressT) String() string {

	var (
		addressStr string
	)

	addressStr = fmt.Sprintf("%s.%s.%s.d%d.n%d",
		a.Version,
		a.Name,
		a.RuleHash,
		a.Depth,
		a.NodeId,
	)

	if a.TermIdx != nil {
		addressStr += fmt.Sprintf(".t%d", *a.TermIdx)
	}

	return addressStr
}

func (a *AstNodeAddressT) GetTermIdx() (uint32, error) {
	if a.TermIdx == nil {
		return 0, ErrNoTermIdx
	}
	return *a.TermIdx, nil
}

func (a *AstNodeAddressT) GetDepth() uint32 {
	return a.Depth
}

func (a *AstNodeAddressT) GetRuleHash() string {
	return a.RuleHash
}

func (a *AstNodeAddressT) GetNodeId() uint32 {
	return a.NodeId
}

func traverseTree(node *AstNodeT, wr io.Writer, depth int) error {

	var (
		obj    string
		parent = "nil"
		err    error
	)

	if node.Metadata.ParentAddress != nil {
		parent = node.Metadata.ParentAddress.String()
	}

	obj = fmt.Sprintf("addr=%s parent=%s scope=%s",
		node.Metadata.Address.String(),
		parent,
		node.Metadata.Scope,
	)

	indent := strings.Repeat("  ", depth)

	if _, err = fmt.Fprintf(wr, "depth_%d: %s%s\n", depth, indent, obj); err != nil {
		return err
	}

	for _, c := range node.Children {
		if err = traverseTree(c, wr, depth+1); err != nil {
			return err
		}
	}

	return nil
}

func DrawTree(tree *AstT, path string) error {
	var (
		f   *os.File
		err error
	)

	if f, err = os.Create(path); err != nil {
		return err
	}

	for _, node := range tree.Nodes {
		if err = traverseTree(node, f, 0); err != nil {
			return err
		}
	}

	return nil
}
