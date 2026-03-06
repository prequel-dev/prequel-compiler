package ast

import (
	"fmt"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

const AstVersion = "v1"

const (
	SeverityCritical = 0
	SeverityHigh     = 1
	SeverityMedium   = 2
	SeverityLow      = 3
	SeverityInfo     = 4
)

const (
	KindPrequel = "prequel"
	KindCustom  = "custom"
)

type AstRuleT struct {
	Root     AstNode
	Cre      *AstCreT
	Metadata AstMetadataT
}

type AstNode interface {
	Type() AstNodeType
	Scope() AstScopeT
	Address() AstNodeAddressT
	Parent() *AstNodeAddressT
}

type AstNodeAddressT struct {
	Type     AstNodeType // Type of node
	RuleId   string      // RuleId is the unique identifier for the rule
	RuleHash string      // unique semantic identifier for the rule
	Rank     uint32      // Index of term/condition into parent's conditions. Used for assertion to assign term idx into parent machines
	Depth    uint32      // Depth of the node in the rule tree
	NodeId   uint32      // Globally unique identifier for the match in the rule tree
}

func (a AstNodeAddressT) String() string {
	return fmt.Sprintf("%s.%s.%s.d%d.n%d.t%d",
		AstVersion,
		a.Type,
		a.RuleHash,
		a.Depth,
		a.NodeId,
		a.Rank,
	)
}

type AstMetadataT struct {
	Name string
	Id   string
	Hash string
	Kind string
	Gen  uint32
}

type AstTermT struct {
	Term       AstNode
	NegateOpts *AstNegateOptsT
}

type AstInnerNodeT struct {
	baseAst
	Window       time.Duration
	Correlations []string
	Terms        []AstTermT
	Negate       []AstTermT
}

type AstFieldT struct {
	Count      uint64
	Field      string
	TermValue  match.TermT
	NegateOpts *AstNegateOptsT
	Extracts   []AstExtractT
}

type AstMatchLeafT struct {
	baseAst
	Window       time.Duration
	Correlations []string
	Terms        []AstFieldT
	Negate       []AstFieldT
	Event        AstEventT
}

type AstNodeType int

const (
	AstNodeTypeSet AstNodeType = iota
	AstNodeTypeSeq
	AstNodeTypeLogSet
	AstNodeTypeLogSeq
	AstNodeTypePromQL
	AstNodeTypeScript
)

func (t AstNodeType) String() string {
	switch t {
	case AstNodeTypeSet:
		return nodeTypeSet
	case AstNodeTypeSeq:
		return nodeTypeSeq
	case AstNodeTypeLogSet:
		return nodeTypeLogSet
	case AstNodeTypeLogSeq:
		return nodeTypeLogSeq
	case AstNodeTypePromQL:
		return nodeTypePromQL
	case AstNodeTypeScript:
		return nodeTypeScript
	default:
		return nodeTypeUnknown
	}
}

type AstScopeT int

const (
	AstScopeNode AstScopeT = iota
	AstScopeCluster
	AstScopeOrganization
	AstScopeGlobal
)

func (s AstScopeT) String() string {
	switch s {
	case AstScopeNode:
		return scopeTypeNode
	case AstScopeCluster:
		return scopeTypeCluster
	case AstScopeOrganization:
		return scopeTypeOrganization
	case AstScopeGlobal:
		return scopeTypeGlobal
	default:
		return scopeTypeUnknown
	}
}

type AstCreT struct {
	Id              string
	Severity        uint
	Title           string
	Category        string
	Tags            []string
	Author          string
	Description     string
	Impact          string
	ImpactScore     uint
	Cause           string
	Mitigation      string
	MitigationScore uint
	References      []string
	Reports         uint
	Applications    []AstAppT
}

type AstEventT struct {
	Source string
	Origin bool
}

type AstAppT struct {
	Name          string
	ProcessName   string
	ProcessPath   string
	ContainerName string
	ImageUrl      string
	RepoUrl       string
	Version       string
}

type AstExtractT struct {
	Name       string
	JqValue    string
	RegexValue string
}

type AstPromT struct {
	baseAst
	Expr     string
	For      time.Duration
	Interval time.Duration
	Event    *AstEventT
}

type AstScriptT struct {
	baseAst
	Code     string
	Language string
	Timeout  time.Duration
	Input    AstNode
}

type AstNegateOptsT struct {
	Window   time.Duration
	Slide    time.Duration
	Anchor   uint32
	Absolute bool
}

type baseAst struct {
	scope   AstScopeT
	address AstNodeAddressT
	parent  *AstNodeAddressT
}

func (b baseAst) Address() AstNodeAddressT {
	return b.address
}

func (b baseAst) Type() AstNodeType {
	return b.address.Type
}

func (b baseAst) Scope() AstScopeT {
	return b.scope
}

func (b baseAst) Parent() *AstNodeAddressT {
	return b.parent
}
