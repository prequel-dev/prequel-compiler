package parser

import (
	"gopkg.in/yaml.v3"
)

// Note that we prefer lower camel case like Kubernetes
// Also, have to keep the JSON tags although we are using YAML.
// The hash function uses JSON serialization, so the JSON tags are required to ensure consistent field names for hashing.

const (
	docRules   = "rules"
	docRule    = "rule"
	docSeq     = "sequence"
	docSet     = "set"
	docOrder   = "order"
	docWindow  = "window"
	docMatch   = "match"
	docNegate  = "negate"
	docTerms   = "terms"
	docSection = "section"
	docVersion = "version"
)

type ParseRuleT struct {
	Metadata ParseRuleMetadataT `yaml:"metadata,omitempty" json:"metadata,omitempty"`
	Cre      ParseCreT          `yaml:"cre,omitempty" json:"cre,omitempty"`
	Rule     ParseRuleDataT     `yaml:"rule,omitempty" json:"rule,omitempty"`
}

type ParseRuleMetadataT struct {
	Name    string `yaml:"name,omitempty" json:"name,omitempty"`
	Id      string `yaml:"id,omitempty" json:"id,omitempty"`
	Hash    string `yaml:"hash,omitempty" json:"hash,omitempty"`
	Gen     uint   `yaml:"generation" json:"generation"`
	Kind    string `yaml:"kind,omitempty" json:"kind,omitempty"`
	Version string `yaml:"version,omitempty" json:"version,omitempty"`
}

type ParseRuleDataT struct {
	Sequence *ParseSequenceT `yaml:"sequence,omitempty"`
	Set      *ParseSetT      `yaml:"set,omitempty"`
}

type ParseApplicationT struct {
	Name          string `yaml:"name,omitempty" json:"name,omitempty"`
	ProcessName   string `yaml:"processName,omitempty" json:"process_name,omitempty"`
	ProcessPath   string `yaml:"processPath,omitempty" json:"process_path,omitempty"`
	ContainerName string `yaml:"containerName,omitempty" json:"container_name,omitempty"`
	ImageUrl      string `yaml:"imageUrl,omitempty" json:"image_url,omitempty"`
	RepoUrl       string `yaml:"repoUrl,omitempty" json:"repo_url,omitempty"`
	Version       string `yaml:"version,omitempty" json:"version,omitempty"`
}

const (
	SeverityCritical = 0
	SeverityHigh     = 1
	SeverityMedium   = 2
	SeverityLow      = 3
	SeverityInfo     = 4
)

type ParseCreT struct {
	Id              string              `yaml:"id,omitempty" json:"id,omitempty"`
	Severity        uint                `yaml:"severity" json:"severity"`
	Title           string              `yaml:"title,omitempty" json:"title,omitempty"`
	Category        string              `yaml:"category,omitempty" json:"category,omitempty"`
	Tags            []string            `yaml:"tags,omitempty" json:"tags,omitempty"`
	Author          string              `yaml:"author,omitempty" json:"author,omitempty"`
	Description     string              `yaml:"description,omitempty" json:"description,omitempty"`
	Impact          string              `yaml:"impact,omitempty" json:"impact,omitempty"`
	ImpactScore     uint                `yaml:"impactScore,omitempty" json:"impact_score,omitempty"`
	Cause           string              `yaml:"cause,omitempty" json:"cause,omitempty"`
	Mitigation      string              `yaml:"mitigation,omitempty" json:"mitigation,omitempty"`
	MitigationScore uint                `yaml:"mitigationScore,omitempty" json:"mitigation_score,omitempty"`
	References      []string            `yaml:"references,omitempty" json:"references,omitempty"`
	Reports         uint                `yaml:"reports,omitempty" json:"reports,omitempty"`
	Applications    []ParseApplicationT `yaml:"applications,omitempty" json:"applications,omitempty"`
}

type ParseSequenceT struct {
	Window       string       `yaml:"window"`
	Correlations []string     `yaml:"correlations,omitempty"`
	Event        *ParseEventT `yaml:"event,omitempty"`
	Origin       bool         `yaml:"origin,omitempty"`
	Order        []ParseTermT `yaml:"order,omitempty"`
	Negate       []ParseTermT `yaml:"negate,omitempty"`
}

type ParseNegateOptsT struct {
	Window   string `yaml:"window,omitempty"`
	Slide    string `yaml:"slide,omitempty"`
	Anchor   uint32 `yaml:"anchor,omitempty"`
	Absolute bool   `yaml:"absolute,omitempty"`
}

type ParseSetT struct {
	Window       string       `yaml:"window,omitempty"`
	Correlations []string     `yaml:"correlations,omitempty"`
	Event        *ParseEventT `yaml:"event,omitempty"`
	Match        []ParseTermT `yaml:"match,omitempty"`
	Negate       []ParseTermT `yaml:"negate,omitempty"`
}

type ParseExtractT struct {
	Name       string `yaml:"name"`
	JqValue    string `yaml:"jq,omitempty"`
	RegexValue string `yaml:"regex,omitempty"`
}

type ParsePromQL struct {
	Expr     string       `yaml:"expr"`
	Interval string       `yaml:"interval,omitempty"`
	For      string       `yaml:"for,omitempty"`
	Event    *ParseEventT `yaml:"event,omitempty"`
}

type ParseScriptT struct {
	Code     string      `yaml:"code"`
	Language string      `yaml:"language,omitempty"` // Assumes 'lua' if empty
	Timeout  string      `yaml:"timeout,omitempty"`  // Uses default if empty; expects duration string
	Input    *ParseTermT `yaml:"input"`              // Required input
}

type ParseEventT struct {
	Source string `yaml:"source"`
	Origin bool   `yaml:"origin,omitempty" json:"origin,omitempty"`
}

type ParseTermT struct {
	Field      string            `yaml:"field,omitempty"`
	StrValue   string            `yaml:"value,omitempty"`
	JqValue    string            `yaml:"jq,omitempty"`
	RegexValue string            `yaml:"regex,omitempty"`
	Count      int               `yaml:"count,omitempty"`
	Set        *ParseSetT        `yaml:"set,omitempty"`
	Sequence   *ParseSequenceT   `yaml:"sequence,omitempty"`
	NegateOpts *ParseNegateOptsT `yaml:",inline,omitempty"`
	PromQL     *ParsePromQL      `yaml:"promql,omitempty"`
	Script     *ParseScriptT     `yaml:"script,omitempty"`
	Extract    []ParseExtractT   `yaml:"extract,omitempty"`
}

func (o *ParseTermT) UnmarshalYAML(unmarshal func(any) error) error {

	// Try to unmarshal as a raw string first.
	// If that fails, unmarshal as a struct.
	// This allows for a shorthand syntax for simple match terms.
	var str string
	if err := unmarshal(&str); err == nil {
		o.StrValue = str
		return nil
	}

	var temp struct {
		Field       string            `yaml:"field"`
		StrValue    string            `yaml:"value"`
		JqValue     string            `yaml:"jq"`
		RegexValue  string            `yaml:"regex"`
		Count       int               `yaml:"count"`
		Set         *ParseSetT        `yaml:"set"`
		Sequence    *ParseSequenceT   `yaml:"sequence"`
		NegateOpts  *ParseNegateOptsT `yaml:",inline"`
		ParsePromQL *ParsePromQL      `yaml:"promql"`
		Script      *ParseScriptT     `yaml:"script"`
		Extract     []ParseExtractT   `yaml:"extract"`
	}
	if err := unmarshal(&temp); err != nil {
		return err
	}
	o.Field = temp.Field
	o.StrValue = temp.StrValue
	o.JqValue = temp.JqValue
	o.RegexValue = temp.RegexValue
	o.Count = temp.Count
	o.Set = temp.Set
	o.Sequence = temp.Sequence
	o.NegateOpts = temp.NegateOpts
	o.PromQL = temp.ParsePromQL
	o.Script = temp.Script
	o.Extract = temp.Extract
	return nil
}

func RootNode(data []byte) (*yaml.Node, error) {
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	return &root, nil
}

type RulesT struct {
	Rules  []ParseRuleT          `yaml:"rules"`
	Root   *yaml.Node            `yaml:"-"`
	TermsT map[string]ParseTermT `yaml:"terms,omitempty"`
	TermsY map[string]*yaml.Node `yaml:"-"`
}

func _parse(data []byte) (*RulesT, *yaml.Node, error) {

	root, err := RootNode(data)
	if err != nil {
		return nil, nil, err
	}

	var rules RulesT
	if err := root.Decode(&rules); err != nil {
		return nil, nil, err

	}

	return &rules, root, nil
}
