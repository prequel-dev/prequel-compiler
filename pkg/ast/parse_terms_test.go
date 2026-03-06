package ast

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/goccy/go-yaml/ast"
	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

const (
	stubRuleId   = "test"
	stubRuleHash = "hash"
	stubSource   = "stubSource"
)

func TestParseTerms(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		negateOff int
		strict    bool
		want      []*protoTerm
		wantErr   error
		wantPos   int
	}{
		{
			name: "valid term with one field",
			yamlInput: `
terms:
  - "simple string match"
`,
			want: []*protoTerm{
				{
					field: &protoField{
						StrValue: "simple string match",
					},
				},
			},
		},
		{
			name: "valid term with two fields",
			yamlInput: `
terms:
  - "simple string match 1"
  - "simple string match 2"
`,
			want: []*protoTerm{
				{
					field: &protoField{
						StrValue: "simple string match 1",
					},
				},
				{
					field: &protoField{
						StrValue: "simple string match 2",
					},
				},
			},
		},
		{
			name: "valid term with one child",
			yamlInput: `
terms:
  - set:
      event:
        source: stubSource
        origin: true
      match:
      - "child node match"
`,
			want: []*protoTerm{
				{
					child: genStubChild([]string{"child node match"}),
				},
			},
		},
		{
			name:   "valid term with two children",
			strict: true,
			yamlInput: `
terms:
  - set:
      event:
        source: stubSource
        origin: true
      match:
      - "child node match 1"
  - set:
      event:
        source: stubSource
      match:
      - "child node match 2"
`,
			want: genStubChildren([]string{"child node match 1", "child node match 2"}),
		},
		{
			name: "invalid term with mixed field and child nodes",
			yamlInput: `
terms:
  - "simple string match"
  - set:
      event:
        source: stubSource
      match:
      - "child node match 2"
`,
			wantErr: ErrTermTypeConflict,
			wantPos: 42, // Position of the "set" key in the second term, which is where the type inconsistency is detected.
		},
		{
			name: "invalid term with mixed field and child nodes reversed",
			yamlInput: `
terms:
  - set:
      event:
        source: stubSource
      match:
      - "child node match 1"
  - "simple string match"
`,
			wantErr: ErrTermTypeConflict,
			wantPos: 104, // Position of the second term node (the "simple string match" string), which is where the type inconsistency is detected.
		},
		{
			name: "test bad terms type",
			yamlInput: `
terms: "not a sequence"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 9, // Position of the "not a sequence" value node (string instead of sequence)
		},
		{
			name: "empty terms strict mode",
			yamlInput: `
terms: []
`,
			wantErr: ErrMissingTerm,
			strict:  true,
			wantPos: 9, // Position of the empty sequence node, which is where the missing term error is detected in strict mode.
		},
		{
			name: "empty terms tolerant mode",
			yamlInput: `
terms: []
`,
			wantErr: nil,
			strict:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)
			// Should be mapping Node with single key "terms"
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}
			v := mapping.Values[0]
			p := &parserT{strict: tt.strict, root: node, maxRank: 10, maxDepth: 5}
			state := newRuleState(&AstMetadataT{Id: stubRuleId, Hash: stubRuleHash})
			got, err := p.parseTerms(state, v.Value, tt.negateOff)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTerms() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseTerm(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		negateOff int
		failJQ    bool
		strict    bool
		want      *protoTerm
		wantErr   error
		wantPos   int
	}{
		{
			name: "simple string term",
			yamlInput: `
term: "simple string match"
`,
			want: &protoTerm{
				field: &protoField{
					StrValue: "simple string match",
				},
			},
		},
		{
			name: "bad term type",
			yamlInput: `
term: 11
`,
			wantErr: ErrUnexpectedType,
			wantPos: 8, // Position of the "11" value node (integer instead of string or mapping)
		},
		{
			name: "bad key type",
			yamlInput: `
term:
  123: "value"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 10, // Position of the "123" key node (integer instead of string)
		},
		{
			name: "simple field term with value key and field",
			yamlInput: `
term:
  field: "field1"
  value: "simple string match with value key"
`,
			want: &protoTerm{
				field: &protoField{
					Count:    1,
					Field:    "field1",
					StrValue: "simple string match with value key",
				},
			},
		},
		{
			name: "simple field term with jq key",
			yamlInput: `
term:
  jq: ".name"
`,
			want: &protoTerm{
				field: &protoField{
					Count:   1,
					JqValue: ".name",
				},
			},
		},
		{
			name: "simple field term with bad jq key",
			yamlInput: `
term:
  jq: "bad jq"
`,
			want: &protoTerm{
				field: &protoField{
					Count:   1,
					JqValue: ".name",
				},
			},
			failJQ:  true,
			wantErr: ErrBadJq,
			wantPos: 14, // Position of the "bad jq" string node, which is where the invalid jq error is detected.
		},
		{
			name: "simple field term with regex key",
			yamlInput: `
term:
  regex: ".*"
`,
			want: &protoTerm{
				field: &protoField{
					Count:      1,
					RegexValue: ".*",
				},
			},
		},
		{
			name: "simple field term with bad regex key",
			yamlInput: `
term:
  regex: "(abc"
`,
			wantErr: ErrBadRegex,
			wantPos: 17, // Position of the "(abc" string node, which is where the invalid regex error is detected.
		},
		{
			name: "simple field term with empty regex key",
			yamlInput: `
term:
  regex: ""
`,
			wantErr: ErrBadRegex,
			wantPos: 17, // Position of the empty string node, which is where the invalid regex error is detected.
		},
		{
			name: "no term defined",
			yamlInput: `
term:
  field: "field1"
`,
			wantErr: ErrBadField,
			wantPos: 6, // Positiong of the term mapping node, which is where the missing term definition is detected.
		},
		{
			name: "empty string term",
			yamlInput: `
term:
  value: ""
`,
			wantErr: ErrBadField,
			wantPos: 17, // Position of the empty string node, which is where the empty value error is detected.
		},
		{
			name: "regex that matches empty string",
			yamlInput: `
term:
  regex: "^$"
`,
			want: &protoTerm{
				field: &protoField{
					Count:      1,
					RegexValue: "^$",
				},
			},
		},
		{
			name: "field already defined",
			yamlInput: `
term:
  value: "field defined first"
  promql:
    expr: "rate(http_requests_total[5m])"
`,
			wantErr: ErrTermRedefined,
			wantPos: 41, // Position of the "promql" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "promql already defined",
			yamlInput: `
term:
  promql:
    expr: "rate(http_requests_total[5m])"
  value: "field defined first"
`,
			wantErr: ErrTermRedefined,
			wantPos: 62, // Position of the "value" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "negate key not allowed on non-negate term",
			yamlInput: `
term:
  value: "simple string match"
  anchor: 0
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 41, // Position of the "value" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "negate key not allowed on promql terms",
			yamlInput: `
term:
  promql:
    expr: "rate(http_requests_total[5m])"
  anchor: 0
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 62, // Position of the "value" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "negate key not allowed on promql terms reversed",
			yamlInput: `
term:
  anchor: 0
  promql:
    expr: "rate(http_requests_total[5m])"
`,
			negateOff: 1,
			wantErr:   ErrUnexpectedKey,
			wantPos:   22, // Position of the "value" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "negate key not allowed on script terms",
			yamlInput: `
term:
  script:
    code: "console.log('hello world')"
    input:
      set:
        event:
          source: stubSource
          origin: true
        match:
        - "child node match"
  anchor: 0
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 192, // Position of the "anchor" key node, which is where the unexpected key error is detected in the context of a script term.
		},
		{
			name: "negate key not allowed on script terms reversed",
			yamlInput: `
term:
  anchor: 0
  script:
    code: "console.log('hello world')"
    input:
      set:
        event:
          source: stubSource
          origin: true
        match:
        - "child node match"
`,
			negateOff: 1,
			wantErr:   ErrUnexpectedKey,
			wantPos:   22, // Position of the "script" key node, which is where the unexpected key error is detected in the context of a script term.
		},
		{
			name: "jq and regex keys not allowed on the same term",
			yamlInput: `
term:
  jq: ".name"
  regex: ".*"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 24, // Position of the "regex" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "jq and regex keys not allowed on the same term reversed",
			yamlInput: `
term:
  regex: ".*"
  jq: ".name"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 24, // Position of the "jq" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "value and regex keys not allowed on the same term",
			yamlInput: `
term:
  regex: ".*"
  value: "some value"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 24, // Position of the "value" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "value and regex keys not allowed on the same term reversed",
			yamlInput: `
term:
  value: "some value"
  regex: ".*"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 32, // Position of the "regex" key node, which is where the term redefinition error is detected (second term definition in the same term).
		},
		{
			name: "simple count",
			yamlInput: `
term:
  value: "some value"
  count: 11
`,
			want: &protoTerm{
				field: &protoField{
					Count:    11,
					StrValue: "some value",
				},
			},
		},
		{
			name: "negative count",
			yamlInput: `
term:
  value: "some value"
  count: -1
`,
			wantErr: ErrUnexpectedType,
			wantPos: 39, // Position of the "-1" integer node, which is where the invalid count error is detected (negative count value).
		},
		{
			name: "zero count",
			yamlInput: `
term:
  value: "some value"
  count: 0
`,
			wantErr: ErrZeroCount,
			wantPos: 32, // Position of the "count" key node, which is where the zero count error is detected.
		},
		{
			name: "non-one count on negative term",
			yamlInput: `
term:
  value: "some value"
  count: 2
`,
			negateOff: 1,
			wantErr:   ErrNegateCount,
			wantPos:   32, // Position of the "count" key node, which is where the non-one count error is detected on a negative term.
		},
		{
			name: "valid negate opts",
			yamlInput: `
term:
  value: "some value"
  anchor: 0
  window: 1m
  slide: 30s
  absolute: true
`,
			negateOff: 1,
			want: &protoTerm{
				field: &protoField{
					Count:    1,
					StrValue: "some value",
				},
				negateOpts: &AstNegateOptsT{
					Anchor:   0,
					Window:   time.Minute,
					Slide:    time.Second * 30,
					Absolute: true,
				},
			},
		},
		{
			name: "negate opts negative anchor",
			yamlInput: `
term:
  value: "some value"
  anchor: -1
`,
			negateOff: 1,
			wantErr:   ErrUnexpectedType,
			wantPos:   40, // Position of the "anchor" key node, which is where the bad anchor error is detected.
		},
		{
			name: "negate opts bad anchor",
			yamlInput: `
term:
  value: "some value"
  anchor: 1
`,
			negateOff: 1,
			wantErr:   ErrBadAnchor,
			wantPos:   40, // Position of the "anchor" key node, which is where the bad anchor error is detected.
		},
		{
			name: "negate opts overflow anchor",
			yamlInput: `
term:
  value: "some value"
  anchor: 256
`,
			negateOff: 257,
			wantErr:   ErrBadAnchor,
			wantPos:   40, // Position of the "anchor" key node, which is where the bad anchor error is detected.
		},
		{
			name: "disallow negate on script term",
			yamlInput: `
term:
  promql:
    expr: "rate(http_requests_total[5m])"
  anchor: 0
`,
			negateOff: 1,
			wantErr:   ErrUnexpectedKey,
			wantPos:   10, // Position of the "anchor" key node, which is where the unexpected key error is detected in the context of a promql term.
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)

			// Should be mapping Node with single key "term"
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}
			v := mapping.Values[0]

			validateJQ := stubValidator
			if tt.failJQ {
				validateJQ = func(s string) error {
					return errors.New("invalid jq")
				}
			}

			p := &parserT{
				strict:         tt.strict,
				root:           node,
				maxRank:        10,
				maxDepth:       5,
				validateJQ:     validateJQ,
				validatePromQL: stubValidator,
				validateLua:    stubValidator,
			}

			state := newRuleState(&AstMetadataT{Id: stubRuleId, Hash: stubRuleHash})
			got, err := p.parseTerm(state, v.Value, tt.negateOff)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseTerm() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestTermsCoverageHack(t *testing.T) {
	// This test exists solely to increase coverage of the default cases in the switch statements of parseTerm and parseNegateOpts,
	// which should be unreachable if parseTermAsMap is correctly implemented, but we include them for completeness.
	key := &ast.StringNode{Value: "unexpected"}
	p := &parserT{}
	_, err := p.parseTermChild(ruleState{}, key, nil, nil)

	if !errors.Is(err, ErrUnexpectedKey) {
		t.Errorf("expected ErrUnexpectedKey, got %v", err)
	}

	err = p.parseNegateOpts("unexpected", nil, nil, 0)
	if !errors.Is(err, ErrUnexpectedKey) {
		t.Errorf("expected ErrUnexpectedKey, got %v", err)
	}

	err = p.parseTermField(key, nil, &protoField{}, false)
	if !errors.Is(err, ErrUnexpectedKey) {
		t.Errorf("expected ErrUnexpectedKey, got %v", err)
	}
}

func genStubChildren(rawMatches []string) []*protoTerm {
	children := make([]*protoTerm, 0, len(rawMatches))
	for i, v := range rawMatches {
		u := uint32(i)
		stub := _genStubChild(2*u, u, i == 0, []string{v})
		children = append(children, &protoTerm{child: stub})
	}
	return children
}

func genStubChild(rawMatches []string) *AstInnerNodeT {
	return _genStubChild(0, 0, true, rawMatches)
}

// _genStubChild is a helper function to generate a stub child node for testing purposes,
// given a list of raw match strings.
func _genStubChild(nodeId, rank uint32, origin bool, rawMatches []string) *AstInnerNodeT {

	parentAddr := &AstNodeAddressT{
		Type:     AstNodeTypeSet,
		RuleId:   stubRuleId,
		RuleHash: stubRuleHash,
		NodeId:   nodeId,
		Rank:     rank,
	}

	leaf := &AstMatchLeafT{
		baseAst: baseAst{
			scope: AstScopeNode,
			address: AstNodeAddressT{
				Type:     AstNodeTypeLogSet,
				RuleId:   stubRuleId,
				RuleHash: stubRuleHash,
				Depth:    1,
				NodeId:   nodeId + 1,
			},
			parent: parentAddr,
		},
		Window: 0,
		Terms:  []AstFieldT{},
		Event: AstEventT{
			Source: stubSource,
			Origin: origin,
		},
	}
	for _, raw := range rawMatches {
		leaf.Terms = append(leaf.Terms, AstFieldT{
			Count: 1,
			TermValue: match.TermT{
				Type:  match.TermRaw,
				Value: raw,
			},
		})
	}

	return &AstInnerNodeT{
		baseAst: baseAst{
			scope:   AstScopeCluster,
			address: *parentAddr,
		},
		Terms: []AstTermT{{Term: leaf}},
	}
}
