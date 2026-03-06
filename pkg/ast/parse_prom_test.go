package ast

import (
	"reflect"
	"testing"
	"time"

	"github.com/goccy/go-yaml/ast"
)

func TestParsePromNode(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		failQL    bool
		want      *AstPromT
		wantErr   error
		wantPos   int
	}{
		{
			name: "valid promql query",
			yamlInput: `
promql:
  expr: "sum(rate(http_request_duration_seconds_count[5m])) by (job)"
  interval: 1m
  for: 5m
  event:
    source: stubSource
    origin: true
`,
			want: &AstPromT{
				baseAst: baseAst{
					scope: AstScopeCluster,
					address: AstNodeAddressT{
						Type:     AstNodeTypePromQL,
						RuleId:   stubRuleId,
						RuleHash: stubRuleHash,
						Rank:     0,
						Depth:    0,
						NodeId:   0,
					},
				},
				Expr:     "sum(rate(http_request_duration_seconds_count[5m])) by (job)",
				Interval: time.Minute,
				For:      time.Minute * 5,
				Event: &AstEventT{
					Source: stubSource,
					Origin: true,
				},
			},
		},
		{
			name: "bad promql",
			yamlInput: `
promql:
  expr: "bad query"
  interval: 1m
  for: 5m
  event:
    source: stubSource
    origin: true
`,
			failQL:  true,
			wantErr: ErrBadPromQL,
			wantPos: 18, // position of the "expr" value in the YAML input
		},
		{
			name: "bad expr node type",
			yamlInput: `
promql:
  expr: 12345
`,
			wantErr: ErrUnexpectedType,
			wantPos: 18, // position of the "12345" value in the YAML input
		},
		{
			name: "bad mapping key",
			yamlInput: `
promql:
  123: "nope"
`,
			strict:  false,
			wantErr: ErrUnexpectedType,
			wantPos: 12, // position of the "123" key in the YAML input
		},
		{
			name: "unexpected key",
			yamlInput: `
promql:
  shrubbery: "nope"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 12, // position of the "shrubbery" key in the YAML input
		},
		{
			name: "bad mapping type",
			yamlInput: `
promql: "nope"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 10, // position of the "nope" key in the YAML input
		},
		{
			name: "negative interval",
			yamlInput: `
promql:
  interval: -1m
`,
			wantErr: ErrUnexpectedType,
			wantPos: 22, // position of the "-1m" value in the YAML input
		},
		{
			name: "zero interval",
			yamlInput: `
promql:
  interval: 0m
`,
			wantErr: ErrUnexpectedType,
			wantPos: 22, // position of the "0m" value in the YAML input
		},
		{
			name: "empty promql expression",
			yamlInput: `
promql:
  expr: ""
`,
			wantErr: ErrBadPromQL,
			wantPos: 18, // position of the "expr" value in the YAML input
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)
			// Should be mapping Node with single key "promql"
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}
			validatePromQL := stubValidator
			if tt.failQL {
				validatePromQL = func(query string) error {
					return ErrBadPromQL
				}
			}
			v := mapping.Values[0]
			p := &parserT{
				strict:         tt.strict,
				root:           node,
				validatePromQL: validatePromQL,
			}
			state := newRuleState(&AstMetadataT{Id: stubRuleId, Hash: stubRuleHash})
			got, err := p.parsePromQLNode(state, v.Value)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parsePromNode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
