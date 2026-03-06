package ast

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/goccy/go-yaml/ast"
)

func TestParseExtracts(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		want      []AstExtractT
		wantErr   error
		wantPos   int
		failJQ    bool
	}{
		{
			name: "valid extract with name and jq",
			yamlInput: `
extract:
  - name: "example"
    jq: ".field"
`,
			strict: true,
			want: []AstExtractT{{
				Name:    "example",
				JqValue: ".field",
			},
			},
		},
		{
			name: "valid extract with name and jq in unusual order",
			yamlInput: `
extract:
  - jq: ".field"
    name: "example"
`,
			strict: true,
			want: []AstExtractT{{
				Name:    "example",
				JqValue: ".field",
			},
			},
		},
		{
			name: "multiple valid extractions",
			yamlInput: `
extract:
  - name: "example"
    regex: ".*"
`,
			strict: true,
			want: []AstExtractT{{
				Name:       "example",
				RegexValue: ".*",
			},
			},
		},
		{
			name: "multiple valid extractions with different types",
			yamlInput: `
extract:
  - name: "example"
    regex: ".*"
  - name: "another_example"
    jq: ".field"
  - name: "yet_another_example"
    regex: "xxx.*xxx"  
`,
			strict: true,
			want: []AstExtractT{
				{
					Name:       "example",
					RegexValue: ".*",
				},
				{
					Name:    "another_example",
					JqValue: ".field",
				},
				{
					Name:       "yet_another_example",
					RegexValue: "xxx.*xxx",
				},
			},
		},
		{
			name: "invalid name",
			yamlInput: `
extract:
  - name: "???"
    jq: ".field"
`,
			wantErr: ErrBadExtractName,
			wantPos: 21, // Position of first '?' in '???'

		},
		{
			name: "empty name",
			yamlInput: `
extract:
  - name: ""
    jq: ".field"
`,
			wantErr: ErrBadExtractName,
			wantPos: 21, // Position of empty string in 'name: ""'

		},
		{
			name: "invalid name type",
			yamlInput: `
extract:
  - name: 123
    jq: ".field"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 21, // Position of first '1' in '123'

		},
		{
			name: "missing name",
			yamlInput: `
extract:
  - jq: ".field"
`,
			wantErr: ErrMissingKey,
			wantPos: 13,
		},
		{
			name: "duplicate extract names",
			yamlInput: `
extract:
  - name: "example"
    jq: ".field"
  - name: "example"
    regex: ".*"
`,
			wantErr: ErrDupeExtractName,
			wantPos: 58,
		},
		{
			name: "bad regex value",
			yamlInput: `
extract:
  - name: "example"
    regex: "[abc"
`,
			wantErr: ErrBadRegex,
			wantPos: 42,
		},
		{
			name: "bad jq value",
			yamlInput: `
extract:
  - name: "example"
    jq: "(.foo"
`,
			wantErr: ErrBadJq,
			wantPos: 39,
			failJQ:  true,
		},
		{
			name: "bad jq value but no validator",
			yamlInput: `
extract:
  - name: "example"
    jq: "(.foo"
`,
			failJQ: false,
		},
		{
			name: "conflicting values",
			yamlInput: `
extract:
  - name: "example"
    jq: "(.foo"
    regex: ".*"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 56,
		},
		{
			name: "conflicting values reversed",
			yamlInput: `
extract:
  - name: "example"
    regex: ".*"
    jq: "(.foo"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 53,
		},
		{
			name: "no extracts defined [strict mode]",
			yamlInput: `
extract: []
`,
			strict:  true,
			wantErr: ErrMissingKey,
			wantPos: 11, // Position of '[' in 'extract: []'
		},
		{
			name: "no extracts defined [non strict mode]",
			yamlInput: `
extract: []
`,
			strict: false,
		},
		{
			name: "bad extracts type",
			yamlInput: `
extract: "not a sequence"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 11, // Position of '"' in 'not a sequence'
		},
		{
			name: "bad extract type",
			yamlInput: `
extract:
- not a mapping
`,
			wantErr: ErrUnexpectedType,
			wantPos: 13, // Position of 'n' in '- not a mapping'
		},
		{
			name: "bad extract key type",
			yamlInput: `
extract:
- 123: "value"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 13, // Position of '1' in '- 123: "value"'
		},
		{
			name: "unexpected key in extract",
			yamlInput: `
extract:
  - name: "example"
    jq: ".field"
    shrubbery: "value"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 52, // Position of 'shrubbery' in 'shrubbery: "value"'
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)
			// Should be mapping Node with single key "extract"
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}
			v := mapping.Values[0]
			p := &parserT{strict: tt.strict, root: node, validateJQ: stubValidator}
			if tt.failJQ {
				p.validateJQ = func(v string) error {
					return fmt.Errorf("invalid jq expression: %s", v)
				}
			}
			got, err := p.parseExtracts(v.Value)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseExtracts() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
