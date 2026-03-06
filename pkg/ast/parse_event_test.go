package ast

import (
	"reflect"
	"testing"

	"github.com/goccy/go-yaml/ast"
)

func TestParseEventNode(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		want      *AstEventT
		wantErr   error
		wantPos   int
	}{
		{
			name: "valid event with both fields",
			yamlInput: `
event:
  source: "syslog"
  origin: true
`,
			strict: true,
			want: &AstEventT{
				Source: "syslog",
				Origin: true,
			},
		},
		{
			name: "valid event with only source",
			yamlInput: `
event:
  source: "auditd"
`,
			strict: true,
			want: &AstEventT{
				Source: "auditd",
				Origin: false,
			},
		},
		{
			name: "invalid event missing source",
			yamlInput: `
event:
  origin: true
`,
			strict:  true,
			wantErr: ErrMissingSource,
			wantPos: 7,
		},
		{
			name: "invalid type for source",
			yamlInput: `
event:
  source: 123
  origin: true
`,
			strict:  true,
			want:    nil,
			wantErr: ErrUnexpectedType,
			wantPos: 19, // Pos of '1' in '123'
		},
		{
			name: "invalid type for origin",
			yamlInput: `
event:
  source: "syslog"
  origin: "yes"
`,
			strict:  true,
			want:    nil,
			wantErr: ErrUnexpectedType,
			wantPos: 38, // Pos of '"' in '"yes"'
		},
		{
			name: "unexpected key in strict mode",
			yamlInput: `
event:
  source: "syslog"
  origin: true
  extra: "field"
`,
			strict:  true,
			want:    nil,
			wantErr: ErrUnexpectedKey,
			wantPos: 45, // Pos of ':' in 'extra:'
		},
		{
			name: "unexpected key in non-strict mode",
			yamlInput: `
event:
  source: "syslog"
  origin: true
  extra: "field"
`,
			strict: false,
			want: &AstEventT{
				Source: "syslog",
				Origin: true,
			},
		},
		{
			name: "unexpected mapping type for event",
			yamlInput: `
event: wrongtype
`,
			strict:  false,
			wantErr: ErrUnexpectedType,
			wantPos: 9, // Pos of 'w' in 'wrongtype'
		},
		{
			name: "unexpected key type",
			yamlInput: `
event:
  123: "syslog"
`,
			strict:  false,
			wantErr: ErrUnexpectedType,
			wantPos: 11, // Pos of '1' in '123'
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)
			// Should be mapping Node with single key "event"
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}
			v := mapping.Values[0]
			p := &parserT{strict: tt.strict, root: node}
			state := newRuleState(&AstMetadataT{Id: "test", Hash: "hash"})
			got, err := p.parseEventNode(state, v.Value)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseEventNode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
