package ast

import (
	"testing"

	"github.com/goccy/go-yaml/ast"
)

func TestParseNode(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		ty        AstNodeType
		strict    bool
		wantErr   error
		wantPos   int
	}{
		{
			name: "bad mapping type",
			ty:   AstNodeTypeSet,
			yamlInput: `
set: not a mapping set
`,
			wantErr: ErrUnexpectedType,
			wantPos: 7, // Position of the 'set' key
		},
		{
			name: "missing window on count with count > 1",
			ty:   AstNodeTypeSet,
			yamlInput: `
set:
  event:
    source: kafka
    origin: true
  match:
    - value: "serenity"
      count: 2
`,
			wantErr: ErrMissingWindow,
			wantPos: 5, // Position of the 'set' key
		},
		{
			name: "bad mapping key",
			ty:   AstNodeTypeSet,
			yamlInput: `
set:
  123: not a string
`,
			wantErr: ErrUnexpectedType,
			wantPos: 9, // Position of the '123' key
		},
		{
			name: "bad correlations",
			ty:   AstNodeTypeSet,
			yamlInput: `
set:
  correlations: not a sequence
`,
			wantErr: ErrUnexpectedType,
			wantPos: 23,
		},
		{
			name: "negative window",
			ty:   AstNodeTypeSet,
			yamlInput: `
set:
  window: -5s
`,
			wantErr: ErrWindowNegative,
			wantPos: 17,
		},
		{
			name: "fail mixed positive and negative field types",
			ty:   AstNodeTypeSet,
			yamlInput: `
set:
  window: 5s
  event:
    source: kafka
    origin: true
  match:
    - value: "serenity"
    - "now"
  negate:
    - set:
        window: 10s
        event:
          source: kafka
        match:
        - "nope"
`,
			wantErr: ErrTermTypeConflict,
			wantPos: 123, // Position of the 'nope' key in the negate term, which is the first negate term and should be highlighted for the error
		},
		{
			name: "disallow event on inner nodes",
			ty:   AstNodeTypeSet,
			yamlInput: `
set:
  window: 5s
  event:
    source: kafka
  match:
    - set:
        event:
          source: kafka
          origin: true
        match:
          - "serenity"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 22, // Position of the 'event' key in the inner set node, which should be highlighted for the error
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)
			// Should be mapping Node with single key kwMetadata
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}
			v := mapping.Values[0]
			p := &parserT{
				strict:   tt.strict,
				root:     node,
				maxGen:   10,
				maxRank:  10,
				maxDepth: 10,
			}
			state := newRuleState(
				&AstMetadataT{Id: "test", Hash: "hash"},
			)

			_, err := p.parseNode(state, tt.ty, v.Value)

			checkParserError(t, err, tt.wantErr, tt.wantPos)
		})
	}
}
