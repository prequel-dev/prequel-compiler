package ast

import (
	"reflect"
	"testing"

	"github.com/goccy/go-yaml/ast"
)

func TestParseMetaNode(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		want      *AstMetadataT
		wantErr   error
		wantPos   int
	}{
		{
			name: "valid meta with all fields",
			yamlInput: `
metadata:
  name: TestRule
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  gen: 10
  kind: prequel
`,
			strict: true,
			want: &AstMetadataT{
				Name: "TestRule",
				Id:   "eeJwJiWQa9TyH3qTYYSZM9",
				Hash: "9GJSdx4smGJeJCdiw6tiK5",
				Gen:  10,
				Kind: KindPrequel,
			},
		},
		{
			name: "missing id",
			yamlInput: `
metadata:
  hash: 9GJSdx4smGJeJCdiw6tiK5
`,
			strict:  true,
			wantErr: ErrMissingKey,
			wantPos: 10, // Position of the metadata mapping node
		},
		{
			name: "missing hash",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
`,
			strict:  true,
			wantErr: ErrMissingKey,
			wantPos: 10, // Position of the metadata mapping node
		},
		{
			name: "only required id,hash",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
`,
			strict: true,
			want: &AstMetadataT{
				Id:   "eeJwJiWQa9TyH3qTYYSZM9",
				Hash: "9GJSdx4smGJeJCdiw6tiK5",
			},
		},
		{
			name: "unexpected key in strict mode",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  extra: value
`,
			strict:  true,
			wantErr: ErrUnexpectedKey,
			wantPos: 74, // Position of the "extra" key node
		},
		{
			name: "bad node type",
			yamlInput: `
metadata: not_a_mapping
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 12, // Position of the "not_a_mapping" scalar node
		},
		{
			name: "bad key type",
			yamlInput: `
metadata:
  999: not_a_string_key
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 14, // Position of the "999" key node (integer instead of string)
		},
		{
			name: "bad value type for id",
			yamlInput: `
metadata:
  id: 12345
  hash: 9GJSdx4smGJeJCdiw6tiK5
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 18, // Position of the "id" value node (integer instead of string)
		},
		{
			name: "bad id value",
			yamlInput: `
metadata:
  id: $$$
  hash: 9GJSdx4smGJeJCdiw6tiK5
`,
			strict:  true,
			wantErr: ErrBadIdentifier,
			wantPos: 18, // Position of "$$$" id value node
		},
		{
			name: "bad value type for hash",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 999
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 49, // Position of the "999" value node (integer instead of string)
		},
		{
			name: "bad hash value",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: $$$
`,
			strict:  true,
			wantErr: ErrBadHash,
			wantPos: 49, // Position of the "$$$" value node (string instead of valid hash)
		},
		{
			name: "bad gen type",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  gen: "not_a_number"
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 79, // Position of the "not_a_number" value node (string instead of valid number)
		},
		{
			name: "negative gen value",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  gen: -1
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 79, // Position of the "not_a_number" value node (string instead of valid number)
		},
		{
			name: "out of range gen value",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  gen: 11 # We set maxGen to 10 in the test.
`,
			strict:  true,
			wantErr: ErrBadGen,
			wantPos: 79, // Position of the "11" value node (integer instead of valid range)
		},
		{
			name: "bad kind type",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  kind: 123
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 80, // Position of the "123" value node (integer instead of valid kind)
		},
		{
			name: "bad kind value strict mode",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  kind: unknown_kind
`,
			strict:  true,
			wantErr: ErrBadKind,
			wantPos: 80, // Position of the "unknown_kind" value node (string instead of valid kind)
		},
		{
			name: "bad kind value tolerant mode",
			yamlInput: `
metadata:
  id: eeJwJiWQa9TyH3qTYYSZM9
  hash: 9GJSdx4smGJeJCdiw6tiK5
  kind: unknown_kind
`,
			strict: false,
			want: &AstMetadataT{
				Id:   "eeJwJiWQa9TyH3qTYYSZM9",
				Hash: "9GJSdx4smGJeJCdiw6tiK5",
				Kind: "unknown_kind", // Should accept unknown kind in tolerant mode
			},
		},
		{
			name: "no hash or id on minimal parse",
			yamlInput: `
metadata:
  kind: unknown_kind
`,
			wantErr: ErrMissingKey,
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
			p := &parserT{strict: tt.strict, root: node, maxGen: 10}

			got, err := p.parseMetadataNode(v.Value)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseMetaNode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestParseMetaNodeMinimal(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		want      *AstMetadataT
		wantErr   error
		wantPos   int
	}{

		{
			name: "no hash or id on minimal parse",
			yamlInput: `
metadata:
  kind: unknown_kind
`,
			wantErr: ErrMissingKey,
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
			p := &parserT{strict: tt.strict, root: node, maxGen: 10}

			got, err := p.parseMetadataNodeMinimal(v.Value)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseMetaNode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}
