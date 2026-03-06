package ast

import (
	"reflect"
	"testing"
	"time"

	"github.com/goccy/go-yaml/ast"
	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

func TestParseScriptNode(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		failLua   bool
		want      *AstScriptT
		wantErr   error
		wantPos   int
	}{
		{
			name: "valid script with all fields",
			yamlInput: `
script:
  code: "print(\"Hello, world!\")"
  language: "lua"
  timeout: 5m
  input:
    set:
      event:
        source: "stubSource"
        origin: true
      match:
      - "pod"
`,
			want: &AstScriptT{
				baseAst: baseAst{
					scope: AstScopeCluster,
					address: AstNodeAddressT{
						Type:     AstNodeTypeScript,
						RuleId:   stubRuleId,
						RuleHash: stubRuleHash,
					},
				},
				Code:     "print(\"Hello, world!\")",
				Language: scriptLua,
				Timeout:  5 * time.Minute,
				Input:    stubLuaInput([]string{"pod"}),
			},
		},
		{
			name: "bad mapping",
			yamlInput: `
script: "not a mapping"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 10, // Position of the "not a mapping" string node, which is where the type error is detected.
		},
		{
			name: "bad key type",
			yamlInput: `
script:
  123: "some value"
`,
			wantErr: ErrUnexpectedType,
			wantPos: 12, // Position of the "123" integer node, which is where the type error is detected.
		},
		{
			name: "bad script code type",
			yamlInput: `
script:
  code: 123
`,
			wantErr: ErrUnexpectedType,
			wantPos: 18, // Position of the "123" integer node, which is where the type error for script code is detected.
		},
		{
			name: "bad script code",
			yamlInput: `
script:
  code: "bad lua code"
`,
			failLua: true,
			wantErr: ErrBadScriptCode,
			wantPos: 18, // Position of the "bad lua code" string node, which is where the type error for script code is detected.
		},
		{
			name: "bad script code no validator",
			yamlInput: `
script:
  code: "bad lua code"
`,
			failLua: false,
			wantErr: ErrMissingScriptInput, // falls through to this error since code validation succeeds and input is required.
			wantPos: 8,                     // Position of the "script" key node, which is where the missing input error is detected since code validation fails and input is required.
		},
		{
			name: "language wrong type",
			yamlInput: `
script:
  language: 123
`,
			wantErr: ErrUnexpectedType,
			wantPos: 22, // Position of the "123" integer node, which is where the type error for script language is detected.
		},
		{
			name: "empty language string",
			yamlInput: `
script:
  code: "print(\"Hello, world!\")"
  language: ""
`,
			strict:  true,
			wantErr: ErrBadScriptLang,
			wantPos: 57, // Position of the empty string node for language, which is where the bad script language error is detected.
		},
		{
			name: "empty language string not strict",
			yamlInput: `
script:
  code: "print(\"Hello, world!\")"
  language: ""
`,
			strict:  false,
			wantErr: ErrMissingScriptInput, // falls through to this error since language validation succeeds and input is required.
			wantPos: 8,                     // Position of the "script" key node, which is where the missing input error is detected since code validation fails and input is required.
		},
		{
			name: "unknown language",
			yamlInput: `
script:
  code: "print(\"Hello, world!\")"
  language: "unknown"
`,
			wantErr: ErrBadScriptLang,
			wantPos: 57, // Position of the "unknown" string node for language, which is where the bad script language error is detected.
		},
		{
			name: "input bad type",
			yamlInput: `
script:
  input: 123
`,
			wantErr: ErrUnexpectedType,
			wantPos: 19, // Position of the "123" integer node, which is where the type error for script input is detected.
		},
		{
			name: "input bad length",
			yamlInput: `
script:
  input:
    set:
      event:
        source: "stubSource"
        origin: true
      match:
      - "pod"
    two: banana
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 122, // Position of the "two" key node, which is where the unexpected key error for script input is detected since there should only be one term definition.
		},
		{
			name: "input bad type in key node",
			yamlInput: `
script:
  input:
    123: banana
`,
			wantErr: ErrUnexpectedType,
			wantPos: 23, // Position of the "123" integer node, which is where the type error for script input is detected.
		},
		{
			name: "empty input",
			yamlInput: `
script:
  input: {}
`,
			wantErr: ErrMissingScriptInput,
			wantPos: 19, // Position of the empty mapping node for input, which is where the missing script input error is detected.
		},
		{
			name: "input malformed",
			yamlInput: `
script:
  input:
    setx:
      event:
        source: "stubSource"
        origin: true
      match:
      - "pod"
    two: banana
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 23, // Position of the "setx" key node, which is where the unexpected key error for script input is detected since "setx" is not a valid input type.
		},
		{
			name: "unexpected key",
			yamlInput: `
script:
  shrubbery: "nope"
`,
			wantErr: ErrUnexpectedKey,
			wantPos: 12, // Position of the "shrubbery" key node, which is where the unexpected key error for script input is detected.
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yamlInput)
			// Should be mapping Node with single key "script"
			mapping, ok := node.(*ast.MappingNode)
			if !ok || len(mapping.Values) != 1 {
				t.Fatalf("expected a mapping node with one key, got %T with %d keys", node, len(mapping.Values))
			}

			validateLua := stubValidator
			if tt.failLua {
				validateLua = func(code string) error {
					return ErrBadScriptCode
				}
			}

			v := mapping.Values[0]
			p := &parserT{
				strict:      tt.strict,
				root:        node,
				maxDepth:    11,
				maxRank:     11,
				validateLua: validateLua,
			}
			state := newRuleState(&AstMetadataT{Id: stubRuleId, Hash: stubRuleHash})
			got, err := p.parseScriptNode(state, v.Value)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if tt.want != nil && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("parseScriptNode() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func stubLuaInput(rawMatches []string) AstNode {

	var fields []AstFieldT
	for _, raw := range rawMatches {
		fields = append(fields, AstFieldT{
			Count: 1,
			TermValue: match.TermT{
				Type:  match.TermRaw,
				Value: raw,
			},
		})
	}

	return &AstMatchLeafT{
		baseAst: baseAst{
			scope: AstScopeNode,
			address: AstNodeAddressT{
				Type:     AstNodeTypeLogSet,
				RuleId:   stubRuleId,
				RuleHash: stubRuleHash,
				Depth:    1,
				NodeId:   1,
			},
			parent: &AstNodeAddressT{
				Type:     AstNodeTypeScript,
				RuleId:   stubRuleId,
				RuleHash: stubRuleHash,
				Depth:    0,
				NodeId:   0,
			},
		},
		Window: 0,
		Terms:  fields,
		Event: AstEventT{
			Source: stubSource,
			Origin: true,
		},
	}
}
