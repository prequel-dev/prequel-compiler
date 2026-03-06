package ast

import (
	"fmt"
	"testing"

	"github.com/goccy/go-yaml/ast"
	"github.com/prequel-dev/prequel-compiler/pkg/version"
)

type wantErrT struct {
	err  error
	pos  int
	hash string
	rule string
}

func TestParseRulesNode(t *testing.T) {
	tests := []struct {
		name      string
		yamlInput string
		strict    bool
		want      [][]string
		wantErr   []wantErrT
	}{
		{
			name: "empty rules sequence",
			yamlInput: `
rules: []
    `,
		},
		{
			name: "bad compiler type",
			yamlInput: `
rules:
  - compiler: 999
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 23},
			},
		},
		{
			name: "empty compiler strict",
			yamlInput: `
rules:
  - compiler: ""
    `,
			strict: true,
			wantErr: []wantErrT{
				{err: ErrMissingVersion, pos: 23},
			},
		},
		{
			name: "empty compiler non-strict",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBeS
      hash: rdJLgqYgkEp8jg8Qks1qiq
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
    compiler: ""
    `,
			strict: false,
			want: [][]string{
				{
					"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
					"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				},
			},
		},
		{
			name: "compiler version specified but invalid",
			yamlInput: `
rules:
  - compiler: "whoops i did it again"
    `,
			strict: true,
			wantErr: []wantErrT{
				{err: version.ErrBadConstraint, pos: 23},
			},
		},
		{
			name: "compiler version specified but invalid",
			yamlInput: fmt.Sprintf(`
rules:
  - compiler: "%s"
    metadata:
      id: "K7uRQTGpGMyL1iFpssnBeS"
      hash: "sdJLgqYgkEp8jg8Qks1qiq"
      999: ignored bad key in metadata
      unknown_key: also ignored in metadata
    `, fmt.Sprintf("<%s", version.SemVer())),
			strict: true,
			wantErr: []wantErrT{
				{
					err:  version.ErrVersionNotAllowed{Version: version.SemVer(), Expression: fmt.Sprintf("<%s", version.SemVer())},
					pos:  23,
					hash: "sdJLgqYgkEp8jg8Qks1qiq",
					rule: "K7uRQTGpGMyL1iFpssnBeS",
				},
			},
		},
		{
			name: "mismatch compiler version with minimal metadata",
			yamlInput: fmt.Sprintf(`
rules:
  - compiler: "%s"
    `, fmt.Sprintf("<%s", version.SemVer())),
			strict: true,
			wantErr: []wantErrT{
				{err: version.ErrVersionNotAllowed{Version: version.SemVer(), Expression: fmt.Sprintf("<%s", version.SemVer())}, pos: 23},
			},
		},
		{
			name: "compiler version specified exact",
			yamlInput: fmt.Sprintf(`
rules:
  - compiler: "%s"
    `, fmt.Sprintf("=%s", version.SemVer())),
			strict: true,
			wantErr: []wantErrT{
				{err: ErrMissingKey, pos: 11}, // fall through to missing metadata
			},
		},
		{
			name: "compiler version specified greater or equal too",
			yamlInput: fmt.Sprintf(`
rules:
  - compiler: "%s"
    `, fmt.Sprintf(">= %s", version.SemVer())),
			strict: true,
			wantErr: []wantErrT{
				{err: ErrMissingKey, pos: 11}, // fall through to missing metadata
			},
		},
		{
			name: "bad rules type",
			yamlInput: `
rules: {}
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 9}, // Position of the rules key node
			},
		},
		{
			name: "bad rule type",
			yamlInput: `
rules:
  - not a mapping
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 13}, // Position of the first rule node
			},
		},
		{
			name: "bad key type in rule mapping",
			yamlInput: `
rules:
  - 123: not a string key
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 13}, // Position of the first rule node
			},
		},
		{
			name: "unexpected key in rule mapping",
			yamlInput: `
rules:
  - unexpected_key: value
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedKey, pos: 13}, // Position of the unexpected key node
			},
		},
		{
			name: "missing meta",
			yamlInput: `
rules:
  - cre:
     id: TestSuccessSimpleRule1
     severity: 1
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
        `,
			wantErr: []wantErrT{
				{err: ErrMissingKey, pos: 11}, // Position of the unexpected key node
			},
		},
		{
			name: "missing root",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    `,
			wantErr: []wantErrT{
				{err: ErrMissingKey,
					pos:  11,
					hash: "rdJLgqYgkEp8jg8Qks1qiq",
					rule: "J7uRQTGpGMyL1iFpssnBeS",
				},
			},
		},
		{
			name: "first rule, ok second rule bad type",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBeS
      hash: rdJLgqYgkEp8jg8Qks1qiq
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
  - not a mapping
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 319}, // Position of the second rule node
			},
			want: [][]string{
				{
					"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
					"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				},
			},
		},
		{
			name: "first rule bad type, second rule ok",
			yamlInput: `
rules:
  - not a mapping
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBeS
      hash: rdJLgqYgkEp8jg8Qks1qiq
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
`,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 13}, // Position of the first rule node
			},
			want: [][]string{
				{
					"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
					"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				},
			},
		},
		{
			name: "two ok followed by a illegal metadata in strict mode",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBe1
      hash: rdJLgqYgkEp8jg8Qks1qi3
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBe2
      hash: rdJLgqYgkEp8jg8Qks1qis
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBe3
      hash: rdJLgqYgkEp8jg8Qks1qir
      nope: not allowed key in strict mode
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
`,
			wantErr: []wantErrT{
				{err: ErrUnexpectedKey, pos: 769}, // Position of bad metadata key
			},
			want: [][]string{
				{
					"v1.machine_set.rdJLgqYgkEp8jg8Qks1qi3.d0.n0.t0",
					"v1.log_set.rdJLgqYgkEp8jg8Qks1qi3.d1.n1.t0",
				},
				{
					"v1.machine_set.rdJLgqYgkEp8jg8Qks1qis.d0.n0.t0",
					"v1.log_set.rdJLgqYgkEp8jg8Qks1qis.d1.n1.t0",
				},
			},
			strict: true,
		},
		{
			name: "single origin",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: J7uRQTGpGMyL1iFpssnBeS
      hash: rdJLgqYgkEp8jg8Qks1qiq
    rule:
      set:
        event:
          source: kafka
          origin: true
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
`,
			want: [][]string{
				{
					"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
					"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				},
			},
		},
		{
			name: "no origin",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule:
      set:
        event:
          source: kafka
        match:
          - value: "io.vertx.core.VertxException: Thread blocked"
`,
			wantErr: []wantErrT{
				{err: ErrMissingOrigin, pos: 163},
			},
			strict: true,
		},
		{
			name: "multiple origin",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule:
      set:
        match:
        - set:
            event:
              source: kafka
              origin: true
            match:
              - value: "io.vertx.core.VertxException: Thread blocked"
        - set:
            event:
              source: shrubbery
              origin: true
            match:
              - value: "io.vertx.core.VertxException: Thread blocked"
`,
			wantErr: []wantErrT{
				{err: ErrMultipleOrigin, pos: 457}, // Position of the second origin node``
			},
		},
		{
			name: "conflicting set and seq nodes",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule:
      set:
        window: 10s
        event:
          source: kafka
          origin: true
        match:
        - "shrubbery"
      sequence:
        window: 10s
        event:
          source: kafka
          origin: true
        order:
        - "shrubbery"
        - "trees"
   `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedKey, pos: 301}, // Position of the conflicting key node
			},
		},
		{
			name: "conflicting set and seq nodes",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule:
      sequence:
        window: 10s
        event:
          source: kafka
          origin: true
        order:
        - "shrubbery"
        - "trees"
      set:
        window: 10s
        event:
          source: kafka
          origin: true
        match:
        - "shrubbery"
   `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedKey, pos: 324}, // Position of the conflicting key node
			},
		},
		{
			name: "missing set and seq nodes",
			yamlInput: `
rules:
  - cre:
      id: TestSuccessSimpleRule1
      severity: 1
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    rule: {}
   `,
			wantErr: []wantErrT{
				{err: ErrMissingKey, pos: 165},
			},
		},
		{
			name: "bad type in rule node",
			yamlInput: `
rules:
  - rule: not a mapping
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 19}, // Position of the first rule node
			},
		},
		{
			name: "bad key type in rule node",
			yamlInput: `
rules:
  - rule:
      123: not a string key
    metadata:
      id: "J7uRQTGpGMyL1iFpssnBeS"
      hash: "rdJLgqYgkEp8jg8Qks1qiq"
    `,
			wantErr: []wantErrT{
				{err: ErrUnexpectedType, pos: 25},
			},
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
				maxDepth: 11,
				maxRank:  11,
			}

			got, err := p.parseRulesNode(v.Value)

			checkParserErrors(t, err, tt.wantErr)

			if len(got) != len(tt.want) {
				t.Fatalf("expected %d rules, got %d", len(tt.want), len(got))
			}

			for i, tree := range tt.want {
				compareTree(t, got[i], tree)
			}

		})
	}
}

func checkParserErrors(t *testing.T, err error, wantErrs []wantErrT) {
	t.Helper()

	switch {
	case len(wantErrs) == 0 && err == nil:
		return // No errors expected and got none, all good.
	case len(wantErrs) == 0 && err != nil:
		t.Fatalf("unexpected error: %v", err)
	case len(wantErrs) > 0 && err == nil:
		t.Fatalf("expected errors but got none")

	}

	type unwrapper interface {
		Unwrap() []error
	}

	if uw, ok := err.(unwrapper); ok {
		errs := uw.Unwrap()
		if len(errs) != len(wantErrs) {
			t.Fatalf("expected %d errors, got %d", len(wantErrs), len(errs))
		}
		for i, want := range wantErrs {
			checkParserError(t, errs[i], want.err, want.pos)
			if want.hash != "" || want.rule != "" {
				pe, ok := errs[i].(ErrRule)
				if !ok {
					t.Fatalf("expected error to be *ErrRule, got %T", errs[i])
				}
				if want.hash != "" && pe.Meta.Hash != want.hash {
					t.Errorf("error %d: expected hash %q, got %q", i, want.hash, pe.Meta.Hash)
				}
				if want.rule != "" && pe.Meta.Id != want.rule {
					t.Errorf("error %d: expected rule %q, got %q", i, want.rule, pe.Meta.Id)
				}
			}
		}
	} else if len(wantErrs) == 1 {
		checkParserError(t, err, wantErrs[0].err, wantErrs[0].pos)
	} else {
		t.Fatalf("expected multiple errors but got a single error: %v", err)
	}
}
