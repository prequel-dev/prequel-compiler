package ast

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/prequel-dev/prequel-compiler/pkg/testdata"
)

func TestAstSuccess(t *testing.T) {

	tests := []struct {
		name      string
		yaml      string
		wantOrder []string
	}{
		{
			name: "Success_Simple1",
			yaml: testdata.TestSuccessSimpleRule1,
			wantOrder: []string{
				"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
			},
		},
		{
			name: "Success_Complex2",
			yaml: testdata.TestSuccessComplexRule2,
			wantOrder: []string{
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n2.t1",
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d1.n3.t2",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d2.n4.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d2.n5.t1",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d2.n6.t2",
			},
		},
		{
			name: "Success_Complex3",
			yaml: testdata.TestSuccessComplexRule3,
			wantOrder: []string{
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n2.t1",
			},
		},
		{
			name: "Success_Complex4",
			yaml: testdata.TestSuccessComplexRule4,
			wantOrder: []string{
				"v1.machine_seq.2KdXQZDAfRbYcH9FBDteBS.d0.n0.t0",
				"v1.log_seq.2KdXQZDAfRbYcH9FBDteBS.d1.n1.t0",
				"v1.machine_seq.2KdXQZDAfRbYcH9FBDteBS.d1.n2.t1",
				"v1.log_seq.2KdXQZDAfRbYcH9FBDteBS.d2.n3.t0",
				"v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n4.t1",
				"v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n5.t2",
				"v1.machine_seq.2KdXQZDAfRbYcH9FBDteBS.d1.n6.t2",
				"v1.log_seq.2KdXQZDAfRbYcH9FBDteBS.d2.n7.t0",
				"v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n8.t1",
				"v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n9.t2",
				"v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d1.n10.t3",
			},
		},
		{
			name: "Success_NegateOptions1",
			yaml: testdata.TestSuccessNegateOptions1,
			wantOrder: []string{
				"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
			},
		},
		{
			name: "Success_NegateOptions2",
			yaml: testdata.TestSuccessNegateOptions2,
			wantOrder: []string{
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n2.t1",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n3.t2",
			},
		},
		{
			name: "Success_Extract1",
			yaml: testdata.TestSuccessSimpleExtraction,
			wantOrder: []string{
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
			},
		},
		{
			name: "Success_PromQL",
			yaml: testdata.TestSuccessSimplePromQL,
			wantOrder: []string{
				"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.promql.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n2.t1",
			},
		},
		{
			name: "Success_ChildScript",
			yaml: testdata.TestSuccessChildScript,
			wantOrder: []string{
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.script.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d2.n2.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n3.t1",
			},
		},
		{
			name: "Success_ChildScriptMultipleInputs",
			yaml: testdata.TestSuccessChildScriptMultipleInputs,
			wantOrder: []string{
				"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.script.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.machine_seq.rdJLgqYgkEp8jg8Qks1qiq.d2.n2.t0",
				"v1.log_seq.rdJLgqYgkEp8jg8Qks1qiq.d3.n3.t0",
				"v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d3.n4.t1",
			},
		},
		{
			name: "Success_ChildScriptPromQLInput",
			yaml: testdata.TestSuccessChildScriptPromQLInput,
			wantOrder: []string{
				"v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0",
				"v1.script.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0",
				"v1.promql.rdJLgqYgkEp8jg8Qks1qiq.d2.n2.t0",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			rules, err := ParseRules([]byte(tt.yaml), WithStrict(true))

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(rules) != 1 {
				t.Fatalf("expected 1 rule, got %d", len(rules))
			}

			compareTree(t, rules[0], tt.wantOrder)
		})
	}
}

func TestAstFail(t *testing.T) {
	tests := []struct {
		name     string
		yaml     string
		strict   bool
		maxRank  uint32
		maxDepth uint32
		wantErr  error
		wantPos  int
	}{
		{
			name:    "Fail_MissingPositiveCondition",
			yaml:    testdata.TestFailMissingPositiveCondition,
			wantErr: ErrMissingTerm,
			wantPos: 607,
		},
		{
			name:    "Fail_NegativeCondition1",
			yaml:    testdata.TestFailNegativeCondition1,
			wantErr: ErrMissingTerm,
			wantPos: 635,
		},
		{
			name:    "Fail_NegativeCondition2",
			yaml:    testdata.TestFailNegativeCondition2,
			wantErr: ErrMissingTerm,
			wantPos: 635,
		},
		{
			name:    "Fail_NegativeCondition2_Strict",
			yaml:    testdata.TestFailNegativeCondition2,
			wantErr: ErrUnexpectedKey, // 'imageUrl'
			strict:  true,
			wantPos: 420,
		},
		{
			name:    "Fail_NegativeCondition3",
			yaml:    testdata.TestFailNegateOptions3,
			wantErr: ErrMissingTerm,
			wantPos: 749,
		},
		{
			name:    "Fail_NegativeCondition4",
			yaml:    testdata.TestFailNegateOptions4,
			wantErr: ErrMissingTerm,
			wantPos: 765,
		},
		{
			name:    "Fail_TermsSyntaxError1",
			yaml:    testdata.TestFailTermsSyntaxError1,
			wantErr: ErrUnexpectedKey,
			wantPos: 664,
		},
		{
			name:    "Fail_TermsSyntaxError2",
			yaml:    testdata.TestFailTermsSyntaxError2,
			wantErr: ErrUnexpectedType,
			wantPos: 672,
		},
		{
			name:    "Fail_TermsSemanticError1",
			yaml:    testdata.TestFailTermsSemanticError1,
			wantErr: ErrShortSequence,
			wantPos: 697,
		},
		{
			name:    "Fail_TermsSemanticError2",
			yaml:    testdata.TestFailTermsSemanticError2,
			wantErr: ErrMissingEvent,
			wantPos: 199,
		},
		{
			name:    "Fail_TermsSemanticError3",
			yaml:    testdata.TestFailTermsSemanticError3,
			wantErr: ErrMissingOrigin,
			wantPos: 183,
		},
		{
			name:    "Fail_TermsSemanticError4",
			yaml:    testdata.TestFailTermsSemanticError4,
			wantErr: ErrUnexpectedType,
			wantPos: 314,
		},
		{
			name:    "Fail_TermsSemanticError5",
			yaml:    testdata.TestFailTermsSemanticError5,
			wantErr: ErrBadAnchor,
			wantPos: 411,
		},
		{
			name:    "Fail_TermsSemanticError6",
			yaml:    testdata.TestFailTermsSemanticError6,
			strict:  true,
			wantErr: ErrMissingOrigin,
			wantPos: 183,
		},
		{
			name:   "Fail_TermsSemanticError6_NonStrict",
			yaml:   testdata.TestFailTermsSemanticError6,
			strict: false, // Normally fails in strict mode due to origin requirement, but should fail with missing origin error in non-strict mode.
		},
		{
			name:    "Fail_MultipleOrigin",
			yaml:    testdata.TestFailMultipleOrigin,
			wantErr: ErrMultipleOrigin,
			wantPos: 502,
		},
		{
			name:    "Fail_Typo",
			yaml:    testdata.TestFailTypo,
			wantErr: ErrUnexpectedKey,
			wantPos: 290,
		},
		{
			name:    "Fail_MissingOrder",
			yaml:    testdata.TestFailMissingOrder,
			wantErr: ErrUnexpectedKey,
			wantPos: 279,
		},
		{
			name:    "Fail_MissingMatch",
			yaml:    testdata.TestFailMissingMatch,
			wantErr: ErrUnexpectedKey,
			wantPos: 274,
		},
		{
			name:    "Fail_InvalidWindow",
			yaml:    testdata.TestFailInvalidWindow,
			wantErr: ErrUnexpectedType,
			wantPos: 224,
		},
		{
			name:    "Fail_UnsupportedRule",
			yaml:    testdata.TestFailUnsupportedRule,
			wantErr: ErrUnexpectedKey,
			wantPos: 203,
		},
		{
			name:    "Fail_MissingCreId",
			yaml:    testdata.TestFailMissingCreRule,
			wantErr: ErrMissingKey,
			wantPos: 36,
		},
		{
			name:    "Fail_MissingRuleId",
			yaml:    testdata.TestFailMissingRuleIdRule,
			wantErr: ErrMissingKey,
			wantPos: 100,
		},
		{
			name:    "Fail_MissingRuleHash",
			yaml:    testdata.TestFailMissingRuleHashRule,
			wantErr: ErrMissingKey,
			wantPos: 102,
		},
		{
			name:    "Fail_BadRuleId",
			yaml:    testdata.TestFailBadRuleIdRule,
			wantErr: ErrBadIdentifier,
			wantPos: 108,
		},
		{
			name:    "Fail_BadCreId",
			yaml:    testdata.TestFailBadCreIdRule,
			wantErr: ErrBadIdentifier,
			wantPos: 48,
		},
		{
			name:    "Fail_BadRuleHash",
			yaml:    testdata.TestFailBadRuleHashRule,
			wantErr: ErrBadHash,
			wantPos: 147,
		},
		{
			name:    "Fail_ScriptRoot",
			yaml:    testdata.TestFailScriptRoot,
			wantErr: ErrUnexpectedKey,
			wantPos: 162,
		},
		{
			name:    "Fail_ScriptNoInput",
			yaml:    testdata.TestFailScriptNoInput,
			wantErr: ErrMissingScriptInput,
			wantPos: 203,
		},
		{
			name:    "Fail_MissingWindow",
			yaml:    testdata.TestFailMissingWindow,
			wantErr: ErrMissingWindow,
			wantPos: 151,
		},
		{
			name:     "Fail_MaxDepthExceeded",
			yaml:     testdata.TestFailMaxDepthExceeded,
			maxDepth: 2,
			wantErr:  ErrMaxDepthExceeded,
			wantPos:  234, // Position of the node that exceeds the max depth
		},
		{
			name:    "Fail_MaxRankExceeded",
			yaml:    testdata.TestFailMaxRankExceeded,
			maxRank: 3,
			wantErr: ErrMaxRankExceeded,
			wantPos: 322, // Position of the node that exceeds the max rank
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			opts := []ParseOpt{WithStrict(tt.strict)}
			if tt.maxDepth > 0 {
				opts = append(opts, WithMaxDepth(tt.maxDepth))
			}
			if tt.maxRank > 0 {
				opts = append(opts, WithMaxRank(tt.maxRank))
			}

			rules, err := ParseRules([]byte(tt.yaml), opts...)

			ok := checkParserError(t, err, tt.wantErr, tt.wantPos)

			switch {
			case ok && rules == nil:
				t.Errorf("expected rules to be returned, got nil")
			case !ok && rules != nil:
				t.Errorf("expected no rules to be returned, got %v", rules)
			}
		})
	}
}

// Validate the following invariants on the tree:
// 1. No duplicate addresses
// 2. Root node has no parent address
// 3. Node ids are unique
// 4. Depth is consistent with distance from root

func extractOrder(t *testing.T, rule AstRuleT) []string {
	t.Helper()

	var (
		order    []string
		dupeIds  = make(map[uint32]struct{})
		dupeAddr = make(map[string]struct{})
	)

	err := rule.Walk(func(node AstNode, _ *AstNegateOptsT) error {
		var (
			addr    = node.Address()
			addrStr = addr.String()
		)

		order = append(order, addrStr)

		if _, exists := dupeIds[addr.NodeId]; exists {
			t.Fatalf("Duplicate node ID found: %d", addr.NodeId)
		}
		dupeIds[addr.NodeId] = struct{}{}

		if _, exists := dupeAddr[addrStr]; exists {
			t.Fatalf("Duplicate address found: %s", addrStr)
		}
		dupeAddr[addrStr] = struct{}{}

		// Depth should be one larger than parent's depth;
		// or can rewinde if going back up the tree
		if parentAddr := node.Parent(); parentAddr == nil {
			if addr.Depth != 0 {
				t.Fatalf("Root node has non-zero depth: %d", addr.Depth)
			}
		} else if addr.Depth != parentAddr.Depth+1 {
			t.Fatalf("Node depth inconsistent with parent's depth: %d (parent: %d)", addr.Depth, parentAddr.Depth)
		}

		return nil
	})

	if err != nil {
		t.Fatalf("Walk failed: %v", err)
	}

	return order
}

func TestSuccessExamples(t *testing.T) {

	rules, err := filepath.Glob(filepath.Join("../testdata", "success_examples", "*.yaml"))
	if err != nil {
		t.Fatalf("Error finding CRE test files: %v", err)
	}

	for _, rule := range rules {

		t.Run(filepath.Base(rule), func(t *testing.T) {
			testData, err := os.ReadFile(rule)
			if err != nil {
				t.Fatalf("Error reading test file %s: %v", rule, err)
			}

			_, err = ParseRules(testData, WithStrict(true))
			if err != nil {
				t.Fatalf("Error building rule %s: %v", rule, err)
			}
		})
	}
}

// Test that multiple documents in a single file are handled correctly. In this case, we just want to make sure that the second document is ignored and does not cause an error.
func TestMultipleDocuments(t *testing.T) {

	docs := []string{
		testdata.TestSuccessSimpleRule1,
		testdata.TestSuccessSimpleExtraction,
		testdata.TestSuccessSimplePromQL,
	}

	input := strings.Join(docs, "\n---\n")

	rules, err := ParseRules([]byte(input), WithStrict(true))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(rules) != 3 {
		t.Fatalf("expected 3 rules, got %d", len(rules))
	}

}

func TestFailureExamples(t *testing.T) {

	rules, err := filepath.Glob(filepath.Join("../testdata", "failure_examples", "*.yaml"))
	if err != nil {
		t.Fatalf("Error finding CRE test files: %v", err)
	}

	for _, rule := range rules {

		t.Run(filepath.Base(rule), func(t *testing.T) {

			testData, err := os.ReadFile(rule)
			if err != nil {
				t.Fatalf("Error reading test file %s: %v", rule, err)
			}

			_, err = ParseRules(testData, WithStrict(true))

			if err == nil {
				t.Fatalf("expected failure, got nil")
			}

		})
	}
}

func TestParseEdgeCases(t *testing.T) {

	tests := []struct {
		name    string
		yaml    string
		strict  bool
		wantErr error
		wantPos int
	}{
		{
			name:    "Fail_NotAMapping",
			yaml:    `- not a mapping`,
			wantErr: ErrUnexpectedType,
			wantPos: 1, // Position of the '99' key
		},
		{
			name:    "Fail_MappingKeyType",
			yaml:    `11: wrong key type`,
			wantErr: ErrUnexpectedType,
			wantPos: 1, // Position of the '11' key
		},
		{
			name: "Fail_BadKeyType",
			yaml: `rules:
  99: wrong key type
`,
			wantErr: ErrUnexpectedType,
			wantPos: 12, // Position of the '99' key
		},
		{
			name:    "Fail_UnknownKey",
			yaml:    `nope: unknown key name`,
			wantErr: ErrUnexpectedKey,
			wantPos: 1, // Position of the 'nope' key
		},
		{
			name:    "Fail_NoRules",
			yaml:    `section: rules`,
			wantErr: ErrMissingKey,
			wantPos: 8, // Position of the 'section' key
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			rules, err := ParseRules([]byte(tt.yaml), WithStrict(tt.strict))
			ok := checkParserError(t, err, tt.wantErr, tt.wantPos)

			switch {
			case ok && rules == nil:
				t.Errorf("expected rules to be returned, got nil")
			case !ok && rules != nil:
				t.Errorf("expected no rules to be returned, got %v", rules)
			}
		})
	}

}

func TestLegacySectionKeywordAtRoot(t *testing.T) {

	prependSection := func(t *testing.T, sectionName, in string) string {
		t.Helper()
		return fmt.Sprintf("%s: %s\n%s", kwSection, sectionName, in)
	}

	tests := []struct {
		name     string
		yaml     string
		strict   bool
		wantErr  error
		wantPos  int
		wantSkip bool
	}{
		{
			name: "Success",
			yaml: prependSection(t, kwSectionRules, testdata.TestSuccessSimpleRule1),
		},
		{
			name:    "Fail strict",
			yaml:    prependSection(t, kwSectionRules, testdata.TestSuccessSimpleRule1),
			strict:  true,
			wantErr: ErrUnexpectedKey,
		},
		{
			name:    "Bad value type",
			yaml:    prependSection(t, "99", testdata.TestSuccessSimpleRule1),
			wantErr: ErrUnexpectedType,
		},
		{
			name:     "Ignore unrecognized section",
			yaml:     prependSection(t, "unrecognized_section", testdata.TestSuccessSimpleRule1),
			strict:   false,
			wantSkip: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			rules, err := ParseRules([]byte(tt.yaml), WithStrict(tt.strict))
			ok := checkParserError(t, err, tt.wantErr, tt.wantPos)

			switch {
			case tt.wantSkip && rules == nil:
			case ok && rules == nil:
				t.Errorf("expected rules to be returned, got nil")
			case !ok && rules != nil:
				t.Errorf("expected no rules to be returned, got %v", rules)
			}
		})
	}
}

// Return true if in a non error state
func checkParserError(t *testing.T, err error, wantErr error, wantPos int) bool {
	t.Helper()

	var (
		perr    ParseError
		hasPerr = errors.As(err, &perr)
	)

	if !errors.Is(err, wantErr) {
		t.Errorf("expected error '%v', got '%v'", wantErr, err)
		if hasPerr {
			t.Log(perr.Format(false, true))
		}
	}

	if wantErr == nil {
		return true
	}

	switch {
	case wantPos <= 0:
	case !hasPerr:
		t.Errorf("expected a parser error, got '%v'", err)
	case perr.Offset() != wantPos:
		t.Errorf("expected error at position %d, got %d", wantPos, perr.Offset())
		t.Log(perr.Format(false, true))
	}

	return false
}

func compareTree(t *testing.T, rule AstRuleT, wantOrder []string) {
	t.Helper()
	order := extractOrder(t, rule)

	minLen := min(len(order), len(wantOrder))

	// Compare shared range.
	for i := range minLen {
		if order[i] != wantOrder[i] {
			fmt.Println(Draw(rule, WithColor()))
			t.Fatalf(
				"first mismatch at position %d: expected '%s' != '%s'",
				i,
				wantOrder[i],
				order[i],
			)
		}
	}

	// Handle length mismatch.
	if len(order) != len(wantOrder) {
		t.Fatalf(
			"expected order  sz %v, got %v",
			len(wantOrder),
			len(order),
		)
	}

}
