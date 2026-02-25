package parser

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/prequel-dev/prequel-compiler/pkg/pqerr"
	"github.com/prequel-dev/prequel-compiler/pkg/testdata"
	"github.com/rs/zerolog/log"
)

// traverses the tree and collects node types in DFS pre-order (root, then children)
func gatherNodeTypes(node any, out *[]string) {
	if node == nil {
		return
	}

	if n, ok := node.(*NodeT); ok {
		*out = append(*out, n.Metadata.Type.String())
		for _, child := range n.Children {
			gatherNodeTypes(child, out)
		}
	}
}

// traverses the tree and collects node negative indexes in DFS pre-order (root, then children)
func gatherNodeNegativeIndexes(node any, out *[]int) {
	if node == nil {
		return
	}

	if n, ok := node.(*NodeT); ok {
		*out = append(*out, n.NegIdx)
		for _, child := range n.Children {
			gatherNodeNegativeIndexes(child, out)
		}
	}
}

func TestParseSuccess(t *testing.T) {

	var opts = []ParseOptT{WithGenIds()}

	var tests = map[string]struct {
		rule               string
		expectedNodeTypes  []string
		expectedNegIndexes []int
	}{
		"Success_Simple1": {
			rule:               testdata.TestSuccessSimpleRule1,
			expectedNodeTypes:  []string{"log_seq"},
			expectedNegIndexes: []int{-1},
		},
		"Success_Complex2": {
			rule:               testdata.TestSuccessComplexRule2,
			expectedNodeTypes:  []string{"machine_seq", "log_seq", "log_set", "machine_seq", "log_seq", "log_set", "log_set"},
			expectedNegIndexes: []int{-1, 2, 2, -1, -1, -1, -1},
		},
		"Success_MissingRuleId": {
			rule:               testdata.TestFailMissingRuleIdRule,
			expectedNodeTypes:  []string{"log_set"},
			expectedNegIndexes: []int{-1},
		},
		"Success_MissingRuleHash": {
			rule:               testdata.TestFailMissingRuleHashRule,
			expectedNodeTypes:  []string{"log_set"},
			expectedNegIndexes: []int{-1},
		},
		"Success_PromQL": {
			rule:               testdata.TestSuccessSimplePromQL,
			expectedNodeTypes:  []string{"machine_set", "promql", "log_set"},
			expectedNegIndexes: []int{-1, -1, -1},
		},
		"Success_ChildScript": {
			rule:               testdata.TestSuccessChildScript,
			expectedNodeTypes:  []string{"machine_seq", "script", "log_seq", "log_set"},
			expectedNegIndexes: []int{-1, -1, -1, -1},
		},
		"Success_ChildScriptMultipleInputs": {
			rule:               testdata.TestSuccessChildScriptMultipleInputs,
			expectedNodeTypes:  []string{"machine_set", "script", "machine_seq", "log_seq", "log_set"},
			expectedNegIndexes: []int{-1, -1, -1, -1, -1},
		},
		"Success_ChildScriptPromQLInput": {
			rule:               testdata.TestSuccessChildScriptPromQLInput,
			expectedNodeTypes:  []string{"machine_set", "script", "promql"},
			expectedNegIndexes: []int{-1, -1, -1},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tree, err := Parse([]byte(test.rule), opts...)
			if err != nil {
				t.Fatalf("Error parsing rule: %v", err)
			}

			if len(tree.Nodes) != 1 {
				t.Fatalf("Expected 1 root node, got %d", len(tree.Nodes))
			}

			var actualNodes []string
			gatherNodeTypes(tree.Nodes[0], &actualNodes)

			if !reflect.DeepEqual(actualNodes, test.expectedNodeTypes) {
				t.Errorf("gathered types = %v, want %v", actualNodes, test.expectedNodeTypes)
			}

			var actualNegIndexes []int
			gatherNodeNegativeIndexes(tree.Nodes[0], &actualNegIndexes)

			if !reflect.DeepEqual(actualNegIndexes, test.expectedNegIndexes) {
				t.Errorf("gathered neg indexes = %v, want %v", actualNegIndexes, test.expectedNegIndexes)
			}
		})
	}
}

func TestSuccessExamples(t *testing.T) {

	var opts = []ParseOptT{WithGenIds()}

	rules, err := filepath.Glob(filepath.Join("../testdata", "success_examples", "*.yaml"))
	if err != nil {
		t.Fatalf("Error finding CRE test files: %v", err)
	}

	for _, rule := range rules {

		// Read the test file
		testData, err := os.ReadFile(rule)
		if err != nil {
			t.Fatalf("Error reading test file %s: %v", rule, err)
		}

		_, err = Parse(testData, opts...)
		if err != nil {
			t.Fatalf("Error parsing rule %s: %v", rule, err)
		}
	}
}

func TestParseFail(t *testing.T) {

	var opts = []ParseOptT{}

	var tests = map[string]struct {
		rule string
		line int
		col  int
		err  error
	}{
		"Fail_Typo": {
			rule: testdata.TestFailTypo,
			line: 16,
			col:  11,
			err:  ErrTermNotFound,
		},
		"Fail_MissingOrder": {
			rule: testdata.TestFailMissingOrder,
			line: 12,
			col:  9,
			err:  ErrMissingOrder,
		},
		"Fail_MissingMatch": {
			rule: testdata.TestFailMissingMatch,
			line: 12,
			col:  9,
			err:  ErrMissingMatch,
		},
		"Fail_InvalidWindow": {
			rule: testdata.TestFailInvalidWindow,
			line: 12,
			col:  17,
			err:  ErrInvalidWindow,
		},
		"Fail_UnsupportedRule": {
			rule: testdata.TestFailUnsupportedRule,
			line: 11,
			col:  7,
			err:  ErrNotSupported,
		},
		"Fail_TermsSyntaxError": {
			rule: testdata.TestFailTermsSyntaxError1,
			line: 34,
			col:  7,
			err:  ErrMissingMatch,
		},
		"Fail_TermsSyntaxError2": {
			rule: testdata.TestFailTermsSyntaxError2,
			line: 36,
			col:  15,
			err:  ErrInvalidWindow,
		},
		"Fail_MissingCreId": {
			rule: testdata.TestFailMissingCreRule,
			line: 10,
			col:  7,
			err:  ErrMissingCreId,
		},
		"Fail_MissingRuleId": {
			rule: testdata.TestFailMissingRuleIdRule,
			line: 10,
			col:  7,
			err:  ErrMissingRuleId,
		},
		"Fail_MissingRuleHash": {
			rule: testdata.TestFailMissingRuleHashRule,
			line: 10,
			col:  7,
			err:  ErrMissingRuleHash,
		},
		"Fail_BadRuleId": {
			rule: testdata.TestFailBadRuleIdRule,
			line: 11,
			col:  7,
			err:  ErrInvalidRuleId,
		},
		"Fail_BadCreId": {
			rule: testdata.TestFailBadCreIdRule,
			line: 11,
			col:  7,
			err:  ErrInvalidCreId,
		},
		"Fail_BadRuleHash": {
			rule: testdata.TestFailBadRuleHashRule,
			line: 11,
			col:  7,
			err:  ErrInvalidRuleHash,
		},
		"Fail_ScriptRoot": {
			rule: testdata.TestFailScriptRoot,
			line: 10,
			col:  7,
			err:  ErrNotSupported,
		},
		"Fail_ScriptNoInput": {
			rule: testdata.TestFailScriptNoInput,
			line: 11,
			col:  9,
			err:  ErrMissingInput,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(test.rule), opts...)
			if err == nil {
				t.Fatalf("Expected error parsing rule")
			}

			if !errors.Is(err, test.err) {
				log.Info().Type("err_type", err).Msg("error")
				t.Errorf("Expected error %v, got %v", test.err, err)
			}

			if pos, ok := pqerr.PosOf(err); ok {
				if pos.Line != test.line {
					t.Errorf("Expected error position line=%d, got line=%d", test.line, pos.Line)
				}
				if pos.Col != test.col {
					t.Errorf("Expected error position col=%d, got col=%d", test.col, pos.Col)
				}
			} else {
				DumpErrorChain(err)
				t.Errorf("Expected wrapped pqerr error %v, got %v", test.err, err)
			}
		})
	}
}

const stableRuleYaml = `
rules:
  - cre:
      id: PREQUEL-2026-0004
      severity: 3
      title: ArgoCD Excessive Syncs
      category: argocd-problems
      author: Prequel
      description: |
        ArgoCD Reconciliation Storm
      tags:
        - argocd
        - sync-loop
        - prequel-v0.14+
      mitigation:
        Remove "CreateNamespace=true" from applications involved in the sync loop reconciliation storm.
      impact: |
        The ArgoCD applications are in a sync loop, which means that they are being synced more than once per minute. This increases the load on the ArgoCD server and the Kubernetes cluster.
      mitigationScore: 3
      impactScore: 4
      references:
        - https://github.com/argoproj/argo-cd/issues/14666#issuecomment-1715538502
        - https://argo-cd.readthedocs.io/en/stable/operator-manual/reconcile/
      applications:
        - name: "argocd"
          processName: "argocd-application-controller"
          processPath: "/app/argocd/argocd-application-controller"
          containerName: "argocd-application-controller"
          imageUrl: "quay.io/argoproj/argocd:v2.7.5"
          repoUrl: "https://github.com/argoproj/argo-cd"

    metadata:
      kind: custom
      id: NRdyR6FoTTsziQRVrxFMv5
      gen: 1
    rule:
      set:
        event:
          source: cre.kubernetes
        correlations:
          - appNamespace
          - appName
        window: 1200s
        match:
          - jq: |
              (.message | test("Initiated automated sync to '.*'"))
              and (.source.component == "argocd-application-controller")
            extract:
              - name: appNamespace
                jq: .involvedObject.namespace
              - name: appName
                jq: .involvedObject.name
            count: 3
          - jq: |
              (.message | test("(Partial s|S)ync operation to .* succeeded"))
              and (.source.component == "argocd-application-controller")
            extract:
              - name: appNamespace
                jq: .involvedObject.namespace
              - name: appName
                jq: .involvedObject.name
            count: 3
          - jq: |
              (.message | test("Updated sync status: Synced -> OutOfSync"))
              and (.source.component == "argocd-application-controller")
            extract:
              - name: appNamespace
                jq: .involvedObject.namespace
              - name: appName
                jq: .involvedObject.name
            count: 3
          - jq: |
              (.message | test("Updated sync status: OutOfSync -> Synced"))
              and (.source.component == "argocd-application-controller")
            extract:
              - name: appNamespace
                jq: .involvedObject.namespace
              - name: appName
                jq: .involvedObject.name
            count: 3
`

func TestStableHashStability(t *testing.T) {
	// Use a stable rule from above for test.
	ruleYaml := stableRuleYaml

	// Unmarshal YAML to ParseRuleT
	rules, err := Unmarshal([]byte(ruleYaml))
	if err != nil {
		t.Fatalf("Failed to unmarshal rule: %v", err)
	}
	if len(rules.Rules) == 0 {
		t.Fatalf("No rules found in testdata")
	}
	rule := rules.Rules[0]

	// Compute stable hash
	hash1, err := StableHash(rule)
	if err != nil {
		t.Fatalf("Failed to compute stable hash: %v", err)
	}

	// Modify non-semantic metadata fields
	rule.Metadata.Version = "v2.0.0"
	rule.Metadata.Gen = 42

	// Compute stable hash again
	hash2, err := StableHash(rule)
	if err != nil {
		t.Fatalf("Failed to compute stable hash after metadata change: %v", err)
	}

	if hash1 != hash2 {
		t.Errorf("StableHash changed after non-semantic metadata update: %s != %s", hash1, hash2)
	}

	if hash1 != "QFr5UWZMni8KYe4B7FkYg64p8CaRr6yeuynwDfPXjDj" {
		t.Errorf("StableHash value changed unexpectedly: got %s, want %s", hash1, "QFr5UWZMni8KYe4B7FkYg64p8CaRr6yeuynwDfPXjDj")
	}
}

func DumpErrorChain(err error) {
	i := 0
	for err != nil {
		fmt.Printf("#%d  %T  %q\n", i, err, err.Error())
		i++
		err = errors.Unwrap(err)
	}
}
