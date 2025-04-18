package ast

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/prequel-dev/prequel-compiler/pkg/testdata"
	"github.com/prequel-dev/prequel-core/pkg/logz"
)

// traverses the tree and collects node types in DFS pre-order (root, then children)
func gatherNodeTypes(node *AstNodeT, out *[]string) {

	if node == nil {
		return
	}

	*out = append(*out, node.Metadata.Type.String())
	for _, child := range node.Children {
		gatherNodeTypes(child, out)
	}
}

func gatherNodeAddresses(node *AstNodeT, out *[]string) {
	if node == nil {
		return
	}

	*out = append(*out, node.Metadata.Address.String())
}

func TestAstSuccess(t *testing.T) {

	logz.InitZerolog(logz.WithPretty(), logz.WithLevel("TRACE"))

	var tests = map[string]struct {
		rule              string
		expectedNodeTypes []string
	}{
		"Success_Simple1": {
			rule:              testdata.TestSuccessSimpleRule1,
			expectedNodeTypes: []string{"machine_seq", "log_seq"},
		},
		"Success_Complex2": {
			rule:              testdata.TestSuccessComplexRule2,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "log_set", "machine_seq", "log_seq", "log_set", "log_set"},
		},
		"Success_Complex3": {
			rule:              testdata.TestSuccessComplexRule3,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "log_set"},
		},
		"Success_Complex4": {
			rule:              testdata.TestSuccessComplexRule4,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "machine_seq", "log_seq", "log_set", "log_set", "machine_seq", "log_seq", "log_set", "log_set", "log_set"},
		},
		"Success_NegateOptions1": {
			rule:              testdata.TestSuccessNegateOptions1,
			expectedNodeTypes: []string{"machine_seq", "log_seq"},
		},
		"Success_NegateOptions2": {
			rule:              testdata.TestSuccessNegateOptions2,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "log_set", "log_set"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {

			var dupeAddresses = make(map[string]struct{})

			ast, err := Build([]byte(test.rule))
			if err != nil {
				t.Fatalf("Error parsing rule: %v", err)
			}

			if err = DrawTree(ast, fmt.Sprintf("rule_%s.dot", name)); err != nil {
				t.Fatalf("Error drawing tree: %v", err)
			}

			if len(ast.Nodes) == 0 {
				t.Fatalf("No nodes found in AST")
			}

			var actualNodes []string
			gatherNodeTypes(ast.Nodes[0], &actualNodes)

			var actualAddresses []string
			gatherNodeAddresses(ast.Nodes[0], &actualAddresses)

			for _, address := range actualAddresses {
				if _, ok := dupeAddresses[address]; ok {
					t.Errorf("Duplicate address found: %s", address)
				}
				dupeAddresses[address] = struct{}{}
			}

			if ast.Nodes[0].Metadata.ParentAddress != nil {
				t.Errorf("Root node has parent address: %s", ast.Nodes[0].Metadata.ParentAddress.String())
			}

			if !reflect.DeepEqual(actualNodes, test.expectedNodeTypes) {
				t.Errorf("gathered types = %v, want %v", actualNodes, test.expectedNodeTypes)
			}
		})
	}
}

func TestAstFail(t *testing.T) {
	logz.InitZerolog(logz.WithLevel(""))

	var tests = map[string]struct {
		rule string
	}{
		"Fail_MissingPositiveCondition": {
			rule: testdata.TestFailMissingPositiveCondition,
		},
		"Fail_BadNegativeCondition1": {
			rule: testdata.TestFailNegativeCondition1,
		},
		"Fail_BadNegativeCondition2": {
			rule: testdata.TestFailNegativeCondition2,
		},
		"Fail_BadNegativeCondition3": {
			rule: testdata.TestFailNegateOptions3,
		},
		"Fail_BadNegativeCondition4": {
			rule: testdata.TestFailNegateOptions4,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Build([]byte(test.rule))
			if err == nil {
				t.Fatalf("Expected error parsing rule")
			}
		})
	}
}

func TestSuccessExamples(t *testing.T) {

	logz.InitZerolog(logz.WithLevel(""))

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

		_, err = Build(testData)
		if err != nil {
			t.Fatalf("Error building rule %s: %v", rule, err)
		}
	}
}

func TestFailureExamples(t *testing.T) {

	logz.InitZerolog(logz.WithLevel(""))

	rules, err := filepath.Glob(filepath.Join("../testdata", "failure_examples", "*.yaml"))
	if err != nil {
		t.Fatalf("Error finding CRE test files: %v", err)
	}

	for _, rule := range rules {

		// Read the test file
		testData, err := os.ReadFile(rule)
		if err != nil {
			t.Fatalf("Error reading test file %s: %v", rule, err)
		}

		_, err = Build(testData)
		if err == nil {
			t.Fatalf("Expected error building rule %s", rule)
		}
	}
}
