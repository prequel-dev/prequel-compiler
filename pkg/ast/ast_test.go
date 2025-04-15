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

func TestAstSuccess(t *testing.T) {

	logz.InitZerolog(logz.WithLevel(""))

	var tests = map[string]struct {
		rule              string
		expectedNodeTypes []string
	}{
		"Success_Simple1": {
			rule:              testdata.TestSuccessSimpleRule1,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "desc"},
		},
		"Success_Complex2": {
			rule:              testdata.TestSuccessComplexRule2,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "desc", "log_set", "desc", "machine_seq", "log_seq", "desc", "log_set", "desc", "log_set", "desc", "desc"},
		},
		"Success_Complex3": {
			rule:              testdata.TestSuccessComplexRule3,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "desc", "log_set", "desc"},
		},
		"Success_Complex4": {
			rule:              testdata.TestSuccessComplexRule4,
			expectedNodeTypes: []string{"machine_seq", "log_seq", "desc", "machine_seq", "log_seq", "desc", "log_set", "desc", "log_set", "desc", "desc", "machine_seq", "log_seq", "desc", "log_set", "desc", "log_set", "desc", "desc", "log_set", "desc"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			ast, err := Build([]byte(test.rule))
			if err != nil {
				t.Fatalf("Error parsing rule: %v", err)
			}

			if err = DrawTree(ast, fmt.Sprintf("rule_%s.dot", name)); err != nil {
				t.Fatalf("Error drawing tree: %v", err)
			}

			var actualNodes []string
			gatherNodeTypes(ast.Nodes[0], &actualNodes)

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
