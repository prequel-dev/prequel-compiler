package parser

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/prequel-dev/prequel-compiler/pkg/testdata"
	"github.com/prequel-dev/prequel-core/pkg/logz"
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

	logz.InitZerolog(logz.WithLevel(""))

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
			expectedNegIndexes: []int{-1, 2, 1, -1, -1, -1, -1},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			tree, err := Parse([]byte(test.rule))
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

		_, err = Parse(testData)
		if err != nil {
			t.Fatalf("Error parsing rule %s: %v", rule, err)
		}
	}
}

func TestParseFail(t *testing.T) {

	logz.InitZerolog(logz.WithLevel(""))

	var tests = map[string]struct {
		rule string
	}{
		"Fail_Typo": {
			rule: testdata.TestFailTypo,
		},
		"Fail_MissingOrder": {
			rule: testdata.TestFailMissingOrder,
		},
		"Fail_MissingMatch": {
			rule: testdata.TestFailMissingMatch,
		},
		"Fail_InvalidWindow": {
			rule: testdata.TestFailInvalidWindow,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(test.rule))
			if err == nil {
				t.Fatalf("Expected error parsing rule")
			}

		})
	}
}
