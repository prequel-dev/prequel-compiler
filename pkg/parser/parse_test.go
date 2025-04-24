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
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(test.rule))
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

func DumpErrorChain(err error) {
	i := 0
	for err != nil {
		fmt.Printf("#%d  %T  %q\n", i, err, err.Error())
		i++
		err = errors.Unwrap(err)
	}
}
