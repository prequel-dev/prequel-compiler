package ast

import (
	"errors"
	"reflect"
	"regexp"
	"testing"
	"time"

	"github.com/goccy/go-yaml/ast"
)

func TestNodeToMapping(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		wantErr bool
	}{
		{"nil node", nil, true},
		{"not mapping", &ast.StringNode{Value: "foo"}, true},
		{"mapping", &ast.MappingNode{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.nodeToMapping(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToMapping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNodeToSequence(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		wantErr bool
	}{
		{"nil node", nil, true},
		{"not sequence", &ast.StringNode{Value: "foo"}, true},
		{"sequence", &ast.SequenceNode{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := p.nodeToSequence(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToSequence() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestNodeToString(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    string
		wantErr bool
	}{
		{"nil node", nil, "", true},
		{"string node", &ast.StringNode{Value: "foo"}, "foo", false},
		{"literal node", &ast.LiteralNode{Value: &ast.StringNode{Value: "bar"}}, "bar", false},
		{"literal node nil value", &ast.LiteralNode{Value: nil}, "", true},
		{"wrong type", &ast.IntegerNode{Value: int64(1)}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToString(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToString() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToInt64(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    int64
		wantErr bool
	}{
		{"nil node", nil, 0, true},
		{"not integer", &ast.StringNode{Value: "foo"}, 0, true},
		{"int64 value", &ast.IntegerNode{Value: int64(42)}, 42, false},
		{"wrong value type", &ast.IntegerNode{Value: "notint"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToInt64(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToInt64() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToInt64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToUint64(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    uint64
		wantErr bool
	}{
		{"nil node", nil, 0, true},
		{"not integer", &ast.StringNode{Value: "foo"}, 0, true},
		{"uint64 value", &ast.IntegerNode{Value: uint64(42)}, 42, false},
		{"wrong value type", &ast.IntegerNode{Value: "notuint"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToUint64(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToUint64() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToUint64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToUint(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    uint
		wantErr bool
	}{
		{"ok", &ast.IntegerNode{Value: uint64(42)}, 42, false},
		{"bad type", &ast.StringNode{Value: "foo"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToUint(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToUint() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToUint() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToBool(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    bool
		wantErr bool
	}{
		{"nil node", nil, false, true},
		{"not bool", &ast.StringNode{Value: "foo"}, false, true},
		{"bool true", &ast.BoolNode{Value: true}, true, false},
		{"bool false", &ast.BoolNode{Value: false}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToBool(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToBool() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToBool() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToStrs(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    []string
		wantErr bool
	}{
		{
			"ok",
			&ast.SequenceNode{Values: []ast.Node{
				&ast.StringNode{Value: "a"},
				&ast.StringNode{Value: "b"},
			}},
			[]string{"a", "b"},
			false,
		},
		{
			"not sequence",
			&ast.StringNode{Value: "foo"},
			nil,
			true,
		},
		{
			"element not string",
			&ast.SequenceNode{Values: []ast.Node{
				&ast.IntegerNode{Value: int64(1)},
			}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToStrs(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToStrs() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("nodeToStrs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToRegex(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    *regexp.Regexp
		wantErr bool
	}{
		{
			"ok",
			&ast.StringNode{Value: "^foo$"},
			regexp.MustCompile("^foo$"),
			false,
		},
		{
			"bad regex",
			&ast.StringNode{Value: "["},
			nil,
			true,
		},
		{
			"not string",
			&ast.IntegerNode{Value: int64(1)},
			nil,
			true,
		},
		{
			"empty regex",
			&ast.StringNode{Value: ""},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToRegex(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToRegex() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.want != nil && got != nil && got.String() != tt.want.String() {
				t.Errorf("nodeToRegex() = %v, want %v", got, tt.want)
			}
			if (tt.want == nil) != (got == nil) {
				t.Errorf("nodeToRegex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToJq(t *testing.T) {
	p := &parserT{
		validateJQ: func(s string) error {
			if s == "bad" {
				return errors.New("bad jq")
			}
			return nil
		},
	}
	tests := []struct {
		name    string
		node    ast.Node
		want    string
		wantErr bool
	}{
		{"ok", &ast.StringNode{Value: ".foo"}, ".foo", false},
		{"bad jq", &ast.StringNode{Value: "bad"}, "", true},
		{"not string", &ast.IntegerNode{Value: int64(1)}, "", true},
		{"empty string", &ast.StringNode{Value: ""}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToJq(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToJq() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToJq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToDuration(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    time.Duration
		wantErr bool
	}{
		{"ok", &ast.StringNode{Value: "1s"}, time.Second, false},
		{"bad duration", &ast.StringNode{Value: "notdur"}, 0, true},
		{"not string", &ast.IntegerNode{Value: int64(1)}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToDuration(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToDuration() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNodeToDurationPositive(t *testing.T) {
	p := &parserT{}
	tests := []struct {
		name    string
		node    ast.Node
		want    time.Duration
		wantErr bool
	}{
		{"ok", &ast.StringNode{Value: "1s"}, time.Second, false},
		{"zero", &ast.StringNode{Value: "0s"}, 0, true},
		{"negative", &ast.StringNode{Value: "-1s"}, 0, true},
		{"bad duration", &ast.StringNode{Value: "notdur"}, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := p.nodeToDurationPositive(tt.node)
			if (err != nil) != tt.wantErr {
				t.Errorf("nodeToDurationPositive() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("nodeToDurationPositive() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindKey(t *testing.T) {
	node := &ast.MappingNode{
		Values: []*ast.MappingValueNode{
			{
				Key:   &ast.StringNode{Value: "foo"},
				Value: &ast.StringNode{Value: "bar"},
			},
			{
				Key:   &ast.StringNode{Value: "baz"},
				Value: &ast.StringNode{Value: "qux"},
			},
		},
	}
	tests := []struct {
		name string
		key  string
		want ast.Node
	}{
		{"found", "foo", &ast.StringNode{Value: "foo"}},
		{"not found", "nope", nil},
		{"not mapping", "foo", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n ast.Node = node
			if tt.name == "not mapping" {
				n = &ast.StringNode{Value: "foo"}
			}
			got := findKey(n, tt.key)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
