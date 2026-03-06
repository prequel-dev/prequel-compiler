package ast

import (
	"reflect"
	"testing"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

func TestAstNodeAddressT_String(t *testing.T) {
	addr := AstNodeAddressT{
		Type:     AstNodeTypeSeq,
		RuleId:   "rule1",
		RuleHash: "hash1",
		Rank:     2,
		Depth:    3,
		NodeId:   4,
	}
	got := addr.String()
	want := "v1.machine_seq.hash1.d3.n4.t2"
	if got != want {
		t.Errorf("AstNodeAddressT.String() = %q, want %q", got, want)
	}
}

func TestAstNodeType_String(t *testing.T) {
	tests := []struct {
		typ  AstNodeType
		want string
	}{
		{AstNodeTypeSet, nodeTypeSet},
		{AstNodeTypeSeq, nodeTypeSeq},
		{AstNodeTypeLogSet, nodeTypeLogSet},
		{AstNodeTypeLogSeq, nodeTypeLogSeq},
		{AstNodeTypePromQL, nodeTypePromQL},
		{AstNodeTypeScript, nodeTypeScript},
		{AstNodeType(99), nodeTypeUnknown},
	}
	for _, tt := range tests {
		got := tt.typ.String()
		if got != tt.want {
			t.Errorf("AstNodeType(%d).String() = %q, want %q", tt.typ, got, tt.want)
		}
	}
}

func TestAstScopeT_String(t *testing.T) {
	tests := []struct {
		scope AstScopeT
		want  string
	}{
		{AstScopeNode, scopeTypeNode},
		{AstScopeCluster, scopeTypeCluster},
		{AstScopeOrganization, scopeTypeOrganization},
		{AstScopeGlobal, scopeTypeGlobal},
		{AstScopeT(99), scopeTypeUnknown},
	}
	for _, tt := range tests {
		got := tt.scope.String()
		if got != tt.want {
			t.Errorf("AstScopeT(%d).String() = %q, want %q", tt.scope, got, tt.want)
		}
	}
}

func TestBaseAst_Methods(t *testing.T) {
	addr := AstNodeAddressT{
		Type:     AstNodeTypeLogSeq,
		RuleId:   "rule2",
		RuleHash: "hash2",
		Rank:     1,
		Depth:    2,
		NodeId:   3,
	}
	parent := &AstNodeAddressT{Type: AstNodeTypeSet}
	b := baseAst{
		scope:   AstScopeCluster,
		address: addr,
		parent:  parent,
	}
	if !reflect.DeepEqual(b.Address(), addr) {
		t.Errorf("baseAst.Address() = %+v, want %+v", b.Address(), addr)
	}
	if b.Type() != AstNodeTypeLogSeq {
		t.Errorf("baseAst.Type() = %v, want %v", b.Type(), AstNodeTypeLogSeq)
	}
	if b.Scope() != AstScopeCluster {
		t.Errorf("baseAst.Scope() = %v, want %v", b.Scope(), AstScopeCluster)
	}
	if b.Parent() != parent {
		t.Errorf("baseAst.Parent() = %+v, want %+v", b.Parent(), parent)
	}
}

func TestAstFieldT_ZeroValue(t *testing.T) {
	var f AstFieldT
	if f.Count != 0 || f.Field != "" || f.TermValue != (match.TermT{}) || f.NegateOpts != nil || f.Extracts != nil {
		t.Errorf("AstFieldT zero value not as expected: %+v", f)
	}
}

func TestAstEventT_ZeroValue(t *testing.T) {
	var e AstEventT
	if e.Source != "" || e.Origin != false {
		t.Errorf("AstEventT zero value not as expected: %+v", e)
	}
}

func TestAstAppT_ZeroValue(t *testing.T) {
	var a AstAppT
	if a.Name != "" || a.ProcessName != "" || a.ProcessPath != "" || a.ContainerName != "" ||
		a.ImageUrl != "" || a.RepoUrl != "" || a.Version != "" {
		t.Errorf("AstAppT zero value not as expected: %+v", a)
	}
}

func TestAstExtractT_ZeroValue(t *testing.T) {
	var e AstExtractT
	if e.Name != "" || e.JqValue != "" || e.RegexValue != "" {
		t.Errorf("AstExtractT zero value not as expected: %+v", e)
	}
}

func TestAstNegateOptsT_ZeroValue(t *testing.T) {
	var n AstNegateOptsT
	if n.Window != 0 || n.Slide != 0 || n.Anchor != 0 || n.Absolute != false {
		t.Errorf("AstNegateOptsT zero value not as expected: %+v", n)
	}
}

func TestAstMetadataT_ZeroValue(t *testing.T) {
	var m AstMetadataT
	if m.Name != "" || m.Id != "" || m.Hash != "" || m.Kind != "" || m.Gen != 0 {
		t.Errorf("AstMetadataT zero value not as expected: %+v", m)
	}
}

func TestAstCreT_ZeroValue(t *testing.T) {
	var c AstCreT
	if c.Id != "" || c.Severity != 0 || c.Title != "" || c.Category != "" || c.Tags != nil ||
		c.Author != "" || c.Description != "" || c.Impact != "" || c.ImpactScore != 0 ||
		c.Cause != "" || c.Mitigation != "" || c.MitigationScore != 0 || c.References != nil ||
		c.Reports != 0 || c.Applications != nil {
		t.Errorf("AstCreT zero value not as expected: %+v", c)
	}
}

func TestAstMatchLeafT_Fields(t *testing.T) {
	leaf := AstMatchLeafT{
		Window:       5 * time.Second,
		Correlations: []string{"foo", "bar"},
		Terms:        []AstFieldT{{Field: "f"}},
		Negate:       []AstFieldT{{Field: "n"}},
		Event:        AstEventT{Source: "syslog", Origin: true},
	}
	if leaf.Window != 5*time.Second ||
		!reflect.DeepEqual(leaf.Correlations, []string{"foo", "bar"}) ||
		len(leaf.Terms) != 1 || leaf.Terms[0].Field != "f" ||
		len(leaf.Negate) != 1 || leaf.Negate[0].Field != "n" ||
		leaf.Event.Source != "syslog" || !leaf.Event.Origin {
		t.Errorf("AstMatchLeafT fields not as expected: %+v", leaf)
	}
}
