package ast

import (
	"strings"
	"testing"
	"time"

	"github.com/prequel-dev/prequel-compiler/pkg/testdata"
	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

// Dummy AstNode for unknown type
type drawDummyNode struct{}

func (d drawDummyNode) Type() AstNodeType        { return AstNodeType(99) }
func (d drawDummyNode) Scope() AstScopeT         { return AstScopeT(99) }
func (d drawDummyNode) Address() AstNodeAddressT { return AstNodeAddressT{} }
func (d drawDummyNode) Parent() *AstNodeAddressT { return nil }

func TestDrawOpts_WithColorAndParent(t *testing.T) {
	opts := parseDrawOpts([]DrawOpt{WithColor(), WithParent()})
	if !opts.colorize {
		t.Errorf("WithColor() did not set colorize")
	}
	if !opts.parent {
		t.Errorf("WithParent() did not set parent")
	}
}

func TestDrawOpts_Defaults(t *testing.T) {
	opts := parseDrawOpts(nil)
	if opts.colorize {
		t.Errorf("Default colorize should be false")
	}
	if opts.parent {
		t.Errorf("Default parent should be false")
	}
}

func TestDrawOpts_StyleMethods(t *testing.T) {
	o := drawOpts{colorize: false}
	if got := o.styleAddr("foo"); got != "foo" {
		t.Errorf("styleAddr() = %q, want %q", got, "foo")
	}
	if got := o.styleScope("bar"); got != "bar" {
		t.Errorf("styleScope() = %q, want %q", got, "bar")
	}
	if got := o.styleTermValue("baz"); got != "baz" {
		t.Errorf("styleTermValue() = %q, want %q", got, "baz")
	}
	if got := o.styleEventSrc("src"); got != "src" {
		t.Errorf("styleEventSrc() = %q, want %q", got, "src")
	}
	if got := o.styleId("id"); got != "id" {
		t.Errorf("styleId() = %q, want %q", got, "id")
	}
	if got := o.styleNegate("neg"); got != "neg" {
		t.Errorf("styleNegate() = %q, want %q", got, "neg")
	}
}

func TestRenderScopeLabel(t *testing.T) {
	o := drawOpts{}
	tests := []struct {
		scope AstScopeT
		want  string
	}{
		{AstScopeGlobal, "[G]"},
		{AstScopeOrganization, "[O]"},
		{AstScopeCluster, "[C]"},
		{AstScopeNode, "[N]"},
		{AstScopeT(99), "[?]"},
	}
	for _, tt := range tests {
		got := renderScopeLabel(tt.scope, nil, o)
		if got != tt.want {
			t.Errorf("renderScopeLabel(%v) = %q, want %q", tt.scope, got, tt.want)
		}
	}
}

func TestRenderNegateOpts(t *testing.T) {
	o := drawOpts{}
	// nil
	if got := renderNegateOpts(nil, o); got != "" {
		t.Errorf("renderNegateOpts(nil) = %q, want empty", got)
	}
	// all fields
	n := &AstNegateOptsT{
		Window:   2 * time.Second,
		Slide:    1 * time.Second,
		Anchor:   3,
		Absolute: true,
	}
	got := renderNegateOpts(n, o)
	if !strings.Contains(got, "window=2s") ||
		!strings.Contains(got, "slide=1s") ||
		!strings.Contains(got, "anchor=3") ||
		!strings.Contains(got, "absolute=true") {
		t.Errorf("renderNegateOpts() = %q, missing expected fields", got)
	}
	// only one field
	n = &AstNegateOptsT{Window: 1 * time.Second}
	got = renderNegateOpts(n, o)
	if !strings.Contains(got, "window=1s") {
		t.Errorf("renderNegateOpts() = %q, want window=1s", got)
	}
}

func TestExtractProps_UnknownType(t *testing.T) {
	props := extractProps(drawDummyNode{}, drawOpts{})
	if len(props) == 0 || props[0] != "type=unknown" {
		t.Errorf("extractProps(dummyNode) = %v, want [type=unknown]", props)
	}
}

func TestExtractProps_AstInnerNodeT(t *testing.T) {
	node := &AstInnerNodeT{
		Window:       5 * time.Second,
		Correlations: []string{"foo", "bar"},
	}
	props := extractProps(node, drawOpts{})
	if !containsAny(props, []string{"type=machine_set", "type=machine_seq", "type=log_set", "type=log_seq"}) {
		t.Errorf("extractProps(AstInnerNodeT) missing type: %v", props)
	}
	if !contains(props, "window=5s") {
		t.Errorf("extractProps(AstInnerNodeT) missing window: %v", props)
	}
	if !containsAny(props, []string{"correlations=[foo bar]", "correlations=[foo,bar]"}) {
		t.Errorf("extractProps(AstInnerNodeT) missing correlations: %v", props)
	}
}

func TestExtractProps_AstMatchLeafT(t *testing.T) {
	node := &AstMatchLeafT{
		Window:       3 * time.Second,
		Correlations: []string{"x"},
		Event:        AstEventT{Source: "syslog", Origin: true},
		Terms: []AstFieldT{
			{Field: "f", TermValue: match.TermT{Type: match.TermRaw, Value: "v"}, Count: 2},
		},
		Negate: []AstFieldT{
			{Field: "nf", TermValue: match.TermT{Type: match.TermRegex, Value: "nv"}, Count: 1, NegateOpts: &AstNegateOptsT{Window: 1 * time.Second}},
		},
	}
	props := extractProps(node, drawOpts{})
	if !contains(props, "type=line_match") {
		t.Errorf("extractProps(AstMatchLeafT) missing type=line_match: %v", props)
	}
	if !contains(props, "window=3s") {
		t.Errorf("extractProps(AstMatchLeafT) missing window: %v", props)
	}
	if !containsAny(props, []string{"correlations=[x]", "correlations=[x]"}) {
		t.Errorf("extractProps(AstMatchLeafT) missing correlations: %v", props)
	}
	if !contains(props, "event_src=syslog") {
		t.Errorf("extractProps(AstMatchLeafT) missing event_src=syslog: %v", props)
	}
	if !contains(props, "origin=true") {
		t.Errorf("extractProps(AstMatchLeafT) missing origin=true: %v", props)
	}
	if !containsAny(props, []string{"term [0,raw,2,field=f]=v", "term [0,raw,2,field=f]=v"}) {
		t.Errorf("extractProps(AstMatchLeafT) missing term: %v", props)
	}
	if !containsAny(props, []string{"negate [0,regex,1,field=nf][window=1s]=nv", "negate [0,regex,1,field=nf][window=1s]=nv"}) {
		t.Errorf("extractProps(AstMatchLeafT) missing negate: %v", props)
	}
}

func TestExtractProps_AstPromT(t *testing.T) {
	node := &AstPromT{
		Expr:     "up",
		For:      10 * time.Second,
		Interval: 5 * time.Second,
		Event:    &AstEventT{Source: "prom", Origin: false},
	}
	props := extractProps(node, drawOpts{})
	if !contains(props, "type=promql") {
		t.Errorf("extractProps(AstPromT) missing type=promql: %v", props)
	}
	if !contains(props, "event_src=prom") {
		t.Errorf("extractProps(AstPromT) missing event_src=prom: %v", props)
	}
	if !contains(props, "expr=up") {
		t.Errorf("extractProps(AstPromT) missing expr=up: %v", props)
	}
	if !contains(props, "interval=5s") {
		t.Errorf("extractProps(AstPromT) missing interval=5s: %v", props)
	}
	if !contains(props, "for=10s") {
		t.Errorf("extractProps(AstPromT) missing for=10s: %v", props)
	}
}

func TestExtractProps_AstScriptT(t *testing.T) {
	node := &AstScriptT{
		Code:     "print(1)",
		Language: "python",
		Timeout:  2 * time.Second,
	}
	props := extractProps(node, drawOpts{})
	if !contains(props, "type=script") {
		t.Errorf("extractProps(AstScriptT) missing type=script: %v", props)
	}
	if !contains(props, "code=print(1)") {
		t.Errorf("extractProps(AstScriptT) missing code: %v", props)
	}
	if !contains(props, "language=python") {
		t.Errorf("extractProps(AstScriptT) missing language=python: %v", props)
	}
	if !contains(props, "timeout=2s") {
		t.Errorf("extractProps(AstScriptT) missing timeout=2s: %v", props)
	}
}

func contains(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

func containsAny(slice []string, options []string) bool {
	for _, v := range slice {
		for _, o := range options {
			if v == o {
				return true
			}
		}
	}
	return false
}

const (
	testOutput_TestSuccessComplexRule4 = `Rule: J7uRQTGpGMyL1iFpssnBeS
│  • hash=2KdXQZDAfRbYcH9FBDteBS
│  • gen=0
│  • cre_id=TestSuccessComplexRule4
└─ [C] v1.machine_seq.2KdXQZDAfRbYcH9FBDteBS.d0.n0.t0 
   │  • type=machine_seq
   │  • window=30s
   │  • correlations=[hostname]
   ├─ [N] v1.log_seq.2KdXQZDAfRbYcH9FBDteBS.d1.n1.t0 
   │     • type=line_match
   │     • window=10s
   │     • event_src=rabbitmq
   │     • origin=true
   │     • term [0,raw,10]=Discarding message
   │     • term [1,raw,1]=Mnesia overloaded
   │     • negate [0,raw,1]=SIGTERM
   ├─ [C] v1.machine_seq.2KdXQZDAfRbYcH9FBDteBS.d1.n2.t1 
   │  │  • type=machine_seq
   │  │  • window=5s
   │  │  • correlations=[container_id]
   │  ├─ [N] v1.log_seq.2KdXQZDAfRbYcH9FBDteBS.d2.n3.t0 
   │  │     • type=line_match
   │  │     • window=1s
   │  │     • event_src=nginx
   │  │     • term [0,raw,1]=error message
   │  │     • term [1,raw,1]=shutdown
   │  ├─ [N] v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n4.t1 
   │  │     • type=line_match
   │  │     • event_src=nginx
   │  │     • term [0,raw,1]=90%
   │  └─ [N] v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n5.t2 
   │        • type=line_match
   │        • event_src=k8s
   │        • term [0,raw,1,field=reason]=Killing
   ├─ [C] v1.machine_seq.2KdXQZDAfRbYcH9FBDteBS.d1.n6.t2 
   │  │  • type=machine_seq
   │  │  • window=5s
   │  │  • correlations=[container_id]
   │  ├─ [N] v1.log_seq.2KdXQZDAfRbYcH9FBDteBS.d2.n7.t0 
   │  │     • type=line_match
   │  │     • window=1s
   │  │     • event_src=nginx
   │  │     • term [0,raw,1]=error message
   │  │     • term [1,raw,1]=shutdown
   │  ├─ [N] v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n8.t1 
   │  │     • type=line_match
   │  │     • event_src=nginx
   │  │     • term [0,raw,1]=90%
   │  └─ [N] v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d2.n9.t2 
   │        • type=line_match
   │        • event_src=k8s
   │        • term [0,raw,1,field=reason]=Killing
   └─ [¬ N] v1.log_set.2KdXQZDAfRbYcH9FBDteBS.d1.n10.t3 
         • type=line_match
         • event_src=k8s
         • term [0,raw,1,field=reason]=NodeShutdown
`
	testOutput_TestSuccessSimpleRule1 = `Rule: J7uRQTGpGMyL1iFpssnBeS
│  • hash=rdJLgqYgkEp8jg8Qks1qiq
│  • gen=1
│  • cre_id=TestSuccessSimpleRule1
└─ [C] v1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0 
   │  • type=machine_set
   └─ [N] v1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0 
         • type=line_match
         • window=10s
         • event_src=kafka
         • origin=true
         • term [0,raw,3]=io.vertx.core.VertxException: Thread blocked
`
	testOutput_TestSuccessSimpleRule1WithColor = "Rule: \x1b[92mJ7uRQTGpGMyL1iFpssnBeS\x1b[0m\n│  • hash=\x1b[92mrdJLgqYgkEp8jg8Qks1qiq\x1b[0m\n│  • gen=1\n│  • cre_id=TestSuccessSimpleRule1\n└─ \x1b[92m[C]\x1b[0m \x1b[93mv1.machine_set.rdJLgqYgkEp8jg8Qks1qiq.d0.n0.t0\x1b[0m \n   │  • type=machine_set\n   └─ \x1b[92m[N]\x1b[0m \x1b[93mv1.log_set.rdJLgqYgkEp8jg8Qks1qiq.d1.n1.t0\x1b[0m \n         • type=line_match\n         • window=10s\n         • event_src=\x1b[95mkafka\x1b[0m\n         • origin=true\n         • term [0,raw,3]=\x1b[36mio.vertx.core.VertxException: Thread blocked\x1b[0m\n"
)

// NOTE: This test is somewhat brittle since it relies on the exact formatting of the output,
// but it serves as a regression test to ensure that the Draw function continues to walk the
// node tree and include all relevant properties in the output. If the formatting changes,
// this test will need to be updated accordingly.
func TestDraw_WalksNodeTree_Brittle(t *testing.T) {

	tests := []struct {
		name   string
		input  string
		output string
		color  bool
	}{
		{
			name:   "TestSuccessComplexRule4",
			input:  testdata.TestSuccessComplexRule4,
			output: testOutput_TestSuccessComplexRule4,
		},
		{
			name:   "TestSuccessSimpleRule1",
			input:  testdata.TestSuccessSimpleRule1,
			output: testOutput_TestSuccessSimpleRule1,
		},
		{
			name:   "TestSuccessSimpleRule1 with color",
			input:  testdata.TestSuccessSimpleRule1,
			output: testOutput_TestSuccessSimpleRule1WithColor,
			color:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			astRules, err := ParseRules([]byte(tt.input))
			if err != nil {
				t.Fatalf("ParseRules() error = %v", err)
			}

			opts := []DrawOpt{}
			if tt.color {
				opts = append(opts, WithColor())
			}

			out := Draw(astRules[0], opts...)

			if out != tt.output {
				t.Errorf("Draw() output = '%s', want '%s'", out, tt.output)
			}
		})
	}

}
