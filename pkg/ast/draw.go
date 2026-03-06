package ast

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/rs/zerolog/log"
	"github.com/thediveo/go-asciitree"
)

type DrawOpt func(*drawOpts)

type drawOpts struct {
	colorize bool
	parent   bool
}

func (o drawOpts) styleAddr(addr string) string {
	if o.colorize {
		return text.FgHiYellow.Sprint(addr)
	}
	return addr
}

func WithColor() DrawOpt {
	return func(opts *drawOpts) {
		opts.colorize = true
	}
}

func WithParent() DrawOpt {
	return func(opts *drawOpts) {
		opts.parent = true
	}
}

func parseDrawOpts(opts []DrawOpt) drawOpts {
	var o drawOpts
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func Draw(r AstRuleT, opts ...DrawOpt) string {
	o := parseDrawOpts(opts)

	type nodeT struct {
		depth    uint32
		Label    string   `asciitree:"label"`
		Props    []string `asciitree:"properties"`
		Children []*nodeT `asciitree:"children"`
	}

	root := &nodeT{
		Label: fmt.Sprintf("Rule: %s", o.styleId(r.Metadata.Id)),
		Props: []string{
			fmt.Sprintf("hash=%s", o.styleId(r.Metadata.Hash)),
			fmt.Sprintf("gen=%d", r.Metadata.Gen),
		},
	}

	if r.Cre != nil {
		root.Props = append(root.Props,
			fmt.Sprintf("cre_id=%s", r.Cre.Id),
		)
	}

	fmtLabel := func(n AstNode, nopts *AstNegateOptsT) string {
		return fmt.Sprintf(
			"%s %s %s",
			renderScopeLabel(n.Scope(), nopts, o),
			o.styleAddr(n.Address().String()),
			renderNegateOpts(nopts, o),
		)
	}

	var stack = []*nodeT{}

	findParent := func(depth uint32) int {
		for i := len(stack) - 1; i >= 0; i-- {
			if stack[i].depth == depth {
				return i
			}
		}
		return -1
	}

	walker := func(n AstNode, nopts *AstNegateOptsT) error {

		var (
			curDepth uint32
			addr     = n.Address()
		)

		if len(stack) > 0 {
			curDepth = stack[len(stack)-1].depth
		}

		switch {
		case len(stack) == 0:
			// first node, add to root
			child := &nodeT{
				Label: fmtLabel(n, nopts),
				Props: extractProps(n, o),
			}
			root.Children = append(root.Children, child)
			stack = append(stack, child)

		case addr.Depth == curDepth:
			// sibling node, add to current parent
			parIdx := findParent(addr.Depth - 1)
			if parIdx < 0 {
				return errors.New("parent not found for node")
			}
			parent := stack[parIdx]
			child := &nodeT{
				depth: addr.Depth,
				Label: fmtLabel(n, nopts),
				Props: extractProps(n, o),
			}
			parent.Children = append(parent.Children, child)
			stack = append(stack, child)
		case addr.Depth > curDepth:
			// child node, add to stack
			parent := stack[len(stack)-1]
			child := &nodeT{
				depth: addr.Depth,
				Label: fmtLabel(n, nopts),
				Props: extractProps(n, o),
			}
			parent.Children = append(parent.Children, child)
			stack = append(stack, child)

		case addr.Depth < curDepth:
			// moving back up the tree, pop stack until we find the correct parent
			parIdx := findParent(addr.Depth - 1)
			if parIdx < 0 {
				return errors.New("parent not found for node")
			}
			parent := stack[parIdx]
			child := &nodeT{
				depth: addr.Depth,
				Label: fmtLabel(n, nopts),
				Props: extractProps(n, o),
			}
			parent.Children = append(parent.Children, child)
			stack = stack[:parIdx+1]
			stack = append(stack, child)
		}

		return nil
	}

	if err := r.Walk(walker); err != nil {
		log.Error().Err(err).Msg("Error walking AST for drawing")
	}
	return asciitree.RenderFancy(root)
}

func extractProps(n AstNode, o drawOpts) []string {
	var props []string

	if o.parent {
		if addr := n.Parent(); addr != nil {
			props = append(props,
				fmt.Sprintf("parent=%s", addr.String()),
			)
		}
	}

	switch node := n.(type) {
	case *AstInnerNodeT:
		props = append(props, fmt.Sprintf("type=%s", node.Type().String()))
		if node.Window != 0 {
			props = append(props, fmt.Sprintf("window=%s", node.Window.String()))
		}
		if len(node.Correlations) > 0 {
			props = append(props, fmt.Sprintf("correlations=%v", node.Correlations))
		}

	case *AstMatchLeafT:
		props = append(props, "type=line_match")
		if node.Window != 0 {
			props = append(props, fmt.Sprintf("window=%s", node.Window.String()))
		}
		if len(node.Correlations) > 0 {
			props = append(props, fmt.Sprintf("correlations=%v", node.Correlations))
		}

		props = append(props, fmt.Sprintf("event_src=%s", o.styleEventSrc(node.Event.Source)))
		if node.Event.Origin {
			props = append(props, "origin=true")
		}

		for i, term := range node.Terms {
			cnt := term.Count
			if cnt == 0 {
				cnt = 1
			}

			opts := fmt.Sprintf("%d,%s,%d", i, term.TermValue.Type.String(), cnt)
			if term.Field != "" {
				opts = fmt.Sprintf("%s,field=%s", opts, term.Field)
			}

			props = append(props,
				fmt.Sprintf("term [%s]=%s", opts, o.styleTermValue(term.TermValue.Value)),
			)

		}
		for i, term := range node.Negate {

			cnt := term.Count
			if cnt == 0 {
				cnt = 1
			}

			opts := fmt.Sprintf("%d,%s,%d", i, term.TermValue.Type.String(), cnt)
			if term.Field != "" {
				opts = fmt.Sprintf("%s,field=%s", opts, term.Field)
			}

			negateProps := renderNegateOpts(term.NegateOpts, o)

			props = append(props,
				fmt.Sprintf(
					"negate [%s]%s=%s",
					opts,
					negateProps,
					o.styleTermValue(term.TermValue.Value)),
			)
		}
	case *AstPromT:
		props = append(props, "type=promql")
		props = append(props, fmt.Sprintf("event_src=%s", o.styleEventSrc(node.Event.Source)))
		if node.Event.Origin {
			props = append(props, "origin=true")
		}
		props = append(props, fmt.Sprintf("expr=%s", o.styleTermValue(node.Expr)))
		if node.Interval != 0 {
			props = append(props, fmt.Sprintf("interval=%v", node.Interval))
		}
		if node.For != 0 {
			props = append(props, fmt.Sprintf("for=%v", node.For))
		}

	case *AstScriptT:
		props = append(props, "type=script")
		// Print first few lines of code
		codePreview := node.Code
		if len(codePreview) > 100 {
			codePreview = codePreview[:100] + "..."
		}
		props = append(props, fmt.Sprintf("code=%s", o.styleTermValue(codePreview)))

		if node.Language != "" {
			props = append(props, fmt.Sprintf("language=%s", node.Language))
		}
		if node.Timeout != 0 {
			props = append(props, fmt.Sprintf("timeout=%s", node.Timeout.String()))
		}
	default:
		props = append(props, "type=unknown")
	}

	return props
}

func renderScopeLabel(scope AstScopeT, nopts *AstNegateOptsT, o drawOpts) string {

	var c string
	switch scope {
	case AstScopeGlobal:
		c = "G"
	case AstScopeOrganization:
		c = "O"
	case AstScopeCluster:
		c = "C"
	case AstScopeNode:
		c = "N"
	default:
		c = "?"
	}

	if nopts != nil {
		return o.styleNegate(fmt.Sprintf("[¬ %s]", c))
	}

	return o.styleScope(fmt.Sprintf("[%s]", c))
}

func renderNegateOpts(nopts *AstNegateOptsT, o drawOpts) string {
	if nopts == nil {
		return ""
	}

	var s []string
	if nopts.Window != 0 {
		s = append(s, fmt.Sprintf("window=%s", nopts.Window.String()))
	}
	if nopts.Slide != 0 {
		s = append(s, fmt.Sprintf("slide=%s", nopts.Slide.String()))
	}
	if nopts.Anchor != 0 {
		s = append(s, fmt.Sprintf("anchor=%d", nopts.Anchor))
	}
	if nopts.Absolute {
		s = append(s, "absolute=true")
	}

	if len(s) == 0 {
		return ""
	}

	negateProps := fmt.Sprintf("[%s]", strings.Join(s, ","))

	return o.styleNegate(negateProps)
}

func (o drawOpts) styleScope(addr string) string {
	if o.colorize {
		return text.FgHiGreen.Sprint(addr)
	}
	return addr
}

func (o drawOpts) styleTermValue(val string) string {
	if o.colorize {
		return text.FgCyan.Sprint(val)
	}
	return val
}

func (o drawOpts) styleEventSrc(val string) string {
	if o.colorize {
		return text.FgHiMagenta.Sprint(val)
	}
	return val
}

func (o drawOpts) styleId(val string) string {
	if o.colorize {
		return text.FgHiGreen.Sprint(val)
	}
	return val
}

func (o drawOpts) styleNegate(val string) string {
	if o.colorize {
		return text.FgRed.Sprint(val)
	}
	return val
}
