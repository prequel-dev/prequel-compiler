package compiler

import (
	"errors"
	"fmt"
	"sort"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
	"github.com/rs/zerolog/log"
)

type ObjsT []*ObjT

type ObjTypeT int

const (
	ObjTypeMatcher ObjTypeT = iota
	ObjTypeAssert
)

func (o ObjTypeT) String() string {
	switch o {
	case ObjTypeMatcher:
		return "matcher"
	case ObjTypeAssert:
		return "assert"
	default:
		return "unknown"
	}
}

type ObjT struct {
	Address       ast.AstNodeAddressT
	ParentAddress *ast.AstNodeAddressT
	Scope         ast.AstScopeT
	AbstractType  ast.AstNodeType
	ObjectType    ObjTypeT
	Event         ast.AstEventT
	Object        any
	Cb            CallbackT
}

type compilerOptsT struct {
	runtime RuntimeI
	plugins map[ast.AstScopeT]PluginI
}

type CompilerOptT func(*compilerOptsT)
type PluginI interface {
	Compile(runtime RuntimeI, node ast.AstNode) (ObjsT, error)
}

func WithRuntime(cb RuntimeI) CompilerOptT {
	return func(o *compilerOptsT) {
		o.runtime = cb
	}
}

func WithPlugin(scope ast.AstScopeT, plugin PluginI) CompilerOptT {
	return func(o *compilerOptsT) {
		o.plugins[scope] = plugin
	}
}

func parseOpts(opts []CompilerOptT) compilerOptsT {

	o := compilerOptsT{
		plugins: map[ast.AstScopeT]PluginI{ast.AstScopeNode: defaultPlugin},
		runtime: defaultRuntime,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func Compile(data []byte, scope ast.AstScopeT, opts ...CompilerOptT) (ObjsT, error) {

	rules, err := ast.ParseRules(data)
	if err != nil {
		return nil, err
	}

	return CompileRules(rules, scope, opts...)
}

func CompileRule(rule ast.AstRuleT, scope ast.AstScopeT, opts ...CompilerOptT) (ObjsT, error) {
	o := parseOpts(opts)
	return compileRule(o, rule, scope)
}

func CompileRules(rules []ast.AstRuleT, scope ast.AstScopeT, opts ...CompilerOptT) (ObjsT, error) {
	o := parseOpts(opts)

	var (
		outObjs ObjsT
		errList []error
	)

	for _, rule := range rules {
		objs, err := compileRule(o, rule, scope)
		if err != nil {
			errList = append(errList, err)
		} else {
			outObjs = append(outObjs, objs...)
		}
	}

	return outObjs, errors.Join(errList...)
}

func compileRule(o compilerOptsT, rule ast.AstRuleT, scope ast.AstScopeT) (ObjsT, error) {

	var (
		outObjs ObjsT
	)

	compile := func(node ast.AstNode, _ *ast.AstNegateOptsT) error {

		if node.Scope() != scope {
			return nil
		}

		plugin, ok := o.plugins[scope]
		if !ok {
			log.Error().Str("scope", scope.String()).Msg("No plugin found")
			return fmt.Errorf("%w: %s", ErrUnsupportedScope, scope.String())
		}

		objs, err := plugin.Compile(o.runtime, node)
		if err != nil {
			return err
		}

		outObjs = append(outObjs, objs...)

		return nil
	}

	if err := rule.Walk(compile); err != nil {
		return nil, err
	}

	sortObjs(outObjs, ast.AstNodeTypeSeq)
	sortObjs(outObjs, ast.AstNodeTypeSet)

	return outObjs, nil
}

func NewObj(node ast.AstNode, objType ObjTypeT) *ObjT {

	return &ObjT{
		Address:       node.Address(),
		ParentAddress: node.Parent(),
		Scope:         node.Scope(),
		AbstractType:  node.Type(),
		ObjectType:    objType,
	}
}

// Should we sort by object type?
func sortObjs(items []*ObjT, t ast.AstNodeType) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].AbstractType == t && items[j].AbstractType != t {
			return true
		}
		if items[j].AbstractType == t && items[i].AbstractType != t {
			return false
		}
		return false
	})
}
