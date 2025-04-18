package compiler

import (
	"errors"
	"sort"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
	"github.com/prequel-dev/prequel-compiler/pkg/parser"
	"github.com/prequel-dev/prequel-compiler/pkg/schema"
	"github.com/rs/zerolog/log"
)

var (
	ErrUnsupportedMatcher = errors.New("unsupported matcher")
	ErrUnsupportedScope   = errors.New("unsupported scope")
	ErrInvalidMatcher     = errors.New("invalid matcher")
)

var (
	defaultPlugin  = &NodePlugin{}
	defaultRuntime = &NoopRuntime{}
)

type CbType uint
type CorrelationsT map[string]string

type CbT struct {
	Callback any
}

type ObjsT []*ObjT

type ObjTypeT string

const (
	ObjTypeMatcher ObjTypeT = "match"
	ObjTypeAssert  ObjTypeT = "assert"
)

func (o ObjTypeT) String() string {
	return string(o)
}

type ObjT struct {
	RuleId        string               `json:"rule_id"`
	Address       *ast.AstNodeAddressT `json:"address"`
	ParentAddress *ast.AstNodeAddressT `json:"parent_address"`
	Scope         string               `json:"scope"`
	AbstractType  schema.NodeTypeT     `json:"abstract_type"`
	ObjectType    ObjTypeT             `json:"object_type"`
	Event         ast.AstEventT        `json:"event"`
	Object        any                  `json:"object"`
	Cb            CbT                  `json:"cb"`
}

type compilerOptsT struct {
	debugTree string
	runtime   RuntimeI
	plugins   map[string]PluginI
}

type CompilerOptT func(*compilerOptsT)
type PluginI interface {
	Compile(runtime RuntimeI, node *ast.AstNodeT) (ObjsT, error)
}

func WithDebugTree(path string) CompilerOptT {
	return func(o *compilerOptsT) {
		o.debugTree = path
	}
}

func WithRuntime(cb RuntimeI) CompilerOptT {
	return func(o *compilerOptsT) {
		o.runtime = cb
	}
}

func WithPlugin(scope string, plugin PluginI) CompilerOptT {
	return func(o *compilerOptsT) {
		o.plugins[scope] = plugin
	}
}

func parseOpts(opts []CompilerOptT) compilerOptsT {
	o := compilerOptsT{
		plugins: map[string]PluginI{"node": defaultPlugin},
		runtime: defaultRuntime,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func traverseTree(node *ast.AstNodeT, scope string, callback func(node *ast.AstNodeT) error) error {
	for _, child := range node.Children {
		if err := traverseTree(child, scope, callback); err != nil {
			return err
		}
	}
	return callback(node)
}

func NewObj(node *ast.AstNodeT, objType ObjTypeT) *ObjT {
	return &ObjT{
		RuleId:        node.Metadata.RuleId,
		Address:       node.Metadata.Address,
		ParentAddress: node.Metadata.ParentAddress,
		Scope:         node.Metadata.Scope,
		AbstractType:  node.Metadata.Type,
		ObjectType:    objType,
	}
}

// Should we sort by object type?
func sortObjs(items []*ObjT, t schema.NodeTypeT) {
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

func CompileTree(pt *parser.TreeT, scope string, opts ...CompilerOptT) (ObjsT, error) {

	var (
		err  error
		o    = parseOpts(opts)
		tree *ast.AstT
	)

	if tree, err = ast.BuildTree(pt); err != nil {
		return nil, err
	}

	if o.debugTree != "" {
		if err = ast.DrawTree(tree, o.debugTree); err != nil {
			return nil, err
		}
	}

	return compile(o, tree, scope)
}

func CompileAst(tree *ast.AstT, scope string, opts ...CompilerOptT) (ObjsT, error) {
	var (
		o = parseOpts(opts)
	)

	if o.debugTree != "" {
		if err := ast.DrawTree(tree, o.debugTree); err != nil {
			return nil, err
		}
	}

	return compile(o, tree, scope)
}

func compile(o compilerOptsT, tree *ast.AstT, scope string) (ObjsT, error) {

	var (
		err     error
		outObjs ObjsT
	)

	compile := func(node *ast.AstNodeT) error {

		if node.Metadata.Scope != scope {
			return nil
		}

		plugin, ok := o.plugins[scope]
		if !ok {
			log.Error().Str("scope", scope).Msg("No plugin found")
			return ErrUnsupportedScope
		}

		objs, err := plugin.Compile(o.runtime, node)
		if err != nil {
			log.Error().
				Err(err).
				Str("scope", scope).
				Msg("Failed to compile")
			return err
		}

		outObjs = append(outObjs, objs...)

		return nil
	}

	for _, node := range tree.Nodes {
		if err = traverseTree(node, scope, compile); err != nil {
			return nil, err
		}
	}

	sortObjs(outObjs, schema.NodeTypeSeq)
	sortObjs(outObjs, schema.NodeTypeSet)

	for _, obj := range outObjs {
		log.Info().
			Str("abstract_type", obj.AbstractType.String()).
			Str("abstract_address", obj.Address.String()).
			Str("object_type", obj.ObjectType.String()).
			Msg("Compiled object")
	}

	return outObjs, nil
}

func Compile(data []byte, scope string, opts ...CompilerOptT) (ObjsT, error) {
	var (
		tree *ast.AstT
		o    = parseOpts(opts)
		err  error
	)

	if tree, err = ast.Build(data); err != nil {
		return nil, err
	}

	if o.debugTree != "" {
		if err = ast.DrawTree(tree, o.debugTree); err != nil {
			return nil, err
		}
	}

	return compile(o, tree, scope)
}
