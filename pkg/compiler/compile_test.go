package compiler

import (
	"testing"

	"github.com/prequel-dev/prequel-compiler/pkg/ast"
)

func TestCompile(t *testing.T) {
	rule := `rules:
  - cre:
      id: set-1x1
      tags:
      - test1
      - test2
      - test3
    metadata:
      id: eeJwJiWQa9TyH3qTYYSZM9
      hash: 9GJSdx4smGJeJCdiw6tiK5
    rule:
      set:
        window: 5m
        event:
          source: nginx.access.log
          origin: true
        match:
          - value: "test"
          - value: "knock"
        negate:
          - value: "nope"
            anchor: 1
`

	xx, err := Compile([]byte(rule), ast.AstScopeNode)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("%+v\n", xx)
}
