package version

import (
	"fmt"

	"github.com/Masterminds/semver/v3"
)

// 'vers' is the semantic version of the prequel-compiler package.
// It tracks the interface and the functionality of the package,
// and should be updated whenever there are changes that
// modify the logic of the rules engine.

// Note that this is not the same as the git commit version of the package.

var (
	vers = "v0.1.0"
	sver = semver.MustParse(vers)
)

var ErrBadConstraint = fmt.Errorf("bad version constraint")

type ErrVersionNotAllowed struct {
	Version    string
	Expression string
}

func (e ErrVersionNotAllowed) Error() string {
	return fmt.Sprintf("compiler version %s does not satisfy version constraint: '%s'", e.Version, e.Expression)
}

func (e ErrVersionNotAllowed) Is(target error) bool {
	_, ok := target.(ErrVersionNotAllowed)
	if !ok {
		tt, ok := target.(*ErrVersionNotAllowed)
		if !ok || tt == nil {
			return false
		}
	}
	return true
}

func SemVer() string {
	return vers
}

// Evaluate version expression against the compiler version. Version expressions are of the form:
// - "0.1.0" (exact match)
// - "<0.2.0" (less than)
// - ">=0.1.0" (greater than or equal to)

func AllowVersion(expression string) error {
	constraint, err := semver.NewConstraint(expression)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrBadConstraint, err)
	}

	if !constraint.Check(sver) {
		return ErrVersionNotAllowed{Expression: expression, Version: vers}
	}

	return nil
}
