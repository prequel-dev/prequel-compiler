package version

import (
	"errors"
	"testing"

	"github.com/Masterminds/semver/v3"
)

func withVers(t *testing.T, tempVers string, fn func()) {
	t.Helper()

	origVers := vers
	origSver := sver
	vers = tempVers
	sver = semver.MustParse(tempVers)
	defer func() {
		vers = origVers
		sver = origSver
	}()
	fn()
}

func TestSemVer(t *testing.T) {
	withVers(t, "v9.9.9", func() {
		got := SemVer()
		want := "v9.9.9"
		if got != want {
			t.Errorf("SemVer() = %q, want %q", got, want)
		}
	})
}

func TestAllowVersion_ExactMatch(t *testing.T) {
	withVers(t, "v1.2.3", func() {
		err := AllowVersion("=1.2.3")
		if err != nil {
			t.Errorf("AllowVersion(\"=1.2.3\") returned error: %v", err)
		}
	})
}

func TestAllowVersion_GreaterThan(t *testing.T) {
	withVers(t, "v2.0.0", func() {
		err := AllowVersion(">1.0.0")
		if err != nil {
			t.Errorf("AllowVersion(\">1.0.0\") returned error: %v", err)
		}
	})
}

func TestAllowVersion_LessThan(t *testing.T) {
	withVers(t, "v1.0.0", func() {
		err := AllowVersion("<2.0.0")
		if err != nil {
			t.Errorf("AllowVersion(\"<2.0.0\") returned error: %v", err)
		}
	})
}

func TestAllowVersion_NotAllowed(t *testing.T) {
	withVers(t, "v1.0.0", func() {
		err := AllowVersion(">2.0.0")
		var vErr ErrVersionNotAllowed
		if !errors.As(err, &vErr) {
			t.Errorf("AllowVersion(>2.0.0) error = %v, want ErrVersionNotAllowed", err)
		}
		if vErr.Version != "v1.0.0" || vErr.Expression != ">2.0.0" {
			t.Errorf("ErrVersionNotAllowed fields = %+v, want Version=v1.0.0, Expression=>2.0.0", vErr)
		}
	})
}

func TestAllowVersion_BadExpression(t *testing.T) {
	withVers(t, "v1.0.0", func() {
		err := AllowVersion("not-a-valid-expression")
		if err == nil {
			t.Errorf("AllowVersion with bad expression should return error")
		}
	})
}

func TestErrVersionNotAllowed_Error(t *testing.T) {
	var (
		e      = ErrVersionNotAllowed{Version: "v1.2.3", Expression: "<1.0.0"}
		expect = "compiler version v1.2.3 does not satisfy version constraint: '<1.0.0'"
	)
	if e.Error() != expect {
		t.Errorf("ErrVersionNotAllowed.Error() = %q, want %q", e.Error(), expect)
	}

}
