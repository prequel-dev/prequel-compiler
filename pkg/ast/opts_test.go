package ast

import (
	"errors"
	"testing"
)

func TestWithStrict(t *testing.T) {
	opt := parseOpts(WithStrict(true))
	if !opt.strict {
		t.Errorf("WithStrict(true) did not set strict to true")
	}
	opt = parseOpts(WithStrict(false))
	if opt.strict {
		t.Errorf("WithStrict(false) did not set strict to false")
	}
}

func TestWithMaxGen(t *testing.T) {
	opt := parseOpts(WithMaxGen(123))
	if opt.maxGen != 123 {
		t.Errorf("WithMaxGen(123) did not set maxGen to 123, got %d", opt.maxGen)
	}
}

func TestWithMaxRank(t *testing.T) {
	opt := parseOpts(WithMaxRank(456))
	if opt.maxRank != 456 {
		t.Errorf("WithMaxRank(456) did not set maxRank to 456, got %d", opt.maxRank)
	}
}

func TestWithMaxDepth(t *testing.T) {
	opt := parseOpts(WithMaxDepth(789))
	if opt.maxDepth != 789 {
		t.Errorf("WithMaxDepth(789) did not set maxDepth to 789, got %d", opt.maxDepth)
	}
}

func TestWithJQValidator(t *testing.T) {
	called := false
	validator := func(s string) error {
		called = true
		if s == "fail" {
			return errors.New("fail")
		}
		return nil
	}
	opt := parseOpts(WithJQValidator(validator))
	if err := opt.jqValidator("ok"); err != nil {
		t.Errorf("jqValidator returned error for 'ok': %v", err)
	}
	if !called {
		t.Errorf("jqValidator was not called")
	}
	called = false
	err := opt.jqValidator("fail")
	if err == nil || err.Error() != "fail" {
		t.Errorf("jqValidator did not return expected error for 'fail'")
	}
}

func TestWithLuaValidator(t *testing.T) {
	called := false
	validator := func(s string) error {
		called = true
		if s == "bad" {
			return errors.New("bad")
		}
		return nil
	}
	opt := parseOpts(WithLuaValidator(validator))
	if err := opt.luaValidator("ok"); err != nil {
		t.Errorf("luaValidator returned error for 'ok': %v", err)
	}
	if !called {
		t.Errorf("luaValidator was not called")
	}
	called = false
	err := opt.luaValidator("bad")
	if err == nil || err.Error() != "bad" {
		t.Errorf("luaValidator did not return expected error for 'bad'")
	}
}

func TestWithPromQLValidator(t *testing.T) {
	called := false
	validator := func(s string) error {
		called = true
		if s == "nope" {
			return errors.New("nope")
		}
		return nil
	}
	opt := parseOpts(WithPromQLValidator(validator))
	if err := opt.promQLValidator("ok"); err != nil {
		t.Errorf("promQLValidator returned error for 'ok': %v", err)
	}
	if !called {
		t.Errorf("promQLValidator was not called")
	}
	called = false
	err := opt.promQLValidator("nope")
	if err == nil || err.Error() != "nope" {
		t.Errorf("promQLValidator did not return expected error for 'nope'")
	}
}

func TestSelectValidator(t *testing.T) {
	// nil returns stubValidator
	v := selectValidator(nil)
	if v == nil {
		t.Errorf("selectValidator(nil) should not return nil")
	}
	if err := v("anything"); err != nil {
		t.Errorf("stubValidator should always return nil")
	}
	// non-nil returns the same
	called := false
	myValidator := func(string) error { called = true; return nil }
	v = selectValidator(myValidator)
	v("test")
	if !called {
		t.Errorf("selectValidator did not return the provided validator")
	}
}

func TestParseOpts_Defaults(t *testing.T) {
	opt := parseOpts()
	if opt.maxGen != defaultMaxGen {
		t.Errorf("default maxGen = %d, want %d", opt.maxGen, defaultMaxGen)
	}
	if opt.maxRank != defaultMaxRank {
		t.Errorf("default maxRank = %d, want %d", opt.maxRank, defaultMaxRank)
	}
	if opt.maxDepth != defaultMaxDepth {
		t.Errorf("default maxDepth = %d, want %d", opt.maxDepth, defaultMaxDepth)
	}
	if opt.strict {
		t.Errorf("default strict should be false")
	}
	if opt.jqValidator == nil || opt.luaValidator == nil || opt.promQLValidator == nil {
		t.Errorf("default validators should not be nil")
	}
}
