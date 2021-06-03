// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package filter

import (
	. "github.com/pingcap/check"
)

func cloneTables(tbs []*Table) []*Table {
	if tbs == nil {
		return nil
	}
	newTbs := make([]*Table, 0, len(tbs))
	for _, tb := range tbs {
		newTbs = append(newTbs, tb.Clone())
	}
	return newTbs
}

func (s *testFilterSuite) TestFilterOnSchema(c *C) {
	cases := []struct {
		rules         *Rules
		Input         []*Table
		Output        []*Table
		caseSensitive bool
	}{
		// empty rules
		{
			rules: &Rules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  nil,
			Output: nil,
		},
		{
			rules: &Rules{
				IgnoreDBs: nil,
				DoDBs:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		// schema-only rules
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}, {"foo1", "bar"}, {"foo1", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}, {"foo1", "bar"}, {"foo1", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		// DoTable rules(Without regex)
		{
			rules: &Rules{
				DoTables: []*Table{{"foo", "bar1"}},
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
			Output: []*Table{{"foo", "bar1"}, {"foo", ""}},
		},
		// ignoreTable rules(Without regex)
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"foo", "bar"}},
				DoTables:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
			Output: []*Table{{"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
		},
		{
			// all regexp
			rules: &Rules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*Table{{"~^foo", "~^sbtest-\\d"}},
			},
			Input:  []*Table{{"foo", "sbtest"}, {"foo1", "sbtest-1"}, {"foo2", ""}, {"fff", "bar"}},
			Output: []*Table{{"foo", "sbtest"}, {"foo2", ""}},
		},
		// test rule with * or ?
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*Table{{"foor", "a"}, {"foo[bar]", "b"}, {"fo", "c"}, {"foo?", "d"}, {"special\\", "e"}},
			Output: []*Table{{"foo[bar]", "b"}, {"fo", "c"}},
		},
		// ensure non case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{"~.*", "~FoO$"}},
			},
			Input:  []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoO"}, {"Foo4", "dfoo"}, {"5", "5"}},
			Output: []*Table{{"5", "5"}},
		},
		// ensure case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{"~.*", "~FoO$"}},
			},
			Input:         []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoo"}, {"Foo4", "dfoo"}, {"5", "5"}},
			Output:        []*Table{{"foo2", "b"}, {"BoO3", "cFoo"}, {"Foo4", "dfoo"}, {"5", "5"}},
			caseSensitive: true,
		},
		// test the rule whose schema part is not regex and the table part is regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"a?b?", "~f[0-9]"}},
			},
			Input:  []*Table{{"abbd", "f1"}, {"aaaa", "f2"}, {"5", "5"}, {"abbc", "fa"}},
			Output: []*Table{{"aaaa", "f2"}, {"5", "5"}, {"abbc", "fa"}},
		},
		// test the rule whose schema part is regex and the table part is not regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"~t[0-8]", "a??"}},
			},
			Input:  []*Table{{"t1", "a01"}, {"t9", "a02"}, {"5", "5"}, {"t9", "a001"}},
			Output: []*Table{{"t9", "a02"}, {"5", "5"}, {"t9", "a001"}},
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"a*", "A*"}},
			},
			Input:         []*Table{{"aB", "Ab"}, {"AaB", "aab"}, {"acB", "Afb"}},
			Output:        []*Table{{"AaB", "aab"}},
			caseSensitive: true,
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"a*", "A*"}},
			},
			Input:  []*Table{{"aB", "Ab"}, {"AaB", "aab"}, {"acB", "Afb"}},
			Output: []*Table(nil),
		},
	}

	for _, t := range cases {
		ft, err := New(t.caseSensitive, t.rules)
		c.Assert(err, IsNil)
		originInput := cloneTables(t.Input)
		got := ft.ApplyOn(t.Input)
		c.Logf("got %+v, expected %+v", got, t.Output)
		c.Assert(originInput, DeepEquals, t.Input)
		c.Assert(got, DeepEquals, t.Output)
	}
}

func (s *testFilterSuite) TestCaseSensitiveApply(c *C) {
	cases := []struct {
		rules         *Rules
		Input         []*Table
		Output        []*Table
		caseSensitive bool
	}{
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo"},
				DoDBs:     []string{"foo"},
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}, {"foo1", "bar"}, {"foo1", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo1"},
				DoDBs:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", ""}, {"foo1", "bar"}, {"foo1", ""}},
			Output: []*Table{{"foo", "bar"}, {"foo", ""}},
		},
		// ignoreTable rules(Without regex)
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"Foo", "bAr"}},
				DoTables:     nil,
			},
			Input:  []*Table{{"foo", "bar"}, {"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
			Output: []*Table{{"foo", "bar1"}, {"foo", ""}, {"fff", "bar1"}},
		},
		{
			// all regexp
			rules: &Rules{
				IgnoreDBs:    nil,
				DoDBs:        []string{"~^foo"},
				IgnoreTables: []*Table{{"~^foo", "~^sbtest-\\d"}},
			},
			Input:  []*Table{{"foo", "sbtest"}, {"foo1", "sbtest-1"}, {"foo2", ""}, {"fff", "bar"}},
			Output: []*Table{{"foo", "sbtest"}, {"foo2", ""}},
		},
		// test rule with * or ?
		{
			rules: &Rules{
				IgnoreDBs: []string{"foo[bar]", "foo?", "special\\"},
			},
			Input:  []*Table{{"foor", "a"}, {"foo[bar]", "b"}, {"Fo", "c"}, {"foo?", "d"}, {"special\\", "e"}},
			Output: []*Table{{"foo[bar]", "b"}, {"Fo", "c"}},
		},
		// ensure non case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{"~.*", "~FoO$"}},
			},
			Input:  []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoO"}, {"Foo4", "dfoo"}, {"5", "5"}},
			Output: []*Table{{"5", "5"}},
		},
		// ensure case-insensitive
		{
			rules: &Rules{
				IgnoreDBs:    []string{"~^FOO"},
				IgnoreTables: []*Table{{"~.*", "~FoO$"}},
			},
			Input:         []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoo"}, {"Foo4", "dfoo"}, {"5", "5"}},
			Output:        []*Table{{"foo2", "b"}, {"BoO3", "cFoo"}, {"Foo4", "dfoo"}, {"5", "5"}},
			caseSensitive: true,
		},
		// test the rule whose schema part is not regex and the table part is regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"a?b?", "~f[0-9]"}},
			},
			Input:  []*Table{{"abBd", "f1"}, {"aAAa", "f2"}, {"5", "5"}, {"abbc", "FA"}},
			Output: []*Table{{"aAAa", "f2"}, {"5", "5"}, {"abbc", "FA"}},
		},
		// test the rule whose schema part is regex and the table part is not regex.
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"~t[0-8]", "A??"}},
			},
			Input:  []*Table{{"t1", "a01"}, {"t9", "A02"}, {"5", "5"}, {"T9", "a001"}},
			Output: []*Table{{"t9", "A02"}, {"5", "5"}, {"T9", "a001"}},
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"a*", "A*"}},
			},
			Input:         []*Table{{"aB", "Ab"}, {"AaB", "aab"}, {"acB", "Afb"}},
			Output:        []*Table{{"AaB", "aab"}},
			caseSensitive: true,
		},
		{
			rules: &Rules{
				IgnoreTables: []*Table{{"a*", "A*"}},
			},
			Input:  []*Table{{"aB", "Ab"}, {"AaB", "aab"}, {"acB", "Afb"}},
			Output: []*Table{},
		},
	}

	for _, t := range cases {
		ft, err := New(t.caseSensitive, t.rules)
		c.Assert(err, IsNil)
		originInput := cloneTables(t.Input)
		got := ft.Apply(t.Input)
		c.Logf("got %+v, expected %+v", got, t.Output)
		c.Assert(originInput, DeepEquals, t.Input)
		c.Assert(got, DeepEquals, t.Output)
	}
}

func (s *testFilterSuite) TestMaxBox(c *C) {
	rules := &Rules{
		DoTables: []*Table{
			{"test1", "t1"},
		},
		IgnoreTables: []*Table{
			{"test1", "t2"},
		},
	}

	r, err := New(false, rules)
	c.Assert(err, IsNil)

	x := &Table{"test1", ""}
	res := r.ApplyOn([]*Table{x})
	c.Assert(res, HasLen, 1)
	c.Assert(res[0], DeepEquals, x)
}

func (s *testFilterSuite) TestCaseSensitive(c *C) {
	// ensure case-sensitive rules are really case-sensitive
	rules := &Rules{
		IgnoreDBs:    []string{"~^FOO"},
		IgnoreTables: []*Table{{"~.*", "~FoO$"}},
	}
	r, err := New(true, rules)
	c.Assert(err, IsNil)

	input := []*Table{{"FOO1", "a"}, {"foo2", "b"}, {"BoO3", "cFoO"}, {"Foo4", "dfoo"}, {"5", "5"}}
	actual := r.ApplyOn(input)
	expected := []*Table{{"foo2", "b"}, {"Foo4", "dfoo"}, {"5", "5"}}
	c.Logf("got %+v, expected %+v", actual, expected)
	c.Assert(actual, DeepEquals, expected)

	inputTable := &Table{"FOO", "a"}
	c.Assert(r.Match(inputTable), IsFalse)

	rules = &Rules{
		DoDBs: []string{"BAR"},
	}

	r, err = New(false, rules)
	c.Assert(err, IsNil)
	inputTable = &Table{"bar", "a"}
	c.Assert(r.Match(inputTable), IsTrue)

	c.Assert(err, IsNil)

	inputTable = &Table{"BAR", "a"}
	originInputTable := inputTable.Clone()
	c.Assert(r.Match(inputTable), IsTrue)
	c.Assert(originInputTable, DeepEquals, inputTable)
}

func (s *testFilterSuite) TestInvalidRegex(c *C) {
	cases := []struct {
		rules *Rules
	}{
		{
			rules: &Rules{
				DoDBs: []string{"~^t[0-9]+((?!_copy).)*$"},
			},
		},
		{
			rules: &Rules{
				DoDBs: []string{"~^t[0-9]+sp(?=copy).*"},
			},
		},
	}
	for _, tc := range cases {
		_, err := New(true, tc.rules)
		c.Assert(err, NotNil)
	}
}

func (s *testFilterSuite) TestMatchReturnsBool(c *C) {
	rules := &Rules{
		DoDBs: []string{"sns"},
	}
	f, err := New(true, rules)
	c.Assert(err, IsNil)
	c.Assert(f.Match(&Table{Schema: "sns"}), IsTrue)
	c.Assert(f.Match(&Table{Schema: "other"}), IsFalse)
	f, err = New(true, nil)
	c.Assert(err, IsNil)
	c.Assert(f.Match(&Table{Schema: "other"}), IsTrue)
}
