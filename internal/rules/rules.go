// Package rules reads alert rules from YAML and checks them against the
// metrics stream.
//
// A rule's condition compares an expression with a number, for example:
//
//	abs(zscore(last_price, lookback=30)) > 4
//	median_ratio(volume, lookback=60) > 5
//	abs(xex_spread_bps) > 25
//	seconds_since_last_trade > 15
//
// Only a few functions are supported (abs, zscore, median_ratio). That covers
// every rule we need without writing a full expression language.
package rules

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"

	"github.com/anibitri/tickstream/internal/domain"
)

// File is the YAML document format (see deploy/rules.yaml).
type File struct {
	// ExpectedExchanges lets stale_feed detect an exchange that stops sending
	// before it has ever produced a trade for a symbol.
	ExpectedExchanges []string   `yaml:"expected_exchanges"`
	Rules             []RuleSpec `yaml:"rules"`
}

// RuleSpec is one rule as written in YAML.
type RuleSpec struct {
	Name         string `yaml:"name"`
	WindowSecs   int32  `yaml:"window_secs"`
	Condition    string `yaml:"condition"`
	Severity     string `yaml:"severity"`
	CooldownSecs int    `yaml:"cooldown_secs"`
}

// Rule is a compiled rule.
type Rule struct {
	Name       string
	WindowSecs int32
	Cond       Condition
	Severity   domain.Severity
	Cooldown   time.Duration
	PerExch    bool    // evaluated once per exchange (seconds_since_last_trade)
	stateful   []*Expr // nodes with history buffers
}

// WithThreshold returns a copy of r with a different threshold (backtest tuning).
func (r *Rule) WithThreshold(th float64) *Rule {
	c := *r
	c.Cond.Threshold = th
	return &c
}

// Set is a validated rule set.
type Set struct {
	Rules             []*Rule
	ExpectedExchanges []string
}

// Load reads and compiles a rules YAML file.
func Load(path string) (*Set, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(b)
}

// Parse compiles a rules YAML document.
func Parse(b []byte) (*Set, error) {
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}
	if len(f.Rules) == 0 {
		return nil, errors.New("rules file defines no rules")
	}
	set := &Set{ExpectedExchanges: f.ExpectedExchanges}
	names := map[string]bool{}
	for i, rs := range f.Rules {
		r, err := compile(rs)
		if err != nil {
			return nil, fmt.Errorf("rule %d (%s): %w", i, rs.Name, err)
		}
		if names[r.Name] {
			return nil, fmt.Errorf("duplicate rule name %q", r.Name)
		}
		names[r.Name] = true
		set.Rules = append(set.Rules, r)
	}
	return set, nil
}

func compile(rs RuleSpec) (*Rule, error) {
	if rs.Name == "" || !isIdent(rs.Name) {
		return nil, fmt.Errorf("name %q must be an identifier", rs.Name)
	}
	cond, err := ParseCondition(rs.Condition)
	if err != nil {
		return nil, err
	}
	sev, err := domain.ParseSeverity(rs.Severity)
	if err != nil {
		return nil, err
	}
	if rs.CooldownSecs < 0 {
		return nil, fmt.Errorf("cooldown_secs must be >= 0")
	}
	r := &Rule{
		Name: rs.Name, WindowSecs: rs.WindowSecs, Cond: cond, Severity: sev,
		Cooldown: time.Duration(rs.CooldownSecs) * time.Second,
		PerExch:  cond.Expr.usesField(FieldSecondsSinceLastTrade),
	}
	if r.WindowSecs == 0 {
		r.WindowSecs = 1 // rules without a window evaluate on the finest windows
	}
	if r.WindowSecs < 0 {
		return nil, fmt.Errorf("window_secs must be positive")
	}
	cond.Expr.walk(func(e *Expr) {
		if e.Func == "zscore" || e.Func == "median_ratio" {
			e.slot = len(r.stateful)
			r.stateful = append(r.stateful, e)
		}
	})
	return r, nil
}

// Fields that an expression may reference.
var metricFields = map[string]bool{
	"last_price": true, "vwap": true, "volume": true, "trade_count": true, "realised_vol": true,
	"xex_spread_bps": true, "high": true, "low": true, "range_bps": true,
	FieldSecondsSinceLastTrade: true,
}

// FieldSecondsSinceLastTrade is evaluated per exchange.
const FieldSecondsSinceLastTrade = "seconds_since_last_trade"

// Op is a comparison operator.
type Op string

const (
	OpGT Op = ">"
	OpGE Op = ">="
	OpLT Op = "<"
	OpLE Op = "<="
)

func (o Op) compare(a, b float64) bool {
	switch o {
	case OpGT:
		return a > b
	case OpGE:
		return a >= b
	case OpLT:
		return a < b
	case OpLE:
		return a <= b
	}
	return false
}

// Expr is a parsed expression node.
type Expr struct {
	Func     string // "" for a bare field; "abs", "zscore", "median_ratio"
	Field    string // for bare fields and zscore/median_ratio
	Arg      *Expr  // for abs
	Lookback int    // for zscore/median_ratio
	slot     int    // index of this node's history buffer (stateful nodes)
}

func (e *Expr) String() string {
	switch e.Func {
	case "":
		return e.Field
	case "abs":
		return "abs(" + e.Arg.String() + ")"
	default:
		return fmt.Sprintf("%s(%s, lookback=%d)", e.Func, e.Field, e.Lookback)
	}
}

// Condition is `expr op threshold`.
type Condition struct {
	Expr      *Expr
	Op        Op
	Threshold float64
}

func (c Condition) String() string {
	return fmt.Sprintf("%s %s %s", c.Expr, c.Op, strconv.FormatFloat(c.Threshold, 'g', -1, 64))
}

// ParseCondition parses a condition string.
func ParseCondition(s string) (Condition, error) {
	p := &parser{toks: tokenize(s)}
	expr, err := p.expr()
	if err != nil {
		return Condition{}, fmt.Errorf("condition %q: %w", s, err)
	}
	opTok := p.next()
	op := Op(opTok)
	switch op {
	case OpGT, OpGE, OpLT, OpLE:
	default:
		return Condition{}, fmt.Errorf("condition %q: expected comparison operator, got %q", s, opTok)
	}
	numTok := p.next()
	th, err := strconv.ParseFloat(numTok, 64)
	if err != nil {
		return Condition{}, fmt.Errorf("condition %q: expected number after %s, got %q", s, op, numTok)
	}
	if rest := p.next(); rest != "" {
		return Condition{}, fmt.Errorf("condition %q: unexpected %q", s, rest)
	}
	return Condition{Expr: expr, Op: op, Threshold: th}, nil
}

type parser struct {
	toks []string
	pos  int
}

func (p *parser) peek() string {
	if p.pos < len(p.toks) {
		return p.toks[p.pos]
	}
	return ""
}

func (p *parser) next() string {
	t := p.peek()
	if t != "" {
		p.pos++
	}
	return t
}

func (p *parser) expect(tok string) error {
	if got := p.next(); got != tok {
		return fmt.Errorf("expected %q, got %q", tok, got)
	}
	return nil
}

func (p *parser) expr() (*Expr, error) {
	name := p.next()
	if name == "" || !isIdent(name) {
		return nil, fmt.Errorf("expected field or function, got %q", name)
	}
	if p.peek() != "(" {
		if !metricFields[name] {
			return nil, fmt.Errorf("unknown field %q", name)
		}
		return &Expr{Field: name}, nil
	}
	p.next() // (
	switch name {
	case "abs":
		arg, err := p.expr()
		if err != nil {
			return nil, err
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		return &Expr{Func: "abs", Arg: arg}, nil
	case "zscore", "median_ratio":
		field := p.next()
		if !metricFields[field] || field == FieldSecondsSinceLastTrade {
			return nil, fmt.Errorf("%s: unsupported field %q", name, field)
		}
		e := &Expr{Func: name, Field: field, Lookback: 30}
		for p.peek() == "," {
			p.next()
			key := p.next()
			if err := p.expect("="); err != nil {
				return nil, err
			}
			val := p.next()
			if key != "lookback" {
				return nil, fmt.Errorf("%s: unknown argument %q", name, key)
			}
			n, err := strconv.Atoi(val)
			if err != nil || n < 2 || n > 10_000 {
				return nil, fmt.Errorf("%s: lookback must be an integer in [2, 10000], got %q", name, val)
			}
			e.Lookback = n
		}
		if err := p.expect(")"); err != nil {
			return nil, err
		}
		return e, nil
	}
	return nil, fmt.Errorf("unknown function %q", name)
}

func isIdent(s string) bool {
	for i, r := range s {
		if !(r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r))) {
			return false
		}
	}
	return true
}

func tokenize(s string) []string {
	var toks []string
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t':
			i++
		case strings.ContainsRune("(),=", rune(c)):
			toks = append(toks, string(c))
			i++
		case c == '>' || c == '<':
			if i+1 < len(s) && s[i+1] == '=' {
				toks = append(toks, s[i:i+2])
				i += 2
			} else {
				toks = append(toks, string(c))
				i++
			}
		default:
			j := i
			for j < len(s) && !strings.ContainsRune(" \t(),=<>", rune(s[j])) {
				j++
			}
			toks = append(toks, s[i:j])
			i = j
		}
	}
	return toks
}

// walk visits e and its children.
func (e *Expr) walk(fn func(*Expr)) {
	fn(e)
	if e.Arg != nil {
		e.Arg.walk(fn)
	}
}

// usesField reports whether the expression references field.
func (e *Expr) usesField(field string) bool {
	found := false
	e.walk(func(x *Expr) {
		if x.Field == field {
			found = true
		}
	})
	return found
}
