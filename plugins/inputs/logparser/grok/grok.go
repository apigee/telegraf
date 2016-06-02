package grok

import (
	"bufio"
	//"fmt"
	"strings"
	"time"

	"github.com/gobwas/glob"
	"github.com/vjeantet/grok"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
)

type Parser struct {
	Pattern           string
	CustomPatterns    string
	CustomPatternFile string

	TagKeys        []string
	tagKeys        glob.Glob
	FieldKeysStr   []string
	fieldKeysStr   glob.Glob
	FieldKeysInt   []string
	fieldKeysInt   glob.Glob
	FieldKeysFloat []string
	fieldKeysFloat glob.Glob

	g *grok.Grok
}

func (p *Parser) Compile() error {
	var err error
	p.g, err = grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true})
	if err != nil {
		return err
	}

	if p.CustomPatternFile != "" {
		if err = p.g.AddPatternsFromPath(p.CustomPatternFile); err != nil {
			return err
		}
	}

	if p.CustomPatterns != "" {
		if err = p.addCustomPatterns(); err != nil {
			return err
		}
	}

	if len(p.TagKeys) > 0 {
		if p.tagKeys, err = internal.CompileFilter(p.TagKeys); err != nil {
			return err
		}
	}

	if len(p.FieldKeysStr) > 0 {
		if p.fieldKeysStr, err = internal.CompileFilter(p.FieldKeysStr); err != nil {
			return err
		}
	}

	if len(p.FieldKeysInt) > 0 {
		if p.fieldKeysInt, err = internal.CompileFilter(p.FieldKeysInt); err != nil {
			return err
		}
	}

	if len(p.FieldKeysFloat) > 0 {
		if p.fieldKeysFloat, err = internal.CompileFilter(p.FieldKeysFloat); err != nil {
			return err
		}
	}

	return nil
}

func (p *Parser) ParseLine(line string) (telegraf.Metric, error) {
	values, err := p.g.Parse(p.Pattern, line)
	if err != nil {
		return nil, err
	}

	fields := make(map[string]interface{})
	tags := make(map[string]string)
	for k, v := range values {
		if k == "" || v == "" {
			continue
		}

		if p.tagKeys != nil {
			if p.tagKeys.Match(k) {
				tags[k] = v
				continue
			}
		}

		if p.fieldKeysInt != nil {
			if p.fieldKeysInt.Match(k) {
				fields[k] = v
			}
			continue
		}

		if p.fieldKeysFloat != nil {
			if p.fieldKeysFloat.Match(k) {
				fields[k] = v
			}
			continue
		}

		if p.fieldKeysStr != nil {
			if p.fieldKeysStr.Match(k) {
				fields[k] = v
			}
			continue
		}

		fields[k] = v
	}

	return telegraf.NewMetric("grok", tags, fields, time.Now())
}

func (p *Parser) addCustomPatterns() error {
	filePatterns := make(map[string]string)

	scanner := bufio.NewScanner(strings.NewReader(p.CustomPatterns))
	for scanner.Scan() {
		l := strings.TrimSpace(scanner.Text())
		if len(l) > 0 && l[0] != '#' {
			names := strings.SplitN(l, " ", 2)
			filePatterns[names[0]] = names[1]
		}
	}

	return p.g.AddPatternsFromMap(filePatterns)
}
