package logparser

import (
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/hpcloud/tail"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal/globpath"
	"github.com/influxdata/telegraf/plugins/inputs"

	// Parsers:
	"github.com/influxdata/telegraf/plugins/inputs/logparser/grok"
)

type LogParser interface {
	ParseLine(line string) (telegraf.Metric, error)
	Compile() error
}

type LogParserPlugin struct {
	Files         []string
	FromBeginning bool

	tailers []*tail.Tail
	wg      sync.WaitGroup
	acc     telegraf.Accumulator

	sync.Mutex

	// list of "active" log parsers
	parsers []LogParser

	GrokParser *grok.Parser `toml:"grok"`
}

func NewLogParserPlugin() *LogParserPlugin {
	return &LogParserPlugin{
		FromBeginning: false,
	}
}

const sampleConfig = `
  ## files to tail.
  ## These accept standard unix glob matching rules, but with the addition of
  ## ** as a "super asterisk". ie:
  ##   "/var/log/**.log"  -> recursively find all .log files in /var/log
  ##   "/var/log/*/*.log" -> find all .log files with a parent dir in /var/log
  ##   "/var/log/apache.log" -> just tail the apache log file
  ##
  ## See https://github.com/gobwas/glob for more examples
  ##
  files = ["/var/log/apache.log"]
  ## Read file from beginning.
  from_beginning = false

  ## For parsing logstash-style "grok" patterns:
  [inputs.logparser.grok]
    pattern = "%{}"
	custom_patterns = '''
      NGUSERNAME [a-zA-Z\.\@\-\+_%]+
      NGUSER %{NGUSERNAME}
      NGINXACCESS %{IPORHOST:clientip} %{NGUSER:ident} %{NGUSER:auth} \[%{HTTPDATE:timestamp}\] "%{WORD:verb} %{URIPATHPARAM:request} HTTP/%{NUMBER:httpversion}" %{NUMBER:response} (?:%{NUMBER:bytes}|-) (?:"(?:%{URI:referrer}|-)"|%{QS:referrer}) %{QS:agent}
	'''
`

func (l *LogParserPlugin) SampleConfig() string {
	return sampleConfig
}

func (l *LogParserPlugin) Description() string {
	return "Stream and parse log file(s)."
}

func (l *LogParserPlugin) Gather(acc telegraf.Accumulator) error {
	return nil
}

func (l *LogParserPlugin) Start(acc telegraf.Accumulator) error {
	l.Lock()
	defer l.Unlock()

	l.acc = acc

	// Looks for fields which implement LogParser interface
	l.parsers = make([]LogParser, 0)
	s := reflect.ValueOf(l).Elem()
	for i := 0; i < s.NumField(); i++ {
		f := s.Field(i)

		if !f.CanInterface() {
			continue
		}

		if lpPlugin, ok := f.Interface().(LogParser); ok {
			if reflect.ValueOf(lpPlugin).IsNil() {
				continue
			}
			l.parsers = append(l.parsers, lpPlugin)
		}
	}

	if len(l.parsers) == 0 {
		return fmt.Errorf("ERROR: logparser input plugin: no parsers defined.")
	}

	// compile all log parser patterns:
	for _, parser := range l.parsers {
		if err := parser.Compile(); err != nil {
			return err
		}
	}

	var seek tail.SeekInfo
	if !l.FromBeginning {
		seek.Whence = 2
		seek.Offset = 0
	}

	var errS string
	// Create a "tailer" for each file
	for _, filepath := range l.Files {
		g, err := globpath.Compile(filepath)
		if err != nil {
			log.Printf("ERROR Glob %s failed to compile, %s", filepath, err)
		}
		for file, _ := range g.Match() {
			tailer, err := tail.TailFile(file,
				tail.Config{
					ReOpen:   true,
					Follow:   true,
					Location: &seek,
				})
			if err != nil {
				errS += err.Error() + " "
				continue
			}
			// create a goroutine for each "tailer"
			l.wg.Add(1)
			go l.receiver(tailer)
			l.tailers = append(l.tailers, tailer)
		}
	}

	if errS != "" {
		return fmt.Errorf(errS)
	}
	return nil
}

// this is launched as a goroutine to continuously watch a tailed logfile
// for changes, parse any incoming msgs, and add to the accumulator.
func (l *LogParserPlugin) receiver(tailer *tail.Tail) {
	defer l.wg.Done()

	var m telegraf.Metric
	var err error
	var line *tail.Line
	for line = range tailer.Lines {
		if line.Err != nil {
			log.Printf("ERROR tailing file %s, Error: %s\n",
				tailer.Filename, err)
			continue
		}
		for _, parser := range l.parsers {
			m, err = parser.ParseLine(line.Text)
			if err == nil {
				l.acc.AddFields(m.Name(), m.Fields(), m.Tags(), m.Time())
			} else {
				log.Printf("Malformed log line in %s: [%s], Error: %s\n",
					tailer.Filename, line.Text, err)
			}
		}
	}
}

func (l *LogParserPlugin) Stop() {
	l.Lock()
	defer l.Unlock()

	for _, t := range l.tailers {
		err := t.Stop()
		if err != nil {
			log.Printf("ERROR stopping tail on file %s\n", t.Filename)
		}
		t.Cleanup()
	}
	l.wg.Wait()
}

func init() {
	inputs.Add("logparser", func() telegraf.Input {
		return NewLogParserPlugin()
	})
}
