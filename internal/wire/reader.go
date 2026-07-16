package wire

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

// endCommand terminates the raw output of a legacy command frame.
var endCommand = []byte("--END COMMAND--")

// A Reader parses the inbound AMI byte stream into messages under the
// configured limits. It is not safe for concurrent use; the connection
// layer owns exactly one Reader per connection and enforces read
// deadlines on the underlying stream.
type Reader struct {
	br  *bufio.Reader
	lim Limits
	buf []byte // line accumulation buffer, reused across reads
}

// NewReader returns a Reader parsing from r under lim. The caller is
// responsible for validating lim; a non-positive limit fails closed.
func NewReader(r io.Reader, lim Limits) *Reader {
	return &Reader{br: bufio.NewReader(r), lim: lim}
}

// ReadBanner reads the protocol banner line the server sends before its
// first message and returns the line content without its terminator. The
// banner is not interpreted; the root package treats it as diagnostic
// data. ReadBanner returns io.EOF when the stream ends before any banner
// byte and io.ErrUnexpectedEOF when it ends mid-line.
func (r *Reader) ReadBanner() (string, error) {
	line, _, err := r.readLine(r.lim.MaxBannerBytes, ErrBannerTooLong)
	if err != nil {
		return "", err
	}
	return string(line), nil
}

// ReadMessage reads one complete message and returns its fields in wire
// order, duplicate keys included. It returns io.EOF when the stream ends
// cleanly at a message boundary and io.ErrUnexpectedEOF when it ends
// inside a message. Any non-nil error means subsequent framing cannot be
// trusted.
func (r *Reader) ReadMessage() ([]Field, error) {
	line, raw, err := r.readLine(r.lim.MaxLineBytes, ErrLineTooLong)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, ErrEmptyMessage
	}
	key, value, err := splitField(line)
	if err != nil {
		return nil, err
	}
	m := msg{lim: r.lim}
	if err := m.add(key, value, raw); err != nil {
		return nil, err
	}
	if strings.EqualFold(key, "Response") && strings.EqualFold(value, "Follows") {
		return r.readLegacyCommand(&m)
	}
	for {
		line, raw, err := r.readLine(r.lim.MaxLineBytes, ErrLineTooLong)
		if err != nil {
			return nil, noEOF(err)
		}
		if len(line) == 0 {
			if err := charge(&m.msgBytes, raw, r.lim.MaxMessageBytes, ErrMessageTooLarge); err != nil {
				return nil, err
			}
			return m.fields, nil
		}
		key, value, err := splitField(line)
		if err != nil {
			return nil, err
		}
		if err := m.add(key, value, raw); err != nil {
			return nil, err
		}
	}
}

// readLegacyCommand finishes a message whose first field was
// "Response: Follows": the legacy Command output framing of Asterisk
// 12–14.1. Optional Privilege and ActionID trailer headers are followed
// by raw command output — whose lines may be empty, contain colons, or
// look like headers — terminated by "--END COMMAND--" and the
// message-ending blank line. Output lines are normalized into
// synthesized "Output" fields. A payload line that merely ends with the
// terminator also terminates output, with its prefix preserved as the
// final output line: CLI output lacking a trailing newline glues the
// terminator onto its last line, and treating it as payload would stall
// the frame until a limit fails it.
func (r *Reader) readLegacyCommand(m *msg) ([]Field, error) {
	headers := true
	for {
		line, raw, err := r.readLine(r.lim.MaxLineBytes, ErrLineTooLong)
		if err != nil {
			return nil, noEOF(err)
		}
		if headers {
			if key, value, err := splitField(line); err == nil &&
				(strings.EqualFold(key, "Privilege") || strings.EqualFold(key, "ActionID")) {
				if err := m.addHeader(key, value, raw); err != nil {
					return nil, err
				}
				continue
			}
			headers = false
		}
		rest, terminated := bytes.CutSuffix(line, endCommand)
		if !terminated {
			if err := m.addOutput("Output", string(line), raw); err != nil {
				return nil, err
			}
			continue
		}
		if len(rest) > 0 {
			if err := m.addOutput("Output", string(rest), raw); err != nil {
				return nil, err
			}
		} else if err := charge(&m.msgBytes, raw, r.lim.MaxMessageBytes, ErrMessageTooLarge); err != nil {
			return nil, err
		}
		line, raw, err = r.readLine(r.lim.MaxLineBytes, ErrLineTooLong)
		if err != nil {
			return nil, noEOF(err)
		}
		if len(line) != 0 {
			return nil, ErrCommandFraming
		}
		if err := charge(&m.msgBytes, raw, r.lim.MaxMessageBytes, ErrMessageTooLarge); err != nil {
			return nil, err
		}
		return m.fields, nil
	}
}

// readLine reads one line terminated by \n, tolerating both \r\n and
// bare \n, and returns the line content without its terminator along
// with the raw byte count consumed, terminator included. Content longer
// than max fails with errTooLong. A stream ending before any byte
// returns io.EOF; one ending mid-line returns io.ErrUnexpectedEOF. The
// returned slice aliases the Reader's internal buffer and is valid only
// until the next read.
func (r *Reader) readLine(max int, errTooLong error) ([]byte, int, error) {
	r.buf = r.buf[:0]
	for {
		frag, err := r.br.ReadSlice('\n')
		r.buf = append(r.buf, frag...)
		if err == bufio.ErrBufferFull {
			// No terminator yet: bound the accumulation before reading
			// more. The content may still lose a trailing \r to the
			// terminator trim, hence the +1.
			if len(r.buf) > max+1 {
				return nil, 0, errTooLong
			}
			continue
		}
		if err == io.EOF {
			if len(r.buf) == 0 {
				return nil, 0, io.EOF
			}
			return nil, 0, io.ErrUnexpectedEOF
		}
		if err != nil {
			return nil, 0, err
		}
		raw := len(r.buf)
		line := bytes.TrimSuffix(r.buf[:raw-1], []byte("\r"))
		if len(line) > max {
			return nil, 0, errTooLong
		}
		return line, raw, nil
	}
}

// splitField splits one field line at its first colon, consuming exactly
// one optional space after the colon; everything else, further leading
// whitespace included, is preserved verbatim. A line with no colon or an
// empty key is not a field.
func splitField(line []byte) (key, value string, err error) {
	i := bytes.IndexByte(line, ':')
	if i <= 0 {
		return "", "", ErrMalformedLine
	}
	rest := line[i+1:]
	if len(rest) > 0 && rest[0] == ' ' {
		rest = rest[1:]
	}
	return string(line[:i]), string(rest), nil
}

// noEOF maps io.EOF to io.ErrUnexpectedEOF for reads inside a message,
// where a clean stream end is impossible.
func noEOF(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

// msg accumulates one message's fields and its split budgets: ordinary
// lines charge the per-message field and byte limits, while command
// output — synthesized legacy payload or modern repeated Output headers
// — charges the command-output line and byte limits.
type msg struct {
	lim      Limits
	fields   []Field
	nFields  int
	msgBytes int
	nOutput  int
	outBytes int
}

// add routes one parsed field to the budget its key selects.
func (m *msg) add(key, value string, raw int) error {
	if strings.EqualFold(key, "Output") {
		return m.addOutput(key, value, raw)
	}
	return m.addHeader(key, value, raw)
}

func (m *msg) addHeader(key, value string, raw int) error {
	m.nFields++
	if m.nFields > m.lim.MaxFields {
		return ErrTooManyFields
	}
	if err := charge(&m.msgBytes, raw, m.lim.MaxMessageBytes, ErrMessageTooLarge); err != nil {
		return err
	}
	m.fields = append(m.fields, Field{Key: key, Value: value})
	return nil
}

func (m *msg) addOutput(key, value string, raw int) error {
	m.nOutput++
	if m.nOutput > m.lim.MaxCommandOutputLines {
		return ErrTooManyOutputLines
	}
	if err := charge(&m.outBytes, raw, m.lim.MaxCommandOutputBytes, ErrOutputTooLarge); err != nil {
		return err
	}
	m.fields = append(m.fields, Field{Key: key, Value: value})
	return nil
}

// charge adds raw to *total and fails with over when the budget is
// exceeded; a total of exactly max is within budget.
func charge(total *int, raw, max int, over error) error {
	*total += raw
	if *total > max {
		return over
	}
	return nil
}
