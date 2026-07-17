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
	br    *bufio.Reader
	lim   Limits
	buf   []byte // line accumulation buffer, reused across reads
	dirty bool   // bytes of an unreturned frame have been consumed

	// onFrameStart fires on the reading goroutine when the first byte
	// of a new banner or message frame is consumed — the moment Dirty
	// transitions to true.
	onFrameStart func()
}

// NewReader returns a Reader parsing from r under lim. The caller is
// responsible for validating lim; a non-positive limit fails closed.
func NewReader(r io.Reader, lim Limits) *Reader {
	rd := &Reader{lim: lim}
	rd.br = bufio.NewReader(&frameStartReader{src: r, r: rd})
	return rd
}

// frameStart marks the first consumed byte of a new frame — at most
// once per frame — and fires the frame-start hook.
func (r *Reader) frameStart() {
	if r.dirty {
		return
	}
	r.dirty = true
	if r.onFrameStart != nil {
		r.onFrameStart()
	}
}

// frameStartReader taps the byte stream below the buffered reader: the
// moment any byte of a new frame arrives from the transport the frame
// clock starts, even when the line it belongs to never completes. Bytes
// already buffered from an earlier transport read are covered by the
// readLine entry check instead.
type frameStartReader struct {
	src io.Reader
	r   *Reader
}

func (f *frameStartReader) Read(p []byte) (int, error) {
	n, err := f.src.Read(p)
	if n > 0 {
		f.r.frameStart()
	}
	return n, err
}

// Dirty reports whether bytes of an unreturned banner or message have
// been consumed from the stream. After a read fails, a false Dirty means
// no byte of the frame was consumed — an interruption at that point (for
// example a poked read deadline) leaves the stream intact and a later
// read can resume cleanly. A true Dirty after a failure means the frame
// is partially consumed and framing cannot be resumed.
func (r *Reader) Dirty() bool { return r.dirty }

// SetFrameStartHook registers fn to run whenever the first byte of a
// new banner or message frame has been consumed, whether it arrived
// from the stream or was already buffered. The connection layer uses
// the hook to arm a partial-frame deadline that an idle stream — no
// pending frame — never starts. fn runs on the reading goroutine.
func (r *Reader) SetFrameStartHook(fn func()) { r.onFrameStart = fn }

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
	r.dirty = false
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
			r.dirty = false
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
		r.dirty = false
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
	if r.br.Buffered() > 0 {
		// Bytes of this frame already sit in the buffer from an earlier
		// transport read; consuming them starts the frame now. Bytes
		// arriving from the stream start it inside frameStartReader.
		r.frameStart()
	}
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
