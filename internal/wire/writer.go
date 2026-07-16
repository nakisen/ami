package wire

import (
	"slices"
	"strings"
)

// AppendMessage validates fields under lim and appends their encoded
// wire form — one "Key: Value" line per field in order, CRLF
// terminators, and the message-ending blank line — to dst, returning the
// extended slice. Validation completes before any byte is appended: on
// error, dst is returned unchanged.
//
// Keys must be non-empty and free of colons, carriage returns, and line
// feeds; values must be free of carriage returns and line feeds. That is
// the full injection surface — semantic key hygiene belongs to the root
// package. A message whose first field encodes "Response: Follows"
// re-parses under the legacy Command output framing rather than as the
// encoded field sequence; synthesizing such a frame requires raw writes
// by design.
func AppendMessage(dst []byte, fields []Field, lim Limits) ([]byte, error) {
	if len(fields) == 0 {
		return dst, ErrEmptyMessage
	}
	if len(fields) > lim.MaxActionFields {
		return dst, ErrTooManyActionFields
	}
	total := 2 // the message-ending blank line
	for _, f := range fields {
		if f.Key == "" || strings.ContainsAny(f.Key, ":\r\n") {
			return dst, ErrInvalidKey
		}
		if strings.ContainsAny(f.Value, "\r\n") {
			return dst, ErrInvalidValue
		}
		n := len(f.Key) + len(": ") + len(f.Value)
		if n > lim.MaxActionLineBytes {
			return dst, ErrActionLineTooLong
		}
		total += n + 2
		if total > lim.MaxActionBytes {
			return dst, ErrActionTooLarge
		}
	}
	dst = slices.Grow(dst, total)
	for _, f := range fields {
		dst = append(dst, f.Key...)
		dst = append(dst, ": "...)
		dst = append(dst, f.Value...)
		dst = append(dst, "\r\n"...)
	}
	return append(dst, "\r\n"...), nil
}
