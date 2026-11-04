package llmcache

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"
)

const KeyVersion = 1

// Key is the privacy-safe identity of a request. CanonicalBody is retained in
// memory for callers that need it; SQLite stores only RequestSHA256 by default.
type Key struct {
	CacheKey        string
	KeyVersion      int
	Method          string
	Path            string
	Model           string
	RequestSHA256   string
	CanonicalBody   []byte
	SemanticHeaders map[string]string
}

// CanonicalizeJSON returns a deterministic RFC 8785-style JSON representation.
// It rejects malformed JSON, duplicate object names, and invalid UTF-8.
func CanonicalizeJSON(input []byte) ([]byte, error) {
	p := jsonParser{data: input}
	v, err := p.parseValue()
	if err != nil {
		return nil, err
	}
	p.ws()
	if p.pos != len(p.data) {
		return nil, fmt.Errorf("json: trailing data at byte %d", p.pos)
	}
	return v.encode(), nil
}

// NewKey creates the versioned SHA-256 key used by the cache.
func NewKey(method, rawPath string, headers http.Header, body []byte) (Key, error) {
	canonical, err := CanonicalizeJSON(body)
	if err != nil {
		return Key{}, err
	}
	method = strings.ToUpper(strings.TrimSpace(method))
	if method == "" {
		return Key{}, errors.New("empty HTTP method")
	}
	normalizedPath, err := normalizePathQuery(rawPath)
	if err != nil {
		return Key{}, err
	}
	semantic := semanticHeaders(headers)
	model := extractModel(canonical)
	bodySum := sha256.Sum256(canonical)
	bodyHash := fmt.Sprintf("%x", bodySum[:])
	var b bytes.Buffer
	for _, field := range []string{"llm-cache-key-v1", method, normalizedPath, encodeHeaders(semantic), string(canonical)} {
		writeField(&b, []byte(field))
	}
	sum := sha256.Sum256(b.Bytes())
	return Key{CacheKey: fmt.Sprintf("%x", sum[:]), KeyVersion: KeyVersion, Method: method,
		Path: normalizedPath, Model: model, RequestSHA256: bodyHash, CanonicalBody: canonical,
		SemanticHeaders: semantic}, nil
}

func writeField(w io.Writer, data []byte) {
	var n [8]byte
	binary.BigEndian.PutUint64(n[:], uint64(len(data)))
	_, _ = w.Write(n[:])
	_, _ = w.Write(data)
}

func encodeHeaders(headers map[string]string) string {
	keys := make([]string, 0, len(headers))
	for k := range headers {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(headers[k])
		b.WriteByte('\n')
	}
	return b.String()
}

func semanticHeaders(h http.Header) map[string]string {
	out := make(map[string]string)
	if values, ok := h["Content-Type"]; ok && len(values) != 0 {
		out["content-type"] = normalizeMediaType(values[0])
	} else if value := h.Get("Content-Type"); value != "" {
		out["content-type"] = normalizeMediaType(value)
	}
	if values, ok := h["Accept"]; ok && len(values) != 0 {
		out["accept"] = normalizeHeaderWhitespace(values[0])
	} else if value := h.Get("Accept"); value != "" {
		out["accept"] = normalizeHeaderWhitespace(value)
	}
	return out
}

func normalizeMediaType(value string) string {
	parts := strings.Split(value, ";")
	typePart := strings.ToLower(strings.TrimSpace(parts[0]))
	return typePart
}

func normalizeHeaderWhitespace(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func normalizePathQuery(raw string) (string, error) {
	if raw == "" {
		raw = "/"
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	p := u.EscapedPath()
	if p == "" {
		p = "/"
	}
	decoded, err := url.PathUnescape(p)
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(decoded, "/") {
		decoded = "/" + decoded
	}
	decoded = path.Clean(decoded)
	if decoded == "." {
		decoded = "/"
	}
	query, err := canonicalQuery(u.RawQuery)
	if err != nil {
		return "", err
	}
	if query != "" {
		return decoded + "?" + query, nil
	}
	return decoded, nil
}

type queryPair struct{ key, value string }

func canonicalQuery(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	var pairs []queryPair
	for _, item := range strings.Split(raw, "&") {
		parts := strings.SplitN(item, "=", 2)
		key, err := percentDecode(parts[0])
		if err != nil {
			return "", err
		}
		value := ""
		if len(parts) == 2 {
			value, err = percentDecode(parts[1])
			if err != nil {
				return "", err
			}
		}
		pairs = append(pairs, queryPair{key, value})
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].key == pairs[j].key {
			return pairs[i].value < pairs[j].value
		}
		return pairs[i].key < pairs[j].key
	})
	parts := make([]string, len(pairs))
	for i, pair := range pairs {
		parts[i] = percentEncode(pair.key) + "=" + percentEncode(pair.value)
	}
	return strings.Join(parts, "&"), nil
}

func percentDecode(s string) (string, error) {
	buf := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '%' {
			buf = append(buf, s[i])
			continue
		}
		if i+2 >= len(s) {
			return "", fmt.Errorf("query: malformed escape")
		}
		hi, ok1 := hex(s[i+1])
		lo, ok2 := hex(s[i+2])
		if !ok1 || !ok2 {
			return "", fmt.Errorf("query: malformed escape")
		}
		buf = append(buf, hi<<4|lo)
		i += 2
	}
	if !utf8.Valid(buf) {
		return "", fmt.Errorf("query: invalid UTF-8")
	}
	return string(buf), nil
}

func hex(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func percentEncode(s string) string {
	const digits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || strings.ContainsRune("-._~", rune(c)) {
			b.WriteByte(c)
		} else {
			b.WriteByte('%')
			b.WriteByte(digits[c>>4])
			b.WriteByte(digits[c&15])
		}
	}
	return b.String()
}

func extractModel(canonical []byte) string {
	p := jsonParser{data: canonical}
	v, err := p.parseValue()
	if err != nil || v.kind != objectValue {
		return ""
	}
	for _, member := range v.object {
		if member.name == "model" && member.value.kind == stringValue {
			return member.value.str
		}
	}
	return ""
}

type jsonKind byte

const (
	nullValue jsonKind = iota
	boolValue
	stringValue
	numberValue
	arrayValue
	objectValue
)

type jsonMember struct {
	name  string
	value *jsonValue
}
type jsonValue struct {
	kind        jsonKind
	boolean     bool
	str, number string
	array       []*jsonValue
	object      []jsonMember
}

func (v *jsonValue) encode() []byte {
	switch v.kind {
	case nullValue:
		return []byte("null")
	case boolValue:
		if v.boolean {
			return []byte("true")
		}
		return []byte("false")
	case stringValue:
		return []byte(quoteJSON(v.str))
	case numberValue:
		return []byte(canonicalNumber(v.number))
	case arrayValue:
		var b strings.Builder
		b.WriteByte('[')
		for i, item := range v.array {
			if i > 0 {
				b.WriteByte(',')
			}
			b.Write(item.encode())
		}
		b.WriteByte(']')
		return []byte(b.String())
	case objectValue:
		members := append([]jsonMember(nil), v.object...)
		sort.SliceStable(members, func(i, j int) bool { return utf16Less(members[i].name, members[j].name) })
		var b strings.Builder
		b.WriteByte('{')
		for i, member := range members {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(quoteJSON(member.name))
			b.WriteByte(':')
			b.Write(member.value.encode())
		}
		b.WriteByte('}')
		return []byte(b.String())
	}
	return nil
}

func utf16Less(a, b string) bool {
	aa, bb := []uint16{}, []uint16{}
	for _, r := range a {
		if r <= 0xffff {
			aa = append(aa, uint16(r))
		} else {
			r -= 0x10000
			aa = append(aa, uint16(0xd800+(r>>10)), uint16(0xdc00+(r&0x3ff)))
		}
	}
	for _, r := range b {
		if r <= 0xffff {
			bb = append(bb, uint16(r))
		} else {
			r -= 0x10000
			bb = append(bb, uint16(0xd800+(r>>10)), uint16(0xdc00+(r&0x3ff)))
		}
	}
	for i := 0; i < len(aa) && i < len(bb); i++ {
		if aa[i] != bb[i] {
			return aa[i] < bb[i]
		}
	}
	return len(aa) < len(bb)
}

func quoteJSON(s string) string {
	if !utf8.ValidString(s) {
		return `"\ufffd"`
	}
	var b strings.Builder
	b.WriteByte('"')
	for _, r := range s {
		switch r {
		case '"':
			b.WriteString(`\"`)
		case '\\':
			b.WriteString(`\\`)
		case '\b':
			b.WriteString(`\b`)
		case '\f':
			b.WriteString(`\f`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			if r < 0x20 {
				b.WriteString(fmt.Sprintf(`\u%04x`, r))
			} else {
				b.WriteRune(r)
			}
		}
	}
	b.WriteByte('"')
	return b.String()
}

func canonicalNumber(raw string) string {
	negative := raw[0] == '-'
	if negative {
		raw = raw[1:]
	}
	parts := strings.SplitN(raw, "e", 2)
	if len(parts) == 1 {
		parts = strings.SplitN(raw, "E", 2)
	}
	exponent := 0
	if len(parts) == 2 {
		exponent, _ = strconv.Atoi(parts[1])
	}
	mantissa := parts[0]
	dot := strings.IndexByte(mantissa, '.')
	intPart := mantissa
	frac := ""
	if dot >= 0 {
		intPart, frac = mantissa[:dot], mantissa[dot+1:]
	}
	digits := intPart + frac
	leading := len(digits) - len(strings.TrimLeft(digits, "0"))
	digits = strings.TrimLeft(digits, "0")
	decimalPos := len(intPart) + exponent - leading
	if digits == "" {
		return "0"
	}
	for len(digits) > 1 && digits[len(digits)-1] == '0' {
		digits = digits[:len(digits)-1]
	}
	exp10 := decimalPos - 1
	var out string
	if exp10 >= -6 && exp10 < 21 {
		switch {
		case decimalPos <= 0:
			out = "0." + strings.Repeat("0", -decimalPos) + digits
		case decimalPos >= len(digits):
			out = digits + strings.Repeat("0", decimalPos-len(digits))
		default:
			out = digits[:decimalPos] + "." + digits[decimalPos:]
		}
	} else {
		out = digits[:1]
		if len(digits) > 1 {
			out += "." + digits[1:]
		}
		sign := "+"
		if exp10 < 0 {
			sign = ""
		}
		out += "e" + sign + strconv.Itoa(exp10)
	}
	if negative {
		return "-" + out
	}
	return out
}

type jsonParser struct {
	data []byte
	pos  int
}

func (p *jsonParser) ws() {
	for p.pos < len(p.data) && (p.data[p.pos] == ' ' || p.data[p.pos] == '\n' || p.data[p.pos] == '\r' || p.data[p.pos] == '\t') {
		p.pos++
	}
}
func (p *jsonParser) parseValue() (*jsonValue, error) {
	p.ws()
	if p.pos >= len(p.data) {
		return nil, errors.New("json: empty input")
	}
	switch p.data[p.pos] {
	case 'n':
		return p.literal("null", &jsonValue{kind: nullValue})
	case 't':
		return p.literal("true", &jsonValue{kind: boolValue, boolean: true})
	case 'f':
		return p.literal("false", &jsonValue{kind: boolValue})
	case '"':
		s, err := p.parseString()
		return &jsonValue{kind: stringValue, str: s}, err
	case '[':
		return p.parseArray()
	case '{':
		return p.parseObject()
	default:
		if p.data[p.pos] == '-' || (p.data[p.pos] >= '0' && p.data[p.pos] <= '9') {
			return p.parseNumber()
		}
		return nil, fmt.Errorf("json: unexpected byte %q at %d", p.data[p.pos], p.pos)
	}
}
func (p *jsonParser) literal(text string, v *jsonValue) (*jsonValue, error) {
	if p.pos+len(text) > len(p.data) || string(p.data[p.pos:p.pos+len(text)]) != text {
		return nil, fmt.Errorf("json: invalid literal")
	}
	p.pos += len(text)
	return v, nil
}
func (p *jsonParser) parseString() (string, error) {
	start := p.pos
	p.pos++
	escaped := false
	for p.pos < len(p.data) {
		c := p.data[p.pos]
		if c < 0x20 {
			return "", fmt.Errorf("json: control character in string")
		}
		if !escaped && c == '"' {
			p.pos++
			raw := p.data[start:p.pos]
			var s string
			if err := unmarshalJSONString(raw, &s); err != nil {
				return "", err
			}
			if !utf8.ValidString(s) {
				return "", errors.New("json: invalid UTF-8")
			}
			return s, nil
		}
		if !escaped && c == '\\' {
			escaped = true
		} else {
			escaped = false
		}
		p.pos++
	}
	return "", errors.New("json: unterminated string")
}
func unmarshalJSONString(raw []byte, target *string) error {
	if len(raw) < 2 {
		return errors.New("json: invalid string")
	}
	var b strings.Builder
	for i := 1; i < len(raw)-1; i++ {
		c := raw[i]
		if c != '\\' {
			b.WriteByte(c)
			continue
		}
		i++
		if i >= len(raw)-1 {
			return errors.New("json: invalid escape")
		}
		switch raw[i] {
		case '"', '\\', '/':
			b.WriteByte(raw[i])
		case 'b':
			b.WriteByte('\b')
		case 'f':
			b.WriteByte('\f')
		case 'n':
			b.WriteByte('\n')
		case 'r':
			b.WriteByte('\r')
		case 't':
			b.WriteByte('\t')
		case 'u':
			if i+4 >= len(raw) {
				return errors.New("json: invalid unicode escape")
			}
			n := 0
			for j := 1; j <= 4; j++ {
				x, ok := hex(raw[i+j])
				if !ok {
					return errors.New("json: invalid unicode escape")
				}
				n = n*16 + int(x)
			}
			i += 4
			r := rune(n)
			if r >= 0xd800 && r <= 0xdbff {
				if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
					return errors.New("json: invalid surrogate")
				}
				n2 := 0
				for j := 3; j <= 6; j++ {
					x, ok := hex(raw[i+j])
					if !ok {
						return errors.New("json: invalid surrogate")
					}
					n2 = n2*16 + int(x)
				}
				i += 6
				if n2 < 0xdc00 || n2 > 0xdfff {
					return errors.New("json: invalid surrogate")
				}
				r = 0x10000 + (rune(n)-0xd800)*0x400 + (rune(n2) - 0xdc00)
			} else if r >= 0xdc00 && r <= 0xdfff {
				return errors.New("json: invalid surrogate")
			}
			b.WriteRune(r)
		default:
			return errors.New("json: invalid escape")
		}
	}
	*target = b.String()
	return nil
}
func (p *jsonParser) parseNumber() (*jsonValue, error) {
	start := p.pos
	if p.pos < len(p.data) && p.data[p.pos] == '-' {
		p.pos++
	}
	if p.pos >= len(p.data) {
		return nil, errors.New("json: invalid number")
	}
	if p.data[p.pos] == '0' {
		p.pos++
	} else if p.data[p.pos] >= '1' && p.data[p.pos] <= '9' {
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
	} else {
		return nil, errors.New("json: invalid number")
	}
	if p.pos < len(p.data) && p.data[p.pos] == '.' {
		p.pos++
		begin := p.pos
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
		if begin == p.pos {
			return nil, errors.New("json: invalid number")
		}
	}
	if p.pos < len(p.data) && (p.data[p.pos] == 'e' || p.data[p.pos] == 'E') {
		p.pos++
		if p.pos < len(p.data) && (p.data[p.pos] == '+' || p.data[p.pos] == '-') {
			p.pos++
		}
		begin := p.pos
		for p.pos < len(p.data) && p.data[p.pos] >= '0' && p.data[p.pos] <= '9' {
			p.pos++
		}
		if begin == p.pos {
			return nil, errors.New("json: invalid exponent")
		}
	}
	return &jsonValue{kind: numberValue, number: string(p.data[start:p.pos])}, nil
}
func (p *jsonParser) parseArray() (*jsonValue, error) {
	p.pos++
	out := &jsonValue{kind: arrayValue}
	p.ws()
	if p.pos < len(p.data) && p.data[p.pos] == ']' {
		p.pos++
		return out, nil
	}
	for {
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out.array = append(out.array, v)
		p.ws()
		if p.pos >= len(p.data) {
			return nil, errors.New("json: unterminated array")
		}
		if p.data[p.pos] == ']' {
			p.pos++
			return out, nil
		}
		if p.data[p.pos] != ',' {
			return nil, errors.New("json: expected comma")
		}
		p.pos++
		p.ws()
		if p.pos < len(p.data) && p.data[p.pos] == ']' {
			return nil, errors.New("json: trailing comma")
		}
	}
}
func (p *jsonParser) parseObject() (*jsonValue, error) {
	p.pos++
	out := &jsonValue{kind: objectValue}
	seen := map[string]bool{}
	p.ws()
	if p.pos < len(p.data) && p.data[p.pos] == '}' {
		p.pos++
		return out, nil
	}
	for {
		p.ws()
		if p.pos >= len(p.data) || p.data[p.pos] != '"' {
			return nil, errors.New("json: expected object name")
		}
		name, err := p.parseString()
		if err != nil {
			return nil, err
		}
		if seen[name] {
			return nil, fmt.Errorf("json: duplicate member %q", name)
		}
		seen[name] = true
		p.ws()
		if p.pos >= len(p.data) || p.data[p.pos] != ':' {
			return nil, errors.New("json: expected colon")
		}
		p.pos++
		v, err := p.parseValue()
		if err != nil {
			return nil, err
		}
		out.object = append(out.object, jsonMember{name, v})
		p.ws()
		if p.pos >= len(p.data) {
			return nil, errors.New("json: unterminated object")
		}
		if p.data[p.pos] == '}' {
			p.pos++
			return out, nil
		}
		if p.data[p.pos] != ',' {
			return nil, errors.New("json: expected comma")
		}
		p.pos++
		p.ws()
		if p.pos < len(p.data) && p.data[p.pos] == '}' {
			return nil, errors.New("json: trailing comma")
		}
	}
}
