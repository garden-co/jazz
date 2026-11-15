package jazz

import (
	"encoding/json"
	"strconv"
	"time"
)

// BigInt is a `bigint` column's value. Jazz carries it as a decimal
// string, so it survives JSON parsers that read numbers as doubles.
type BigInt int64

// MarshalJSON writes the decimal string.
func (b BigInt) MarshalJSON() ([]byte, error) {
	return json.Marshal(strconv.FormatInt(int64(b), 10))
}

// UnmarshalJSON reads a decimal string (or a plain number).
func (b *BigInt) UnmarshalJSON(data []byte) error {
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		var n int64
		if err := json.Unmarshal(data, &n); err != nil {
			return err
		}
		*b = BigInt(n)
		return nil
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return err
	}
	*b = BigInt(n)
	return nil
}

// Timestamp is a `timestamp` column's value: milliseconds since the Unix
// epoch, as Jazz carries it.
type Timestamp int64

// TimestampOf is t as a Timestamp.
func TimestampOf(t time.Time) Timestamp { return Timestamp(t.UnixMilli()) }

// Now is the current time as a Timestamp.
func Now() Timestamp { return TimestampOf(time.Now()) }

// Time is ts as a time.Time.
func (ts Timestamp) Time() time.Time { return time.UnixMilli(int64(ts)) }

// Ptr is a pointer to v: for the nullable fields of rows and inserts.
func Ptr[T any](v T) *T { return &v }

// Patch is one field of a generated patch struct: left as it is unless
// Set. For a nullable column (T a pointer), Set(nil) sets it to null.
type Patch[T any] struct {
	value T
	set   bool
}

// Set is a patch field that sets the column to v.
func Set[T any](v T) Patch[T] { return Patch[T]{value: v, set: true} }

// IsSet reports whether the field changes the column.
func (p Patch[T]) IsSet() bool { return p.set }

// Value is what the field sets the column to.
func (p Patch[T]) Value() T { return p.value }

// PatchEntry is a patch field under its column's name: what generated
// MarshalJSON methods pass to MarshalPatch.
type PatchEntry struct {
	name  string
	value any
	set   bool
}

// Entry is the field as the column name's entry.
func (p Patch[T]) Entry(name string) PatchEntry {
	return PatchEntry{name: name, value: p.value, set: p.set}
}

// MarshalPatch writes the entries that are set as a JSON object.
func MarshalPatch(entries ...PatchEntry) ([]byte, error) {
	object := make(map[string]any, len(entries))
	for _, e := range entries {
		if e.set {
			object[e.name] = e.value
		}
	}
	return json.Marshal(object)
}

// PatchJSON is MarshalPatch's object, as a value to write.
func PatchJSON(entries ...PatchEntry) (any, error) {
	b, err := MarshalPatch(entries...)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// MarshalCase writes a payload enum's case as Jazz carries it:
// {"case": name, "fields": {…}}. Generated case structs use it.
func MarshalCase(name string, fields any) ([]byte, error) {
	return json.Marshal(struct {
		Case   string `json:"case"`
		Fields any    `json:"fields"`
	}{name, fields})
}

// DecodeCase reads a payload enum's {"case", "fields"}: its case ("" for
// null) and its fields (an empty object when it has none).
func DecodeCase(raw json.RawMessage) (string, json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil, nil
	}
	var v struct {
		Case   string          `json:"case"`
		Fields json.RawMessage `json:"fields"`
	}
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", nil, err
	}
	if len(v.Fields) == 0 || string(v.Fields) == "null" {
		v.Fields = json.RawMessage("{}")
	}
	return v.Case, v.Fields, nil
}

// UnknownCase is the error for a payload enum case the bindings don't
// know: regenerate them.
func UnknownCase(what, name string) error {
	return &Error{Kind: KindRefused, Message: what + " has no case " + name + ": regenerate the bindings"}
}
