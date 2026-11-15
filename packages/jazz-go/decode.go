package jazz

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// required is, per row type, the JSON names of its fields that can't hold
// null: everything but pointers, slices, maps and interfaces.
var required sync.Map // reflect.Type → []string

func requiredFields(t reflect.Type) []string {
	if cached, ok := required.Load(t); ok {
		return cached.([]string)
	}
	var names []string
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
			if !f.IsExported() || name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			switch f.Type.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Map, reflect.Interface:
			default:
				names = append(names, name)
			}
		}
	}
	required.Store(t, names)
	return names
}

// decodeRow decodes a row of table into out. A null in a field that can't
// hold one (a non-finite double, say, which JSON carries as null) is
// refused, rather than read as the zero value.
func decodeRow[R any](app any, table string, raw json.RawMessage, out *R) error {
	if names := requiredFields(reflect.TypeOf(out).Elem()); len(names) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		for _, name := range names {
			if value, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return &Error{Kind: KindRefused, Table: table, Column: name, Message: fmt.Sprintf(
					"%s.%s is null, which its Go field can't hold (a non-finite double reads as null)", table, name)}
			}
		}
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return err
	}
	if d, ok := any(out).(Decoder); ok {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		return d.JazzDecode(app, fields)
	}
	return nil
}

func decodeRows[R any](app any, table string, raws []json.RawMessage) ([]R, error) {
	rows := make([]R, len(raws))
	for i, raw := range raws {
		if err := decodeRow(app, table, raw, &rows[i]); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func decodeDelta[R any](app any, table, text string) (Delta[R], error) {
	var raw struct {
		Added     []json.RawMessage `json:"added"`
		Updated   []json.RawMessage `json:"updated"`
		Removed   []string          `json:"removed"`
		Reset     bool              `json:"reset"`
		Coalesced bool              `json:"coalesced"`
	}
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return Delta[R]{}, err
	}
	d := Delta[R]{Removed: raw.Removed, Reset: raw.Reset, Coalesced: raw.Coalesced}
	var err error
	if d.Added, err = decodeRows[R](app, table, raw.Added); err != nil {
		return Delta[R]{}, err
	}
	if d.Updated, err = decodeRows[R](app, table, raw.Updated); err != nil {
		return Delta[R]{}, err
	}
	return d, nil
}

// Decoder is a generated row that converts after decoding: its
// transformed columns' views and its computed fields, from the app's
// Computed.
type Decoder interface {
	JazzDecode(app any, fields map[string]json.RawMessage) error
}

// Encoder is a generated insert or patch that converts before writing:
// its transformed columns' stored values.
type Encoder interface {
	JazzEncode(app any) (any, error)
}

// ErrNoComputed: the app declares computed fields or transforms, but was
// opened without its Computed (open it with jazzapp.Open).
var ErrNoComputed = &Error{Kind: KindRefused, Message: "the app's computed fields and transforms need its Computed: open it with jazzapp.Open(ctx, cfg, computed)"}

func encode(q Querier, value any) (any, error) {
	if e, ok := value.(Encoder); ok {
		return e.JazzEncode(q.computed())
	}
	return value, nil
}

// Fields is v's JSON object, as fields to add to: what generated
// encoders start from.
func Fields(v any) (map[string]any, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(b, &raw); err != nil {
		return nil, err
	}
	out := make(map[string]any, len(raw))
	for k, v := range raw {
		out[k] = v
	}
	return out, nil
}
