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
	return runHooks(app, reflect.ValueOf(out).Elem(), raw)
}

// hooked is, per row type, whether it or a row it includes is a Decoder.
var hooked sync.Map // reflect.Type → bool

var decoderType = reflect.TypeOf((*Decoder)(nil)).Elem()

func hasHooks(t reflect.Type) bool {
	if cached, ok := hooked.Load(t); ok {
		return cached.(bool)
	}
	hooked.Store(t, false) // a row type that includes itself
	result := reflect.PointerTo(t).Implements(decoderType)
	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField() && !result; i++ {
			if inner, ok := includedRow(t.Field(i).Type); ok {
				result = hasHooks(inner)
			}
		}
	}
	hooked.Store(t, result)
	return result
}

// includedRow is the row type of a relation field: *Row or []Row.
func includedRow(t reflect.Type) (reflect.Type, bool) {
	if (t.Kind() == reflect.Pointer || t.Kind() == reflect.Slice) && t.Elem().Kind() == reflect.Struct {
		return t.Elem(), true
	}
	return nil, false
}

// runHooks runs the Decoder hooks of the row v (read from raw) and of the
// rows it includes, innermost first.
func runHooks(app any, v reflect.Value, raw json.RawMessage) error {
	if v.Kind() != reflect.Struct || !hasHooks(v.Type()) {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return err
	}
	t := v.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		inner, ok := includedRow(f.Type)
		if !ok || !f.IsExported() || !hasHooks(inner) {
			continue
		}
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" {
			name = f.Name
		}
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			continue
		}
		field := v.Field(i)
		switch f.Type.Kind() {
		case reflect.Pointer:
			if !field.IsNil() {
				if err := runHooks(app, field.Elem(), value); err != nil {
					return err
				}
			}
		case reflect.Slice:
			var items []json.RawMessage
			if err := json.Unmarshal(value, &items); err != nil {
				return err
			}
			for j := 0; j < field.Len() && j < len(items); j++ {
				if err := runHooks(app, field.Index(j), items[j]); err != nil {
					return err
				}
			}
		}
	}
	if d, ok := v.Addr().Interface().(Decoder); ok {
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
		Added       []json.RawMessage `json:"added"`
		Updated     []json.RawMessage `json:"updated"`
		Removed     []string          `json:"removed"`
		RemovedKeys []string          `json:"removedKeys"`
		Reset       bool              `json:"reset"`
		Coalesced   bool              `json:"coalesced"`
	}
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return Delta[R]{}, err
	}
	d := Delta[R]{Removed: raw.Removed, Reset: raw.Reset, Coalesced: raw.Coalesced,
		AddedKeys: resultKeys(raw.Added), UpdatedKeys: resultKeys(raw.Updated),
		RemovedKeys: removedKeys(raw.Removed, raw.RemovedKeys)}
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

// resultKey is a delta row's result key: its "$key", or its id from a
// libjazz_ffi without result keys.
func resultKey(row json.RawMessage) string {
	var keys struct {
		Key string `json:"$key"`
		ID  string `json:"id"`
	}
	_ = json.Unmarshal(row, &keys)
	if keys.Key != "" {
		return keys.Key
	}
	return keys.ID
}

func resultKeys(rows []json.RawMessage) []string {
	keys := make([]string, len(rows))
	for i, row := range rows {
		keys[i] = resultKey(row)
	}
	return keys
}

// removedKeys is a delta's "removedKeys", or its removed ids from a
// libjazz_ffi without result keys.
func removedKeys(removed, keys []string) []string {
	if len(keys) == len(removed) {
		return keys
	}
	return removed
}
