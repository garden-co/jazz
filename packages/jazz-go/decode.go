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
func decodeRow[R any](table string, raw json.RawMessage, out *R) error {
	if names := requiredFields(reflect.TypeOf(out).Elem()); len(names) > 0 {
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			return err
		}
		for _, name := range names {
			if value, ok := fields[name]; ok && bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				return &Error{Kind: "refused", Message: fmt.Sprintf(
					"%s.%s is null, which its Go field can't hold (a non-finite double reads as null)", table, name)}
			}
		}
	}
	return json.Unmarshal(raw, out)
}

func decodeRows[R any](table string, raws []json.RawMessage) ([]R, error) {
	rows := make([]R, len(raws))
	for i, raw := range raws {
		if err := decodeRow(table, raw, &rows[i]); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

func decodeDelta[R any](table, text string) (Delta[R], error) {
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
	if d.Added, err = decodeRows[R](table, raw.Added); err != nil {
		return Delta[R]{}, err
	}
	if d.Updated, err = decodeRows[R](table, raw.Updated); err != nil {
		return Delta[R]{}, err
	}
	return d, nil
}
