package jazzgo

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

type Value = json.RawMessage

type ColumnType struct {
	Type     string             `json:"type"`
	Variants []string           `json:"variants,omitempty"`
	Element  *ColumnType        `json:"element,omitempty"`
	Schema   json.RawMessage    `json:"schema,omitempty"`
	Columns  []ColumnDescriptor `json:"columns,omitempty"`
}

type ColumnDescriptor struct {
	Name          string          `json:"name"`
	ColumnType    ColumnType      `json:"column_type"`
	Nullable      bool            `json:"nullable"`
	Default       json.RawMessage `json:"default,omitempty"`
	References    string          `json:"references,omitempty"`
	MergeStrategy string          `json:"merge_strategy,omitempty"`
}

type TableSchema struct {
	Columns  []ColumnDescriptor `json:"columns"`
	Policies json.RawMessage    `json:"policies,omitempty"`
}

type Schema map[string]TableSchema

type runtimeSchemaEnvelope struct {
	RuntimeSchemaMarker int    `json:"__jazzRuntimeSchema"`
	Schema              Schema `json:"schema"`
}

func ParseSchemaJSON(data []byte) (Schema, error) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil, errors.New("empty schema json")
	}

	var envelope runtimeSchemaEnvelope
	if err := json.Unmarshal(trimmed, &envelope); err == nil && envelope.RuntimeSchemaMarker == 1 {
		if len(envelope.Schema) == 0 {
			return nil, errors.New("runtime schema envelope did not contain schema")
		}
		return envelope.Schema, nil
	}

	var schema Schema
	if err := json.Unmarshal(trimmed, &schema); err != nil {
		return nil, fmt.Errorf("parse schema json: %w", err)
	}
	if len(schema) == 0 {
		return nil, errors.New("schema did not contain any tables")
	}
	return schema, nil
}
