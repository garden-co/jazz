package jazzgo

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
)

type Client struct {
	baseURL    string
	appID      string
	httpClient *http.Client
}

type HealthResponse struct {
	OK     bool   `json:"ok"`
	Status string `json:"status"`
}

type SchemaHashesResponse struct {
	Hashes []string `json:"hashes"`
}

type StoredSchemaResponse struct {
	Schema      Schema  `json:"schema"`
	PublishedAt *uint64 `json:"publishedAt,omitempty"`
}

func NewClient(baseURL, appID string) *Client {
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		appID:      appID,
		httpClient: http.DefaultClient,
	}
}

func (c *Client) AppWSURL() string {
	base := c.baseURL
	switch {
	case strings.HasPrefix(base, "https://"):
		base = "wss://" + strings.TrimPrefix(base, "https://")
	case strings.HasPrefix(base, "http://"):
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return base + "/apps/" + url.PathEscape(c.appID) + "/ws"
}

func (c *Client) Health(ctx context.Context) (HealthResponse, error) {
	var resp HealthResponse
	err := c.getJSON(ctx, "/health", &resp)
	return resp, err
}

func (c *Client) SchemaHashes(ctx context.Context) (SchemaHashesResponse, error) {
	var resp SchemaHashesResponse
	err := c.getJSON(ctx, "/apps/"+url.PathEscape(c.appID)+"/schemas", &resp)
	return resp, err
}

func (c *Client) SchemaByHash(ctx context.Context, hash string) (StoredSchemaResponse, error) {
	var resp StoredSchemaResponse
	err := c.getJSON(ctx, "/apps/"+url.PathEscape(c.appID)+"/schema/"+url.PathEscape(hash), &resp)
	return resp, err
}

func (c *Client) getJSON(ctx context.Context, path string, target any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		return err
	}
	res, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("jazz2 request %s failed: %s", path, res.Status)
	}
	return json.NewDecoder(res.Body).Decode(target)
}
