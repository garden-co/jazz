package jazz

import "testing"

// Vectors from jazz-tools' own endpointScope + accountAppId.
func TestEndpointAppIDMatchesTheTypeScriptClient(t *testing.T) {
	for _, c := range [][2]string{
		{"http://127.0.0.1:8711/jazz", "4174c705-9ec7-542e-b1a5-9524c8f947ba"},
		{"https://app.example/jazz/", "9b632529-119f-59b2-859d-5c8446faaa00"},
		{"wss://App.Example:443/jazz", "9b632529-119f-59b2-859d-5c8446faaa00"},
		{"ws://localhost:3000/api/jazz//", "39bbab4a-eb75-5fd7-adbc-bd8fe8909454"},
		{"https://app.example", "aef742d9-b21a-5106-83c6-115c1efbbe8f"},
		{"http://[::1]:8080/jazz", "7850e17b-ed46-529d-8f4c-535a421cd31c"},
	} {
		got, err := EndpointAppID(c[0])
		if err != nil || got != c[1] {
			t.Errorf("%s: %s %v, want %s", c[0], got, err, c[1])
		}
	}
	for _, bad := range []string{"ftp://x/jazz", "/jazz", "not a url"} {
		if _, err := EndpointAppID(bad); err == nil {
			t.Errorf("%q: accepted", bad)
		}
	}
}
