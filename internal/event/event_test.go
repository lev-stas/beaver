package event

import "testing"

const validEditPayload = `{
	"$schema": "/mediawiki/recentchange/1.0.0",
	"meta": {
		"domain": "en.wikipedia.org",
		"stream": "mediawiki.recentchange"
	},
	"id": 987654321,
	"type": "edit",
	"namespace": 0,
	"title": "Foo",
	"comment": "test edit",
	"timestamp": 1752148800,
	"user": "SomeUser",
	"bot": false,
	"length": {"old": 100, "new": 120},
	"revision": {"old": 111, "new": 112},
	"wiki": "enwiki"
}`

func TestParse_Valid(t *testing.T) {
	rc, err := Parse([]byte(validEditPayload))
	if err != nil {
		t.Fatalf("Parse returned unexpected error: %v", err)
	}

	if rc.ID != 987654321 {
		t.Errorf("ID = %d, want 987654321", rc.ID)
	}
	if rc.Type != "edit" {
		t.Errorf("Type = %q, want %q", rc.Type, "edit")
	}
	if rc.Title != "Foo" {
		t.Errorf("Title = %q, want %q", rc.Title, "Foo")
	}
	if rc.Wiki != "enwiki" {
		t.Errorf("Wiki = %q, want %q", rc.Wiki, "enwiki")
	}
	if rc.Revision.New != 112 {
		t.Errorf("Revision.New = %d, want 112", rc.Revision.New)
	}
	if string(rc.Raw) != validEditPayload {
		t.Errorf("Raw does not match original payload")
	}
}

func TestParse_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"malformed json", `{"type": "edit",`},
		{"missing id", `{"type": "edit", "wiki": "enwiki", "title": "Foo", "timestamp": 1752148800}`},
		{"missing type", `{"id": 1, "wiki": "enwiki", "title": "Foo", "timestamp": 1752148800}`},
		{"missing wiki", `{"id": 1, "type": "edit", "title": "Foo", "timestamp": 1752148800}`},
		{"missing title", `{"id": 1, "type": "edit", "wiki": "enwiki", "timestamp": 1752148800}`},
		{"missing timestamp", `{"id": 1, "type": "edit", "wiki": "enwiki", "title": "Foo"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := Parse([]byte(tt.payload)); err == nil {
				t.Errorf("Parse(%q) returned no error, want error", tt.payload)
			}
		})
	}
}
