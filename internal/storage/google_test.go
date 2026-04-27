package storage

import "testing"

func TestDriveQueryLiteralEscapesQuotesAndBackslashes(t *testing.T) {
	got := driveQueryLiteral(`a\b'c`)
	want := `'a\\b\'c'`
	if got != want {
		t.Fatalf("driveQueryLiteral() = %q, want %q", got, want)
	}
}

func TestDriveListQueryTermAvoidsHyphenatedPrefixSearch(t *testing.T) {
	tests := map[string]string{
		"req-":          "req",
		"res-dev1-mux-": "dev1",
		"plain":         "plain",
	}

	for prefix, want := range tests {
		if got := driveListQueryTerm(prefix); got != want {
			t.Fatalf("driveListQueryTerm(%q) = %q, want %q", prefix, got, want)
		}
	}
}
