package storage

import "testing"

func TestDriveQueryLiteralEscapesQuotesAndBackslashes(t *testing.T) {
	got := driveQueryLiteral(`a\b'c`)
	want := `'a\\b\'c'`
	if got != want {
		t.Fatalf("driveQueryLiteral() = %q, want %q", got, want)
	}
}
