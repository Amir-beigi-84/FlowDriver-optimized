package httpclient

import (
	"io"
	"net/http"
	"testing"
)

type captureRoundTripper struct {
	host string
}

func (rt *captureRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.host = req.Host
	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       io.NopCloser(http.NoBody),
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func TestHostRewriteTransportSetsHostHeader(t *testing.T) {
	capture := &captureRoundTripper{}
	rt := &hostRewriteTransport{
		Transport:  capture,
		HostHeader: "www.googleapis.com",
	}

	req, err := http.NewRequest(http.MethodGet, "https://example.invalid/drive/v3/files", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}

	resp, err := rt.RoundTrip(req)
	if err != nil {
		t.Fatalf("round trip: %v", err)
	}
	resp.Body.Close()

	if capture.host != "www.googleapis.com" {
		t.Fatalf("host = %q, want www.googleapis.com", capture.host)
	}
}
