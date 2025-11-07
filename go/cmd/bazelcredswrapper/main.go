// Binary bazelcredswrapper is used to authenticate using bazel style credentials helper with the remote-apis-sdks
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	log "github.com/golang/glog"
)

var (
	credsPath = flag.String("credentials_helper_path", "", "Path to the user's credentials helper binary.")
	uri       = flag.String("uri", "", "The URI of the credentials request.")
)

func main() {
	flag.Parse()
	var err error
	if *credsPath == "" {
		log.Errorf("No credentials helper path provided.")
		os.Exit(1)
	}
	uriObj := fmt.Sprintf(`{"uri":"%v"}`, *uri)
	cmd := exec.Command(*credsPath, "get")
	var stdin, stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	stdin.Write([]byte(uriObj))
	cmd.Stdin = &stdin
	err = cmd.Run()
	out := stdout.String()
	if stderr.String() != "" {
		log.Error(stderr.String())
	}
	if err != nil {
		log.Fatalf("Failed running the credentials helper: %v, with err: %v", *credsPath, err)
	}

	headers, expiry := parseCredsOut(out)
	// Bazel-style headers are of the form map[string][]string but we need them
	// to be of the form map[string]string to match PerRPC credentials
	hdrs := map[string]string{}
	for k, v := range headers {
		hdrs[k] = strings.Join(v, ",")
	}
	jsonHdrs, err := json.Marshal(hdrs)
	if err != nil {
		log.Error(err)
		os.Exit(1)
	}

	// Time format conversion note: Converting from time.RFC3339 to time.UnixDate is
	// problematic because RFC3339 is a standardized, unambiguous format with timezone
	// information (e.g., "2023-01-01T12:00:00Z"), while UnixDate is a human-readable
	// format without explicit timezone indicators (e.g., "Sun Jan 1 12:00:00 GMT 2023").
	// This conversion can lead to timezone ambiguity and parsing errors in systems
	// expecting RFC3339 format, potentially causing authentication failures.
	// Convert to UTC to avoid parsing errors when timezone can't be determined.
	loc, err := time.LoadLocation("UTC")
	if err != nil {
		log.Fatalf("Failed to load timezone data.")
	}
	// Convert expiry to GMT time
	expiry_utc := expiry.In(loc)

	fmt.Printf(`{"headers":%s, "token":"%s", "expiry":"%s"}`, jsonHdrs,
		"", expiry_utc.Format(time.UnixDate))
}

type CredshelperOut struct {
	Headers map[string][]string `json:"headers"`
	Expires string              `json:"expires"`
}

func parseCredsOut(out string) (map[string][]string, time.Time) {
	var credsOut CredshelperOut
	if err := json.Unmarshal([]byte(out), &credsOut); err != nil {
		log.Errorf("Error while decoding credshelper output: %v", err)
		os.Exit(1)
	}
	hdrs := credsOut.Headers
	var exp time.Time
	if credsOut.Expires != "" {
		expiry, err := time.Parse(time.RFC3339, credsOut.Expires)
		if err != nil {
			log.Errorf("Failed to parse creds expiry: %v", err)
			os.Exit(1)
		}
		exp = expiry
	}
	return hdrs, exp
}
