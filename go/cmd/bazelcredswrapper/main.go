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
	defer log.Flush()
	flag.Parse()
	log.Flush()
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
	fmt.Printf(`{"headers":%s, "token":"%s", "expiry":"%s"}`, jsonHdrs,
		"", expiry.Format(time.UnixDate))
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
