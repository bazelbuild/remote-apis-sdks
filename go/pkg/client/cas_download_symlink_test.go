package client_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/remote-apis-sdks/go/pkg/fakes"
	"github.com/bazelbuild/remote-apis-sdks/go/pkg/filemetadata"
	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
)

func TestDownloadActionOutputsDoesNotFollowExistingSymlinks(t *testing.T) {
	tests := []struct {
		name       string
		outputPath string
	}{
		{
			// outDir/
			//   └── link ──(symlink)──> victimDir/
			//         └── victim-file
			name:       "intermediate directory symlink",
			outputPath: filepath.Join("link", "victim-file"),
		},
		{
			// outDir/
			//   └── link ──(symlink)──> victimDir/victim-file
			name:       "output file symlink",
			outputPath: "link",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, mode := range []string{"single", "batch"} {
				t.Run(mode, func(t *testing.T) {
					ctx := context.Background()
					e, cleanup := fakes.NewTestEnv(t)
					defer cleanup()
					c := e.Client.GrpcClient
					cache := filemetadata.NewSingleFlightCache()

					payload1 := []byte("remote-controlled")
					payloadDigest1 := e.Server.CAS.Put(payload1)
					payload2 := []byte("regular-content")
					payloadDigest2 := e.Server.CAS.Put(payload2)

					sandbox := t.TempDir()
					outDir := filepath.Join(sandbox, "out")
					victimDir := filepath.Join(sandbox, "victim")
					if err := os.MkdirAll(outDir, os.ModePerm); err != nil {
						t.Fatal(err)
					}
					if err := os.MkdirAll(victimDir, os.ModePerm); err != nil {
						t.Fatal(err)
					}
					victimFile := filepath.Join(victimDir, "victim-file")
					if err := os.WriteFile(victimFile, []byte("preserve"), 0600); err != nil {
						t.Fatal(err)
					}

					linkPath := tc.outputPath
					linkTarget := victimFile
					if dir := filepath.Dir(tc.outputPath); dir != "." {
						linkPath = dir
						linkTarget = victimDir
					}
					if err := os.Symlink(linkTarget, filepath.Join(outDir, linkPath)); err != nil {
						t.Fatal(err)
					}

					ar := &repb.ActionResult{
						OutputFiles: []*repb.OutputFile{
							{
								Path:   tc.outputPath,
								Digest: payloadDigest1.ToProto(),
							},
						},
					}
					if mode == "batch" {
						ar.OutputFiles = append(ar.OutputFiles, &repb.OutputFile{
							Path:   "valid-file",
							Digest: payloadDigest2.ToProto(),
						})
					}

					_, downloadErr := c.DownloadActionOutputs(ctx, ar, outDir, cache)
					if downloadErr == nil {
						t.Fatalf("DownloadActionOutputs accepted symlink escape")
					}

					victimContents, err := os.ReadFile(victimFile)
					if err != nil {
						t.Fatal(err)
					}
					if string(victimContents) != "preserve" {
						t.Fatalf("remote output overwrote file outside outDir through symlink: %q", victimContents)
					}
				})
			}
		})
	}
}
