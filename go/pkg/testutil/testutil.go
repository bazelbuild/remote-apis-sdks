package testutil

import (
	"os"
	"testing"
)

func CreateFile(t *testing.T, executable bool, contents string) (string, error) {
	t.Helper()
	perm := os.FileMode(0666)
	if executable {
		perm = os.FileMode(0766)
	}
	tmpFile, err := os.CreateTemp(t.TempDir(), "")
	if err != nil {
		return "", err
	}
	if err := tmpFile.Chmod(perm); err != nil {
		return "", err
	}
	if err := tmpFile.Close(); err != nil {
		return "", err
	}
	filename := tmpFile.Name()
	if err = os.WriteFile(filename, []byte(contents), os.ModeTemporary); err != nil {
		return "", err
	}
	return filename, nil
}
