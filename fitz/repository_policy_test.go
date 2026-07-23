package fitz_test

import (
	"errors"
	"io/fs"
	"os"
	"testing"
)

func TestShouldKeepOneOffAutomationOutOfTopLevelScriptsDirectory(t *testing.T) {
	_, err := os.Stat("../scripts")
	if err == nil {
		t.Fatal("top-level scripts directory is not allowed; use Go tests or explicit workflow steps")
	}
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("inspect top-level scripts directory: %v", err)
	}
}
