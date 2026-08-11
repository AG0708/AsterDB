package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/anishathalye/porcupine"
)

func TestRegisterModelAcceptsOverlap(t *testing.T) {
	operations := []porcupine.Operation{
		{ClientId: 0, Input: registerInput{Write: true, Value: 9}, Call: 1, Output: true, Return: 4},
		{ClientId: 1, Input: registerInput{}, Call: 2, Output: int64(9), Return: 3},
	}
	if !porcupine.CheckOperations(registerModel(0), operations) {
		t.Fatal("legal overlapping register history was rejected")
	}
}

func writeHistory(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "history.json")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunReportsOkForLegalHistory(t *testing.T) {
	path := writeHistory(t, `{
  "schema": "asterdb.porcupine-register-history.v1",
  "initial": 0,
  "operations": [
    {"client": 0, "kind": "write", "value": 9, "output": true, "call": 1, "return": 2},
    {"client": 1, "kind": "read", "value": null, "output": 9, "call": 3, "return": 4}
  ]
}`)
	result, err := run(path)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Linearizable || result.Result != string(porcupine.Ok) {
		t.Fatalf("legal history result = %+v", result)
	}
}

func TestRunReportsIllegalForStaleRead(t *testing.T) {
	path := writeHistory(t, `{
  "schema": "asterdb.porcupine-register-history.v1",
  "initial": 0,
  "operations": [
    {"client": 0, "kind": "write", "value": 9, "output": true, "call": 1, "return": 2},
    {"client": 1, "kind": "read", "value": null, "output": 0, "call": 3, "return": 4}
  ]
}`)
	result, err := run(path)
	if err != nil {
		t.Fatal(err)
	}
	if result.Linearizable || result.Result != string(porcupine.Illegal) {
		t.Fatalf("illegal history result = %+v", result)
	}
}

func TestRegisterModelRejectsStaleRead(t *testing.T) {
	operations := []porcupine.Operation{
		{ClientId: 0, Input: registerInput{Write: true, Value: 9}, Call: 1, Output: true, Return: 2},
		{ClientId: 1, Input: registerInput{}, Call: 3, Output: int64(0), Return: 4},
	}
	if porcupine.CheckOperations(registerModel(0), operations) {
		t.Fatal("stale read after a completed write was accepted")
	}
}
