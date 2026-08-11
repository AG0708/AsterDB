package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/anishathalye/porcupine"
)

type historyFile struct {
	Schema     string             `json:"schema"`
	Initial    int64              `json:"initial"`
	Operations []historyOperation `json:"operations"`
}

type historyOperation struct {
	Client int    `json:"client"`
	Kind   string `json:"kind"`
	Value  *int64 `json:"value"`
	Output any    `json:"output"`
	Call   int64  `json:"call"`
	Return int64  `json:"return"`
}

type registerInput struct {
	Write bool
	Value int64
}

type checkResult struct {
	Schema       string `json:"schema"`
	Linearizable bool   `json:"linearizable"`
	Result       string `json:"result"`
	Operations   int    `json:"operations"`
	Checker      string `json:"checker"`
	Version      string `json:"version"`
}

func registerModel(initial int64) porcupine.Model {
	return porcupine.Model{
		Init: func() any { return initial },
		Step: func(state, input, output any) (bool, any) {
			operation := input.(registerInput)
			if operation.Write {
				acknowledged, ok := output.(bool)
				return ok && acknowledged, operation.Value
			}
			observed, ok := output.(int64)
			return ok && observed == state.(int64), state
		},
		DescribeOperation: func(input, output any) string {
			operation := input.(registerInput)
			if operation.Write {
				return fmt.Sprintf("write(%d) -> %v", operation.Value, output)
			}
			return fmt.Sprintf("read() -> %v", output)
		},
		DescribeState: func(state any) string { return fmt.Sprint(state) },
	}
}

func decode(path string) (historyFile, []porcupine.Operation, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return historyFile{}, nil, err
	}
	var history historyFile
	if err := json.Unmarshal(encoded, &history); err != nil {
		return historyFile{}, nil, err
	}
	if history.Schema != "asterdb.porcupine-register-history.v1" {
		return historyFile{}, nil, fmt.Errorf("unsupported history schema %q", history.Schema)
	}
	operations := make([]porcupine.Operation, 0, len(history.Operations))
	for index, operation := range history.Operations {
		if operation.Call <= 0 || operation.Return <= operation.Call || operation.Client < 0 {
			return historyFile{}, nil, fmt.Errorf("operation %d has an invalid interval/client", index)
		}
		input := registerInput{}
		var output any
		switch operation.Kind {
		case "write":
			if operation.Value == nil {
				return historyFile{}, nil, fmt.Errorf("write %d has no value", index)
			}
			acknowledged, ok := operation.Output.(bool)
			if !ok {
				return historyFile{}, nil, fmt.Errorf("write %d has a non-boolean output", index)
			}
			input = registerInput{Write: true, Value: *operation.Value}
			output = acknowledged
		case "read":
			if operation.Value != nil {
				return historyFile{}, nil, fmt.Errorf("read %d unexpectedly has an input value", index)
			}
			observed, ok := operation.Output.(float64)
			if !ok || observed != float64(int64(observed)) {
				return historyFile{}, nil, fmt.Errorf("read %d has a non-integer output", index)
			}
			output = int64(observed)
		default:
			return historyFile{}, nil, fmt.Errorf("operation %d has unknown kind %q", index, operation.Kind)
		}
		operations = append(operations, porcupine.Operation{
			ClientId: operation.Client,
			Input:    input,
			Call:     operation.Call,
			Output:   output,
			Return:   operation.Return,
		})
	}
	return history, operations, nil
}

func run(path string) (checkResult, error) {
	history, operations, err := decode(path)
	if err != nil {
		return checkResult{}, err
	}
	checked := porcupine.CheckOperationsTimeout(
		registerModel(history.Initial),
		operations,
		30*time.Second,
	)
	return checkResult{
		Schema:       "asterdb.porcupine-result.v1",
		Linearizable: checked == porcupine.Ok,
		Result:       string(checked),
		Operations:   len(operations),
		Checker:      "github.com/anishathalye/porcupine",
		Version:      "v1.0.3",
	}, nil
}

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "usage: porcupine-check HISTORY.json")
		os.Exit(2)
	}
	result, err := run(os.Args[1])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(result); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if !result.Linearizable {
		os.Exit(1)
	}
}
