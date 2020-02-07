package vBus

import (
	"encoding/json"
	"gotest.tools/assert"
	"testing"
)

func scan(time int, segments []string) int {
	return 42
}

func TestInvoke(t *testing.T) {
	// create args from a Json string
	argsJson := `[40]`
	var args []interface{}
	_ = json.Unmarshal([]byte(argsJson), &args)

	ret, err := invokeFunc(scan, append(args, []string{"foo", "bar"})...)
	assert.Equal(t, err, nil)
	assert.Equal(t, ret, 42)
}

func TestGetPathInObj(t *testing.T) {
	obj := JsonObj{
		"name": JsonObj{
			"foo": JsonObj{
				"bar": JsonObj{
					"found": true,
				},
			},
		},
	}
	found := getPathInObj(obj, "name", "foo", "bar")
	assert.Equal(t, true, hasKey(found, "found"))
}