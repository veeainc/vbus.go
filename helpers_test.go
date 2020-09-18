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

func funcWithArray(arr JsonByteArray, segments []string) int {
	return 42
}

func TestInvoke_array(t *testing.T) {
	// create args from a Json string
	argsJson := `[[1,2,3]]`
	var args []interface{}
	_ = json.Unmarshal([]byte(argsJson), &args)

	ret, err := invokeFunc(funcWithArray, append(args, []string{"foo", "bar"})...)
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


func TestFromVbus(t *testing.T) {
	data := []byte{}
	_, err := fromVbus(data)
	assert.NilError(t, err)
}

func TestJsonByteArray_MarshalJSON(t *testing.T) {
	array := JsonByteArray{1, 2, 3}
	str := "[1,2,3]"

	data, _ := array.MarshalJSON()
	assert.Equal(t, string(data), str)
}

func TestSanitizeNatsSegment(t *testing.T) {
	assert.Equal(t, sanitizeNatsSegment("veea.local"), "veea_local")
	assert.Equal(t, sanitizeNatsSegment("boolangery-ThinkPad-P1-Gen-2"), "boolangery-ThinkPad-P1-Gen-2")
}