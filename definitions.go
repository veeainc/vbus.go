// Node definitions are used to hold user data like the json structure, callbacks, etc...
// They are not connected to Vbus. They just act as a data holder.
// Each of theses classes can be serialized to Json with ToRepr() to be sent on Vbus.
package vBus

import (
	"github.com/alecthomas/jsonschema"
	"github.com/pkg/errors"
	"github.com/xeipuuv/gojsonschema"
	"reflect"
)

type iDefinition interface {
	// Search for a path in this definition.
	// It can returns a iDefinition or none if not found.
	searchPath(parts []string) iDefinition

	// Tells how to handle a set request from Vbus.
	handleSet(data interface{}, parts []string) (interface{}, error)

	// Tells how to handle a set request from Vbus.
	handleGet(data interface{}, parts []string) (interface{}, error)

	// Get the Vbus representation.
	ToRepr() JsonObj
}

// Tells if a raw node is an attribute.
func isAttribute(node interface{}) bool {
	return hasKey(node, "schema")
}

// Tells if a raw node is a method.
func isMethod(node interface{}) bool {
	return hasKey(node, "params") && hasKey(node, "returns")
}

// Tells if a raw node is a node.
func isNode(node interface{}) bool {
	return !isAttribute(node) && !isMethod(node)
}

type SetCallback = func(data interface{}, segment []string)
type GetCallback = func(data interface{}, segment []string) interface{}

// Advanced Nats methods options
type DefOptions struct {
	OnSet SetCallback
	OnGet GetCallback
}

// Option is a function on the options for a connection.
type defOption func(*DefOptions)

func OnGet(g GetCallback) defOption {
	return func(o *DefOptions) {
		o.OnGet = g
	}
}

func OnSet(g SetCallback) defOption {
	return func(o *DefOptions) {
		o.OnSet = g
	}
}

// Retrieve all options to a struct
func getDefOptions(advOpts ...defOption) DefOptions {
	// set default options
	opts := DefOptions{
		OnGet: nil,
		OnSet: nil,
	}
	for _, opt := range advOpts {
		opt(&opts)
	}
	return opts
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Error Definition
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Vbus error codes.
type ErrorCode int

const (
	ErrorPathNotFound ErrorCode = 1000
	ErrorInternal     ErrorCode = 2000
	ErrorValidation   ErrorCode = 3000
)

// Represents a Vbus error.
type ErrorDefinition struct { // implements iDefinition
	code    ErrorCode
	message string
	detail  interface{}
}

// Creates a new error definition.
func NewErrorDefinition(code ErrorCode, message string) *ErrorDefinition {
	return &ErrorDefinition{
		code:    code,
		message: message,
	}
}

// Creates a new error definition with detail.
func NewErrorDefinitionWithDetail(code ErrorCode, message string, detail interface{}) *ErrorDefinition {
	return &ErrorDefinition{
		code:    code,
		message: message,
		detail:  detail,
	}
}

func (e *ErrorDefinition) searchPath(parts []string) iDefinition {
	if len(parts) <= 0 {
		return e
	}
	return nil
}

func (e *ErrorDefinition) handleSet(data interface{}, parts []string) (interface{}, error) {
	log.Trace("not implemented")
	return nil, nil
}

func (e *ErrorDefinition) handleGet(data interface{}, parts []string) (interface{}, error) {
	return e.ToRepr(), nil
}

func (e *ErrorDefinition) ToRepr() JsonObj {
	if e.detail == nil {
		return map[string]interface{}{
			"code":    e.code,
			"message": e.message,
		}
	} else {
		return map[string]interface{}{
			"code":    e.code,
			"message": e.message,
			"errors":  e.detail,
		}
	}
}

func NewPathNotFoundError() *ErrorDefinition {
	return NewErrorDefinition(ErrorPathNotFound, "not found")
}

func NewPathNotFoundErrorWithDetail(p string) *ErrorDefinition {
	return NewErrorDefinitionWithDetail(ErrorPathNotFound, "not found", p)
}

func NewInternalError(err error) *ErrorDefinition {
	return NewErrorDefinitionWithDetail(ErrorInternal, "internal server error", err.Error())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Method Definition
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A Method definition.
// It holds a user callback.
type MethodDef struct { // implements iDefinition
	method        MethodDefCallback
	name          string
	paramsSchema  JsonObj
	returnsSchema JsonObj
}

// Method callback
type MethodDefCallback = interface{}

// Creates a new method definition with auto json schema.
func NewMethodDef(method MethodDefCallback) *MethodDef {
	md := &MethodDef{
		method:        method,
		name:          getFunctionName(method),
		paramsSchema:  nil,
		returnsSchema: nil,
	}

	err := md.inspectMethod()
	if err != nil {
		panic(errors.Wrap(err, "invalid method signature"))
	}

	return md
}

// Creates a new method def with the provided json schema.
func NewMethodDefWithSchema(method MethodDefCallback, paramsSchema JsonObj, returnsSchema JsonObj) *MethodDef {
	return &MethodDef{
		method:        method,
		name:          getFunctionName(method),
		paramsSchema:  paramsSchema,
		returnsSchema: returnsSchema,
	}
}

// Try to create json schema from method with introspection.
func (md *MethodDef) inspectMethod() error {
	if !isFunc(md.method) {
		return errors.New("not a func")
	}

	x := reflect.TypeOf(md.method)

	numIn := x.NumIn()   // Count inbound parameters
	numOut := x.NumOut() // Count out-bounding parameters

	if numOut > 1 {
		return errors.New("MethodDef only accept callback with one return value")
	}

	if numIn == 0 {
		return errors.New("the func should have at least one param: path []string")
	}

	// the last argument must be the path: path []string
	lastArg := x.In(numIn - 1)
	var checker []string
	if lastArg != reflect.TypeOf(checker) {
		return errors.New("the last parameter should be: path []string")
	}

	var paramsSchema []interface{}

	for i := 0; i < numIn-1; i++ { // ignore last param
		argType := x.In(i)
		schema := jsonschema.ReflectFromType(argType)
		paramsSchema = append(paramsSchema, schema)
	}

	var returnSchema *jsonschema.Schema

	if numOut > 0 { // has return value
		returnSchema = jsonschema.ReflectFromType(x.Out(0))
	} else { // no return value
		returnSchema = &jsonschema.Schema{
			Type: &jsonschema.Type{Type: "null"},
		}
	}

	md.paramsSchema = JsonObj{
		"type":  "array",
		"items": paramsSchema,
	}
	md.returnsSchema = structToJsonObj(returnSchema)
	return nil
}

func (md *MethodDef) searchPath(parts []string) iDefinition {
	if len(parts) <= 0 {
		return md
	}
	return nil
}

func (md *MethodDef) handleSet(args interface{}, parts []string) (interface{}, error) {
	if isSlice(args) {
		slice := args.([]interface{}) // to slice
		return invokeFunc(md.method, append(slice, parts)...)
	} else {
		// consider no args
		return invokeFunc(md.method, parts)
	}
}

func (md *MethodDef) handleGet(data interface{}, parts []string) (interface{}, error) {
	return md.ToRepr(), nil
}

func (md *MethodDef) ToRepr() JsonObj {
	return map[string]interface{}{
		"params": map[string]interface{}{
			"schema": md.paramsSchema,
		},
		"returns": map[string]interface{}{
			"schema": md.returnsSchema,
		},
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attribute iDefinition
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type AttributeDef struct { // implements iDefinition
	uuid   string
	value  interface{}
	schema interface{}
	onSet  SetCallback
	onGet  GetCallback
}

// Creates an attribute definition with an inferred Json-Schema
func NewAttributeDef(uuid string, value interface{}, options ...defOption) *AttributeDef {
	opts := getDefOptions(options...)
	if value == nil {
		log.Warnf("%s is null, no json schema can be inferred, use NewAttributeDefWithSchema", uuid)
	}

	schema := jsonschema.Reflect(value)
	return &AttributeDef{
		uuid:   uuid,
		value:  value,
		schema: schema,
		onSet:  opts.OnSet,
		onGet:  opts.OnGet,
	}
}

// Creates an attribute definition with a specific Json-Schema
func NewAttributeDefWithSchema(uuid string, value interface{}, schema map[string]interface{},
	onSet SetCallback, onGet GetCallback) *AttributeDef {
	return &AttributeDef{
		uuid:   uuid,
		value:  value,
		schema: schema,
		onSet:  onSet,
		onGet:  onGet}
}

func (a *AttributeDef) searchPath(parts []string) iDefinition {
	if len(parts) <= 0 {
		return a
	} else if sliceEqual(parts, []string{"value"}) {
		return a
	}
	return nil
}

func (a *AttributeDef) handleSet(data interface{}, parts []string) (interface{}, error) {
	schemaLoader := gojsonschema.NewGoLoader(a.schema)
	documentLoader := gojsonschema.NewGoLoader(data)
	result, err := gojsonschema.Validate(schemaLoader, documentLoader)
	if err != nil {
		return nil, err
	}

	if !result.Valid() {
		log.Warnf("invalid value received for attribute %s : %v (%v)", a.uuid, data, result.Errors())
		return nil, nil
	}

	if a.onSet != nil {
		return invokeFunc(a.onSet, data, parts)
	}
	log.Debugf("no set handler attached to %s", a.uuid)
	return nil, nil
}

func (a *AttributeDef) handleGet(data interface{}, parts []string) (interface{}, error) {
	if lastString(parts) == "value" { // request on value
		if a.onGet != nil {
			return invokeFunc(a.onGet, data, parts)
		} else {
			log.Debugf("no get handler attached to %s, returning cache", a.uuid)
			return a.value, nil
		}
	} else { // request on definition
		return a.ToRepr(), nil
	}
}

func (a *AttributeDef) ToRepr() JsonObj {
	if a.value == nil {
		return map[string]interface{}{
			"schema": a.schema,
		}
	} else {
		return map[string]interface{}{
			"schema": a.schema,
			"value":  a.value,
		}
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Node iDefinition
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

type RawNode = map[string]interface{}
type NodeStruct = map[string]iDefinition

// A node definition.
// It holds a user structure (Python object) and optional callbacks.
type NodeDef struct { // implements iDefinition
	structure NodeStruct
	onSet     SetCallback
}

func NewNodeDef(rawNode RawNode, option ...defOption) *NodeDef {
	opts := getDefOptions(option...)
	return &NodeDef{
		structure: initializeStructure(rawNode),
		onSet:     opts.OnSet,
	}
}

func (nd *NodeDef) searchPath(parts []string) iDefinition {
	if len(parts) <= 0 {
		return nd
	} else if v, ok := nd.structure[parts[0]]; ok {
		return v.searchPath(parts[1:])
	}
	return nil
}

func (nd *NodeDef) handleSet(data interface{}, parts []string) (interface{}, error) {
	if nd.onSet != nil {
		return invokeFunc(nd.onSet, data, parts)
	}
	return nil, nil
}

func (nd *NodeDef) handleGet(data interface{}, parts []string) (interface{}, error) {
	return nd.ToRepr(), nil
}

func (nd *NodeDef) ToRepr() JsonObj {
	repr := JsonObj{}

	for k, v := range nd.structure {
		repr[k] = v.ToRepr()
	}

	return repr
}

func initializeStructure(rawNode RawNode) NodeStruct {
	var structure = NodeStruct{}

	for k, v := range rawNode {
		if isMap(v) { // if its a map
			structure[k] = NewNodeDef(v.(RawNode))
		} else if d, ok := v.(iDefinition); ok { // if its already a definition
			structure[k] = d
		} else {
			structure[k] = NewAttributeDef(k, v)
		}
	}
	return structure
}

func (nd *NodeDef) AddChild(uuid string, node iDefinition) {
	nd.structure[uuid] = node
}

func (nd *NodeDef) RemoveChild(uuid string) iDefinition {
	if v, ok := nd.structure[uuid]; ok {
		delete(nd.structure, uuid)
		return v
	}
	return nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Aliases
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

var N = NewNodeDef
var A = NewAttributeDef
var M = NewMethodDef
