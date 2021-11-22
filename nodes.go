// A connected node is composed of a node definition and send commands over the Vbus when the
// user performs action on it. For example: add a child node, delete a node, call a method, etc...
package vBus

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"robpike.io/filter"
)

const (
	notifAdded       = "add"
	notifRemoved     = "del"
	notifGet         = "get"
	notifValueGet    = "value.get"
	notifSetted      = "set"
	notifValueSetted = "value.set"
	notifOwnership   = "own"
	exposeNodeUuid   = "uris"
)

var _nodesLog = getNamedLogger()

var (
	ErrOwnership = errors.New("MeshExclusive Ownership already taken")
)

// A Vbus connected element that send updates.
type IElement interface {
	getDefinition() IDefinition

	GetPath() string
}

// Base struct for all Vbus connected elements.
type Element struct {
	client     *ExtendedNatsClient
	uuid       string
	definition IDefinition
	parent     IElement
}

func (e *Element) GetUuid() string {
	return e.uuid
}

func (e *Element) getDefinition() IDefinition {
	return e.definition
}

// Returns the full path recursively.
func (e *Element) GetPath() string {
	if e.parent != nil {
		return joinPath(e.parent.GetPath(), e.uuid)
	} else {
		return e.uuid
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Node
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A Vbus connected node.
// It contains a node definition and send update over Vbus.
type Node struct { // implements Element
	*Element
	//
	definition *NodeDef
}

// Check remote node ownership
func (n *Node) checkNodeOwnership(node *Node, options ...defOption) error {
	opts := getDefOptions(options...)
	// if scope is mesh:exclusive, reduce scope
	// test only "master node": the one we provide the scope information
	if opts.Scope == meshExclusive {
		result, err := n.client.Request(joinPath(node.GetPath(), notifGet), nil, WithMesh())
		if err == nil {
			_, ok := result.(JsonObj)
			if ok == true {
				if opts.ForceOwnership == false {
					_nodesLog.Warn(node.GetPath() + " already exist -> reduce scope to local")
					node.definition.scope = local
					return ErrOwnership
				}
				_nodesLog.Warn(node.GetPath() + " already exist -> force ownership")
				return node.TakeOwnership()
			}
		}
	}
	return nil
}

func (n *Node) TakeOwnership() error {
	n.definition.scope = meshExclusive
	return n.client.Publish(joinPath(n.GetPath(), notifOwnership), n.client.hostname, n.definition.scope, WithMesh())
}

// Creates a Node.
func NewNode(nats *ExtendedNatsClient, uuid string, definition *NodeDef, parent IElement) *Node {
	return &Node{
		Element: &Element{
			uuid:       uuid,
			parent:     parent,
			client:     nats,
			definition: definition,
		},
		definition: definition,
	}
}

// Get node definition.
func (n *Node) Definition() *NodeDef {
	return n.definition
}

// Add a child node and notify Vbus
// Returns: a new node
func (n *Node) AddNode(uuid string, rawNode RawNode, options ...defOption) (*Node, error) {
	node, err := n.CreateNode(uuid, rawNode, options...)
	if err == nil {
		err = n.PublishNode((node))
	}
	return node, err
}

// Create a child node in Vbus
// but do not publish the node
// Returns: a new node
func (n *Node) CreateNode(uuid string, rawNode RawNode, options ...defOption) (*Node, error) {
	def := NewNodeDef(n, rawNode, options...) // create the definition
	node := NewNode(n.client, uuid, def, n)   // create the connected node
	err := n.checkNodeOwnership(node, options...)
	n.definition.AddChild(uuid, def) // add it
	return node, err
}

// Publish the node previously created with CreateNode
// Returns: a new node
func (n *Node) PublishNode(node *Node) error {
	// send the node definition on Vbus
	packet := JsonObj{node.uuid: node.getDefinition().ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), notifAdded), packet, node.definition.scope)
	if err != nil {
		return errors.Wrap(err, "cannot publish new node")
	}
	return nil
}

// Check remote attribute ownership
func (n *Node) checkAttributeOwnership(node *Attribute, options ...defOption) error {
	opts := getDefOptions(options...)
	// if scope is mesh:exclusive, reduce scope
	if opts.Scope == meshExclusive {
		result, err := n.client.Request(joinPath(node.GetPath(), notifGet), nil, WithMesh())
		if err == nil {
			_, ok := result.(JsonObj)
			if ok == true {
				if opts.ForceOwnership == false {
					_nodesLog.Warn(node.GetPath() + " already exist -> reduce scope to local")
					node.definition.scope = local
					return ErrOwnership
				}
				_nodesLog.Warn(node.GetPath() + " already exist -> force ownership")
				return node.TakeOwnership()
			}
		}
	}
	return nil
}

func (a *Attribute) TakeOwnership() error {
	a.definition.scope = meshExclusive
	return a.client.Publish(joinPath(a.GetPath(), notifOwnership), a.client.hostname, a.definition.scope, WithMesh())
}

// Add a child attribute and notify Vbus
func (n *Node) AddAttribute(uuid string, value interface{}, options ...defOption) (*Attribute, error) {
	node, err := n.CreateAttribute(uuid, value, options...)
	if err == nil {
		err = n.PublishAttribute(node)
	}
	return node, err
}

// Create a child attribute in Vbus
// but do not publish the attribute
// returns: attribute
func (n *Node) CreateAttribute(uuid string, value interface{}, options ...defOption) (*Attribute, error) {
	def := NewAttributeDef(n, uuid, value, options...) // create the definition
	node := NewAttribute(n.client, uuid, def, n)       // create the connected node
	err := n.checkAttributeOwnership(node)
	n.definition.AddChild(uuid, def) // add it
	return node, err
}

// Create a child attribute in Vbus with json schema
// but do not publish the attribute
// returns: attribute
func (n *Node) CreateAttributeWithSchema(uuid string, value interface{}, schema map[string]interface{}, options ...defOption) (*Attribute, error) {
	def := NewAttributeDefWithSchema(n, uuid, value, schema, options...) // create the definition
	node := NewAttribute(n.client, uuid, def, n)                         // create the connected node
	err := n.checkAttributeOwnership(node)
	n.definition.AddChild(uuid, def) // add it
	return node, err
}

// Publish the attribute previously created with CreateAttribute
// Returns: error
func (n *Node) PublishAttribute(node *Attribute) error {
	// send the node definition on Vbus
	packet := JsonObj{node.uuid: node.getDefinition().ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), notifAdded), packet, node.definition.scope)
	if err != nil {
		return errors.Wrap(err, "cannot publish new attribute")
	}
	return nil
}

// Check remote method ownership
func (n *Node) checkMethodOwnership(node *Method, options ...defOption) error {
	opts := getDefOptions(options...)
	// if scope is mesh:exclusive, reduce scope
	if opts.Scope == meshExclusive {
		result, err := n.client.Request(joinPath(node.GetPath(), notifGet), nil, WithMesh())
		if err == nil {
			_, ok := result.(JsonObj)
			if ok == true {
				if opts.ForceOwnership == false {
					_nodesLog.Warn(node.GetPath() + " already exist -> reduce scope to local")
					node.definition.scope = local
					return ErrOwnership
				}
				_nodesLog.Warn(node.GetPath() + " already exist -> force ownership")
				return node.TakeOwnership()
			}
		}
	}
	return nil
}

func (m *Method) TakeOwnership() error {
	m.definition.scope = meshExclusive
	return m.client.Publish(joinPath(m.GetPath(), notifOwnership), m.client.hostname, m.definition.scope, WithMesh())
}

// Add a child method node and notify Vbus
// The method must be a func(args..., path []string)
// The last argument is mandatory, it will receive the splited Nats path.
func (n *Node) AddMethod(uuid string, method MethodDefCallback, options ...defOption) (*Method, error) {
	node, err := n.CreateMethod(uuid, method, options...)
	if err == nil {
		err = n.PublishMethod(node)
	}
	return node, err
}

// Create a child method node but do not publish on Vbus
// The method must be a func(args..., path []string)
// The last argument is mandatory, it will receive the split Nats path.
func (n *Node) CreateMethod(uuid string, method MethodDefCallback, options ...defOption) (*Method, error) {
	// send the node definition on Vbus
	def := NewMethodDef(n, method, options...) // create the definition
	node := NewMethod(n.client, uuid, def, n)  // create the connected node
	err := n.checkMethodOwnership(node)
	n.definition.AddChild(uuid, def) // add it
	return node, err
}

// Create a child method in Vbus with json schema
// but do not publish the method
// returns: method
func (n *Node) CreateMethodWithSchema(uuid string, paramsSchema map[string]interface{}, returnsSchema map[string]interface{}, method MethodDefCallback, options ...defOption) (*Method, error) {
	// send the node definition on Vbus
	def := NewMethodDefWithSchema(n, method, paramsSchema, returnsSchema, options...) // create the definition
	node := NewMethod(n.client, uuid, def, n)                                         // create the connected node
	err := n.checkMethodOwnership(node)
	n.definition.AddChild(uuid, def) // add it
	return node, err
}

// Publish the method previously created with CreateMethod
// Returns: error
func (n *Node) PublishMethod(node *Method) error {
	// send the node definition on Vbus
	packet := JsonObj{node.uuid: node.getDefinition().ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), notifAdded), packet, node.definition.scope)
	if err != nil {
		return errors.Wrap(err, "cannot publish new method")
	}
	return nil
}

// Call the  method with some arguments.
func (m *Method) Call(args ...interface{}) error {
	return m.client.Publish(joinPath(m.GetPath(), notifSetted), args, m.definition.scope)
}

func (n *Node) GetAttribute(parts ...string) (*Attribute, error) {
	def := n.definition.searchPath(parts)
	if def == nil {
		return nil, errors.New("not found")
	}

	// test that the definition is an attribute def
	if attrDef, ok := def.(*AttributeDef); ok {
		return NewAttribute(n.client, joinPath(parts...), attrDef, n), nil
	} else {
		return nil, errors.New("not an attribute")
	}
}

// Remove an element in this node and notify Vbus
func (n *Node) RemoveElement(uuid string) error {
	def := n.definition.RemoveChild(uuid)
	if def == nil {
		return errors.New(fmt.Sprintf("element not found: %v", uuid))
	}

	// send the node definition on Vbus
	packet := JsonObj{uuid: def.ToRepr()}
	if err := n.client.Publish(joinPath(n.GetPath(), notifRemoved), packet, def.Scope()); err != nil {
		return errors.Wrap(err, "element deleted but cannot send vbus notification")
	}
	return nil // success
}

func (n *Node) String() string {
	return ToPrettyJson((*n.definition).ToRepr())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attribute
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A VBus connected attribute.
type Attribute struct { // implements IElement
	*Element
	//
	definition *AttributeDef
}

// Creates an attribute.
// You should create attribute with node.AddAttribute
func NewAttribute(nats *ExtendedNatsClient, uuid string, definition *AttributeDef, parent IElement) *Attribute {
	return &Attribute{
		Element: &Element{
			uuid:       uuid,
			parent:     parent,
			client:     nats,
			definition: definition,
		},
		definition: definition,
	}
}

// get attribute value.
func (a *Attribute) GetValue() interface{} {
	return a.definition.value
}

// Set attribute value.
func (a *Attribute) SetValue(value interface{}) error {
	a.definition.value = value
	return a.client.Publish(joinPath(a.GetPath(), notifValueSetted), value, a.definition.scope)
}

// Add option to an existing attribute
func (a *Attribute) AddOptions(options ...defOption) {
	a.definition.AddOptions(options...)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Method
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A VBus connect method.
type Method struct { // implements IElement
	*Element
	//
	definition *MethodDef
}

// Creates a Method.
func NewMethod(nats *ExtendedNatsClient, uuid string, definition *MethodDef, parent IElement) *Method {
	return &Method{
		Element: &Element{
			uuid:       uuid,
			parent:     parent,
			client:     nats,
			definition: definition,
		},
		definition: definition,
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Node Manager
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// The NodeManager handle high level action like discovering nodes.
type NodeManager struct {
	*Node
	//
	opts     natsClientOptions
	subs     []*nats.Subscription
	urisNode *Node
}

type ModuleStatus struct {
	HeapSize uint64 `json:"heapSize"`
}

// Response format for module info
type ModuleInfo struct {
	Id             string       `json:"id"`
	Hostname       string       `json:"hostname"`
	Client         string       `json:"client"`
	HasStaticFiles bool         `json:"hasStaticFiles"`
	Status         ModuleStatus `json:"status"`
}

// Creates a new NodeManager. Don't forget to defer Close()
func NewNodeManager(nats *ExtendedNatsClient, opts natsClientOptions) *NodeManager {
	return &NodeManager{
		Node:     NewNode(nats, "", NewNodeDef(nil, RawNode{}), nil),
		opts:     opts,
		urisNode: nil,
	}
}

func (nm *NodeManager) Discover(natsPath string, timeout time.Duration) (*UnknownProxy, error) {
	var resp JsonObj

	inbox := nm.client.client.NewRespInbox()

	sub, err := nm.client.client.Subscribe(inbox, func(msg *nats.Msg) {
		data, err := fromVbus(msg.Data)
		if err != nil {
			_nodesLog.WithFields(LF{"path": msg.Subject}).Warn("received invalid node")
			return // skip
		}

		o, ok := data.(JsonObj)
		if !ok {
			_nodesLog.Warn("received data is not a json object")
			return // skip
		}

		resp = mergeJsonObjs(resp, o)
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot subscribe to inbox")
	}

	err = nm.client.client.PublishRequest(natsPath, inbox, toVbus(nil))
	if err != nil {
		return nil, errors.Wrap(err, "cannot publish")
	}

	timer := time.NewTimer(timeout)
	<-timer.C

	_ = sub.Unsubscribe()
	_ = sub.Drain()

	return NewUnknownProxy(nm.client, natsPath, resp), nil
}

func (nm *NodeManager) DiscoverModules(timeout time.Duration) ([]ModuleInfo, error) {
	var resp []ModuleInfo

	inbox := nm.client.client.NewRespInbox()
	sub, err := nm.client.client.Subscribe(inbox, func(msg *nats.Msg) {
		var info ModuleInfo
		err := json.Unmarshal(msg.Data, &info)
		if err != nil {
			_nodesLog.WithFields(LF{"path": msg.Subject}).Warn("received invalid info, skipping")
			return // skip
		}
		resp = append(resp, info)
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot subscribe to inbox")
	}

	err = nm.client.client.PublishRequest("info", inbox, toVbus(nil))
	if err != nil {
		return nil, errors.Wrap(err, "cannot publish")
	}

	timer := time.NewTimer(timeout)
	<-timer.C

	_ = sub.Unsubscribe()
	_ = sub.Drain()

	return resp, nil
}

// A factory to create the handler for vbus static method.
func getVbusStaticMethod(opts natsClientOptions) func(method string, uri string, segments []string) ([]byte, error) {
	return func(method, uri string, segments []string) ([]byte, error) {
		logrus.Debugf("static: received %v on %v", method, uri)

		filepath := path.Join(opts.StaticPath, uri)
		if !fileExists(filepath) {
			filepath = path.Join(opts.StaticPath, "index.html") // assume SPA
		}

		content, err := ioutil.ReadFile(filepath)
		if err != nil {
			logrus.Error(err)
			return []byte{}, errors.New("file not found")
		}

		return content, nil
	}
}

func (nm *NodeManager) Initialize() error {
	// Subscribe to root path: "app-domain.app-name"
	sub, err := nm.client.Subscribe("", func(data interface{}, segments []string) interface{} {
		// get all nodes
		return JsonObj{nm.client.hostname: (*nm.definition).ToRepr()}
	}, WithoutHost())
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to root path")
	}
	nm.subs = append(nm.subs, sub) // save sub

	// Subscribe to all "mesh"
	sub, err = nm.client.Subscribe(">", func(data interface{}, segments []string) interface{} {
		// Get a specific path
		parts := strings.Split(segments[0], ".")               // split the first segment (">") to string list.
		parts = filter.Choose(parts, isStrNotEmpty).([]string) // filter empty strings
		if len(parts) < 1 {
			return nil // invalid path, missing event ("add", "del"...)
		}

		event, parts := parts[len(parts)-1], parts[:len(parts)-1] // pop event from parts
		if nm.handleMesh(parts...) == false {
			return nil // element not mesh visibility
		}
		return nm.handleEvent(data, event, parts...)
	}, WithMesh())
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to all")
	}
	nm.subs = append(nm.subs, sub) // save sub

	// Subscribe to all "local"
	sub, err = nm.client.Subscribe(">", func(data interface{}, segments []string) interface{} {
		// Get a specific path
		parts := strings.Split(segments[0], ".")               // split the first segment (">") to string list.
		parts = filter.Choose(parts, isStrNotEmpty).([]string) // filter empty strings
		if len(parts) < 1 {
			return nil // invalid path, missing event ("add", "del"...)
		}

		event, parts := parts[len(parts)-1], parts[:len(parts)-1] // pop event from parts
		return nm.handleEvent(data, event, parts...)
	})
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to all")
	}
	nm.subs = append(nm.subs, sub) // save sub

	// Subscribe to generic info path
	sub, err = nm.client.Subscribe("info", func(data interface{}, segments []string) interface{} {
		return nm.getModuleInfo()
	}, WithoutHost(), WithoutId())
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to info path")
	}
	nm.subs = append(nm.subs, sub) // save sub

	// handle static file server
	if nm.opts.HasStaticPath {
		_, err = nm.AddMethod("static", getVbusStaticMethod(nm.opts))
		if err != nil {
			return errors.Wrap(err, "cannot register file server method")
		}
	}

	return nil
}

func (nm *NodeManager) getModuleInfo() ModuleInfo {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	return ModuleInfo{
		Id:             nm.client.GetId(),
		Hostname:       nm.client.GetHostname(),
		Client:         "golang",
		HasStaticFiles: nm.opts.HasStaticPath,
		Status: ModuleStatus{
			HeapSize: m.HeapAlloc,
		},
	}
}

// Handle incoming get requests.
func (nm *NodeManager) handleEvent(data interface{}, event string, segments ...string) interface{} {
	nodeDef := (*nm.definition).searchPath(segments)
	if nodeDef != nil { // if found
		var ret interface{}
		var err error

		switch event {
		case notifGet:
			ret, err = nodeDef.handleGet(data, segments)
		case notifSetted:
			ret, err = nodeDef.handleSet(data, segments)
		case notifOwnership:
			ret, err = nodeDef.handleOwnership(data, segments, nm.client.hostname)
		default:
			return nil
		}

		if err != nil {
			switch err.(type) {
			// Internal error
			default:
				_nodesLog.WithFields(LF{"path": joinPath(segments...), "error": err}).Warn("internal error")
				return NewInternalError(err).ToRepr()

			case userError:
				_nodesLog.WithFields(LF{"path": joinPath(segments...), "error": err}).Debugf("user side error")
				return NewUserSideError(err).ToRepr()
			}
		}

		return ret
	} else { // Path not found
		_nodesLog.WithFields(LF{"path": joinPath(segments...)}).Warn("path not found")
		return NewPathNotFoundErrorWithDetail(joinPath(segments...)).ToRepr()
	}
}

// Handle mesh requests
func (nm *NodeManager) handleMesh(segments ...string) bool {
	nodeDef := (*nm.definition).searchPath(segments)
	if nodeDef != nil {
		scope := nodeDef.Scope()
		if (scope == meshExclusive) || (scope == meshInclusive) {
			return true
		}
	}
	return false
}

func (nm *NodeManager) Close() error {
	_ = nm.client.client.Flush()
	for _, sid := range nm.subs {
		err := sid.Unsubscribe()
		if err != nil {
			return errors.Wrap(err, "cannot unsubscribe from all")
		}
	}
	return nil
}

// Retrieve a remote node
func (nm *NodeManager) GetRemoteNode(parts ...string) (*NodeProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetNode(parts...)
}

// Retrieve a remote method
func (nm *NodeManager) GetRemoteMethod(parts ...string) (*MethodProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetMethod(parts...)
}

// Retrieve a remote attribute
func (nm *NodeManager) GetRemoteAttr(parts ...string) (*AttributeProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetAttribute(parts...)
}

// Retrieve a remote element (node, attribute or method)
func (nm *NodeManager) GetRemoteElement(parts ...string) (*UnknownProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetElement(parts...)
}

// Retrieve a remote element with timeout (node, attribute or method)
func (nm *NodeManager) GetRemoteElementWithTimeout(timeout time.Duration, parts ...string) (*UnknownProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetElementWithTimeout(timeout, parts...)
}

// Retrieve the network IP
func (nm *NodeManager) GetNetworkIP() (string, error) {
	conf, err := nm.client.readOrGetDefaultConfig()
	if err != nil {
		return "", err
	}

	networkIP := conf.Vbus.NetworkIp

	if networkIP == "" {
		return "", errors.New("no network IP available")
	}

	return networkIP, nil

}

// Expose a service identified with an uri on Vbus.
func (nm *NodeManager) Expose(name, protocol string, port int, path string) error {
	conf, err := nm.client.readOrGetDefaultConfig()
	if err != nil {
		return err
	}

	networkIp := conf.Vbus.NetworkIp

	if networkIp == "" {
		networkIp = nm.client.client.ConnectedAddr()

		// remove port information
		if strings.Contains(networkIp, ":") {
			networkIp = strings.Split(networkIp, ":")[0]
		}

		_nodesLog.WithField("ip", networkIp).Warn("expose: network ip not populated, using nats connection ip instead")
	}

	uri := fmt.Sprintf("%v://%v:%v/%v", protocol, networkIp, port, path)

	if nm.urisNode == nil {
		node, err := nm.AddNode(exposeNodeUuid, RawNode{})
		if err != nil {
			return errors.Wrap(err, "cannot create 'uris' node")
		}
		nm.urisNode = node
	}

	_, err = nm.urisNode.AddAttribute(name, uri)

	if err == nil {
		_nodesLog.WithField("uri", uri).Info("successfully exposed service")
	}

	return err
}
