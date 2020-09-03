// A connected node is composed of a node definition and send commands over the Vbus when the
// user performs action on it. For example: add a child node, delete a node, call a method, etc...
package vBus

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"path"
	"runtime"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/robpike/filter"
	"github.com/sirupsen/logrus"
)

const (
	notifAdded       = "add"
	notifRemoved     = "del"
	notifGet         = "get"
	notifValueGet    = "value.get"
	notifSetted      = "set"
	notifValueSetted = "value.set"
	exposeNodeUuid   = "uris"
)

var _nodesLog = getNamedLogger()

// A Vbus connected element that send updates.
type IElement interface {
	getDefinition() iDefinition

	GetPath() string
}

// Base struct for all Vbus connected elements.
type Element struct {
	client     *ExtendedNatsClient
	uuid       string
	definition iDefinition
	parent     IElement
}

func (e *Element) GetUuid() string {
	return e.uuid
}

func (e *Element) getDefinition() iDefinition {
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

// Add a child node and notify Vbus
// Returns: a new node
func (n *Node) AddNode(uuid string, rawNode RawNode, options ...defOption) (*Node, error) {
	def := NewNodeDef(rawNode, options...)  // create the definition
	node := NewNode(n.client, uuid, def, n) // create the connected node
	n.definition.AddChild(uuid, def)        // add it

	// send the node definition on Vbus
	packet := JsonObj{uuid: def.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), notifAdded), packet)
	if err != nil {
		return node, errors.Wrap(err, "cannot publish new node")
	}

	return node, nil
}

// Add a child attribute and notify Vbus
func (n *Node) AddAttribute(uuid string, value interface{}, options ...defOption) (*Attribute, error) {
	def := NewAttributeDef(uuid, value, options...) // create the definition
	node := NewAttribute(n.client, uuid, def, n)    // create the connected node
	n.definition.AddChild(uuid, def)                // add it

	// send the node definition on Vbus
	packet := JsonObj{uuid: def.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), notifAdded), packet)
	if err != nil {
		return node, errors.Wrap(err, "cannot publish new attribute")
	}

	return node, nil
}

// Add a child method node and notify Vbus
// The method must be a func(args..., path []string)
// The last argument is mandatory, it will receive the splited Nats path.
func (n *Node) AddMethod(uuid string, method MethodDefCallback) (*Method, error) {
	def := NewMethodDef(method)               // create the definition
	node := NewMethod(n.client, uuid, def, n) // create the connected node
	n.definition.AddChild(uuid, def)          // add it

	// send the node definition on Vbus
	packet := JsonObj{uuid: def.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), notifAdded), packet)
	if err != nil {
		return node, errors.Wrap(err, "cannot publish new method")
	}

	return node, nil
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
	if err := n.client.Publish(joinPath(n.GetPath(), notifRemoved), packet); err != nil {
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

// Set attribute value.
func (a *Attribute) SetValue(value interface{}) error {
	a.definition.value = value
	return a.client.Publish(joinPath(a.GetPath(), notifValueSetted), value)
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
		Node:     NewNode(nats, "", NewNodeDef(RawNode{}), nil),
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
			_nodesLog.Warnf("received invalid node from %s", msg.Subject)
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
			_nodesLog.Warnf("received invalid info from %s, skipping", msg.Subject)
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

	// Subscribe to all
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
		default:
			return nil
		}

		if err != nil {
			switch err.(type) {
			// Internal error
			default:
				_nodesLog.Warnf("internal error while handling %s (%v)", joinPath(segments...), err.Error())
				return NewInternalError(err).ToRepr()

			case userError:
				_nodesLog.Debugf("user side error while handling %s (%v)", joinPath(segments...), err.Error())
				return NewUserSideError(err).ToRepr()
			}
		}

		return ret
	} else { // Path not found
		_nodesLog.Warnf("path not found %s", joinPath(segments...))
		return NewPathNotFoundErrorWithDetail(joinPath(segments...)).ToRepr()
	}
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

// Expose a service identified with an uri on Vbus.
func (nm *NodeManager) Expose(name, protocol string, port int, path string) error {
	// resolve current hostname to get ip address
	addr, err := net.LookupIP(nm.client.GetHostname())
	if err != nil {
		return errors.Wrap(err, "Cannot resolve hostname")
	}
	ipAddress := addr[0]
	uri := fmt.Sprintf("%v://%v:%v/%v", protocol, ipAddress, port, path)

	if nm.urisNode == nil {
		node, err := nm.AddNode(exposeNodeUuid, RawNode{})
		if err != nil {
			return errors.Wrap(err, "cannot create 'uris' node")
		}
		nm.urisNode = node
	}

	_, err = nm.urisNode.AddAttribute(name, uri)
	return err
}
