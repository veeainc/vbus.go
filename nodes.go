// This module contains connected nodes.
// A connected node is composed of a node definition and send commands over the Vbus when the
// user performs action on it. For example: add a child node, delete a node, call a method, etc...
package vBus

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"strings"
	"time"
)

const (
	notifAdded       = "add"
	notifRemoved     = "del"
	notifGet         = "get"
	notifValueGet    = "value.get"
	notifSetted      = "set"
	notifValueSetted = "value.set"
)

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
	node := NewNode(n.client, uuid, def, n) // create the connected nod
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
	node := NewAttribute(n.client, uuid, def, n)    // create the connected nod
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
	def, err := NewMethodDef(method)               // create the definition
	if err != nil {
		return nil, err
	}

	node := NewMethod(n.client, uuid, def, n) // create the connected nod
	n.definition.AddChild(uuid, def)          // add it

	// send the node definition on Vbus
	packet := JsonObj{uuid: def.ToRepr()}
	err = n.client.Publish(joinPath(n.GetPath(), notifAdded), packet)
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

	if def != nil {
		// send the node definition on Vbus
		packet := JsonObj{uuid: def.ToRepr()}
		err := n.client.Publish(joinPath(n.GetPath(), notifRemoved), packet)
		if err != nil {
			return nil
		}
	}
	return nil
}

func (n *Node) String() string {
	return ToPrettyJson((*n.definition).ToRepr())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attribute
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// A VBus connect attribute.
type Attribute struct { // implements IElement
	*Element
	//
	definition *AttributeDef
}

// Creates an Attribute.
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
	subs []*nats.Subscription
}

// Creates a new NodeManager. Don't forget to defer Close()
func NewNodeManager(nats *ExtendedNatsClient) *NodeManager {
	return &NodeManager{
		Node: NewNode(nats, "", NewNodeDef(RawNode{}), nil),
	}
}

func (nm *NodeManager) Discover(natsPath string, timeout time.Duration) (*NodeProxy, error) {
	var resp JsonObj

	inbox := nm.client.client.NewRespInbox()

	sub, err := nm.client.client.Subscribe(inbox, func(msg *nats.Msg) {
		data, err := fromVbus(msg.Data)
		if err != nil {
			log.Warnf("received invalid node from %s", msg.Subject)
			return // skip
		}

		o, ok := data.(JsonObj)
		if !ok {
			log.Warn("received data is not a json object")
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

	return NewNodeProxy(nm.client, natsPath, resp), nil
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
		parts := strings.Split(segments[0], ".") // split the first segment (">") to string list.
		if len(parts) < 2 {
			return nil // invalid path, missing event ("add", "del"...)
		}

		event, parts := parts[len(parts)-1], parts[:len(parts)-1] // pop event from parts
		return nm.handleEvent(data, event, parts...)
	})
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to all")
	}
	nm.subs = append(nm.subs, sub) // save sub
	return nil
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

		// Internal error
		if err != nil {
			log.Warnf("internal error while handling %s (%v)", joinPath(segments...), err.Error())
			return NewInternalError(err).ToRepr()
		}
		return ret
	} else { // Path not found
		log.Warnf("path not found %s", joinPath(segments...))
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

func (nm *NodeManager) GetRemoteNode(parts ...string) (*NodeProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetNode(parts...)
}

func (nm *NodeManager) GetRemoteMethod(parts ...string) (*MethodProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetMethod(parts...)
}

func (nm *NodeManager) GetRemoteAttr(parts ...string) (*AttributeProxy, error) {
	return NewNodeProxy(nm.client, "", JsonObj{}).GetAttribute(parts...)
}
