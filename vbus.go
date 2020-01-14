package vBus

import (
	//    "fl
	//"errors"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/gabs/v2"
	"github.com/godbus/dbus"
	"github.com/grandcat/zeroconf"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/bcrypt"
)

var localConfig *gabs.Container
var subListAdd []string
var subListGet []string
var subListSet []string
var subListRemove []string

// Node is the generic element of a vbus tree
type Node struct {
	nc      *nats.Conn
	element *gabs.Container
	base    string
	sub     *nats.Subscription
}

// NodeCallback prototype for Node Subscribe callback function
type NodeCallback func(string) string

// Method is the generic element of a vbus tree
type Method struct {
	nc      *nats.Conn
	element *gabs.Container
	base    string
	sub     *nats.Subscription
}

// MethodCallback prototype for Method Subscribe callback function
type MethodCallback func([]byte) []byte

// Attribute is the generic attribute of a vbus tree
type Attribute struct {
	nc      *nats.Conn
	value   interface{}
	path    string
	key     string
	atype   string
	element *gabs.Container
	sub     *nats.Subscription
}

// AttributeCallback prototype for Attribute Subscribe callback function
// provide the attribute value as a function parameter
type AttributeCallback func(value interface{}) interface{}

const (
	// PasswordLength Make sure the password is reasonably long to generate enough entropy.
	PasswordLength = 22
	// DefaultCost Common advice from the past couple of years suggests that 10 should be sufficient.
	// Up that a little, to 11. Feel free to raise this higher if this value from 2015 is
	// no longer appropriate. Min is bcrypt.MinCost, Max is bcrypt.MaxCost.
	DefaultCost = 11
)

func genPassword() string {
	var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")
	b := make([]byte, PasswordLength)
	max := big.NewInt(int64(len(ch)))
	for i := range b {
		ri, err := rand.Int(rand.Reader, max)
		if err != nil {
			log.Fatalf("Error producing random integer: %v\n", err)
		}
		b[i] = ch[int(ri.Int64())]
	}
	return string(b)
}

func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

func testvBusURL(url string) bool {
	vbusTest := false
	tmp, err := nats.Connect(url, nats.UserInfo("anonymous", "anonymous"))
	if err == nil {
		vbusTest = true
		tmp.Close()
	}
	return vbusTest
}

// Open - Create a new connection to vbus.
func Open(id string) (*Node, error) {
	privatekey := genPassword()
	return OpenWithPassword(id, privatekey)
}

// OpenWithPassword - Create a new connection to vbus,  with password specified.
func OpenWithPassword(id string, pwd string) (*Node, error) {
	// create vbus element struct
	v := &(Node{})

	// check if we already have a vbus config file
	rootfolder := os.Getenv("VBUS_PATH")
	if rootfolder == "" {
		rootfolder = os.Getenv("HOME")
		rootfolder = rootfolder + "/vbus/"
	} else {
		if strings.HasSuffix(rootfolder, "/") == false {
			rootfolder = rootfolder + "/"
		}
	}
	log.Printf("vbus config path: " + rootfolder + "\n")

	// create user name
	hostnameLocal, _ := os.Hostname()
	hostname := strings.Split(hostnameLocal, ".")[0]

	isVeeaHub := false
	dbusconn, err := dbus.SystemBus()
	if err != nil {
		log.Println(err)
		//panic(err)
		log.Printf("cannot connect to dbus, but let's continue anyway\n")
	} else {
		obj := dbusconn.Object("io.veea.VeeaHub.Info", "/io/veea/VeeaHub/Info")
		call := obj.Call("io.veea.VeeaHub.Info.Hostname", 0)
		if call.Err != nil {
			log.Println("Failed to get hostname:", call.Err)
			log.Printf("cannot connect to veea dbus api but let's continue anyway\n")
			//panic(err)
		}
		call.Store(&hostname)
		isVeeaHub = true
	}
	log.Println("hostname: " + hostname)
	uuid := id + "." + hostname
	log.Println("uuid: " + uuid)

	localConfig = gabs.New()
	os.MkdirAll(rootfolder, os.ModePerm)
	_, err = os.Stat(uuid + ".conf")
	if err != nil {
		log.Printf("no existing configuration file for " + uuid + "\n")
		log.Printf("configuration file creation\n")

		// create auth tree
		publickey, _ := bcrypt.GenerateFromPassword([]byte(pwd), DefaultCost)
		localConfig.Set(uuid, "client", "user")
		localConfig.Set(string(publickey), "client", "password")
		localConfig.Array("client", "permissions", "subscribe")
		localConfig.Array("client", "permissions", "publish")
		localConfig.ArrayAppend(id+".>", "client", "permissions", "subscribe")
		localConfig.ArrayAppend(id, "client", "permissions", "subscribe")
		localConfig.ArrayAppend(id+".>", "client", "permissions", "publish")
		localConfig.ArrayAppend(id, "client", "permissions", "publish")
		localConfig.Set(string(pwd), "key", "private")

		// create private tree

		log.Printf(localConfig.StringIndent("", " "))
	} else {
		log.Printf("load existing configuration file for " + id + "\n")
		file, _ := ioutil.ReadFile(uuid + ".conf")
		localConfig, _ = gabs.ParseJSON([]byte(file))
		log.Printf(localConfig.StringIndent("", " "))
	}

	var vbusURL string
	// find vbus server  - strategy 1: get url from localConfig file
	if vbusURL == "" {
		vbusURLExists := localConfig.Exists("vbus", "url")
		if vbusURLExists == true {
			if testvBusURL(localConfig.Search("vbus", "url").Data().(string)) == true {
				vbusURL = localConfig.Search("vbus", "url").Data().(string)
				log.Printf("url from config file ok: " + vbusURL + "\n")
			} else {
				log.Printf("url from config file hs: " + localConfig.Search("vbus", "url").Data().(string) + "\n")
			}
		}
		vbusHostnameExists := localConfig.Exists("vbus", "hostname")
		if vbusHostnameExists == true {
			hostname = localConfig.Search("vbus", "hostname").Data().(string)
		}
	}

	// find vbus server  - strategy 2: get url from ENV:vbusURL
	if vbusURL == "" {
		vbusENVURL := os.Getenv("VBUS_URL")
		if vbusENVURL != "" {
			if testvBusURL(vbusENVURL) == true {
				vbusURL = vbusENVURL
				log.Printf("url from ENV ok: " + vbusENVURL + "\n")
			} else {
				log.Printf("url from ENV hs: " + vbusENVURL + "\n")
			}
		}
	}

	// find vbus server  - strategy 3: try default url nats://hostname:21400
	if vbusURL == "" {
		hostname, _ := os.Hostname()
		vBusDefaultURL := "nats://" + hostname + ".local:21400"
		if testvBusURL(vBusDefaultURL) == true {
			vbusURL = vBusDefaultURL
			log.Printf("url from default ok: " + vBusDefaultURL + "\n")
		} else {
			log.Printf("url from default hs: " + vBusDefaultURL + "\n")
		}
	}

	// find vbus server  - strategy 4: find it using avahi
	if vbusURL == "" {
		log.Printf("find vbus on network\n")
		resolver, err := zeroconf.NewResolver(nil)
		if err != nil {
			log.Fatalln("Failed to initialize resolver:", err.Error())
		}

		servicelist := make(chan *zeroconf.ServiceEntry, 1)
		entries := make(chan *zeroconf.ServiceEntry)
		go func(results <-chan *zeroconf.ServiceEntry) {
			for entry := range results {
				log.Println(entry)
				if "vBus" == entry.Instance {
					// next step compare host_name to choose the same one than the service if available
					log.Println("vbus found !!")
					servicelist <- entry
				}
			}
			log.Println("No more entries.")
		}(entries)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		err = resolver.Browse(ctx, "_nats._tcp", "local.", entries)
		if err != nil {
			log.Fatalln("Failed to browse:", err.Error())
		}

		<-ctx.Done()

		select {
		case firstservice := <-servicelist:
			routesStr := "nats://" + firstservice.AddrIPv4[0].String() + ":" + strconv.Itoa(firstservice.Port)
			log.Println("vbus url discovered is: " + routesStr)
			if testvBusURL(routesStr) == true {
				vbusURL = routesStr
				log.Printf("url from discovery ok: " + routesStr + "\n")
				if isVeeaHub == false {
					// try to retrieve real VH hostname case we are not on a VH
					hostParsed := strings.Split(firstservice.Text[0], "=")
					if hostParsed[0] == "hostname" {
						hostname = hostParsed[1]
						log.Printf("hostname retrived from mDns: " + hostname)
					}
				}
			} else {
				log.Printf("url from discovery hs: " + routesStr + "\n")
			}
		default:
			log.Println("no service found")
		}
	}

	// record config file
	if vbusURL == "" {
		panic("no valid url vbus found")
	}

	localConfig.Set(vbusURL, "vbus", "url")
	localConfig.Set(hostname, "vbus", "hostname")
	ioutil.WriteFile(uuid+".conf", localConfig.Bytes(), 0666)

	// create base element tree
	// only a base "object" type described by it's empty schema
	v.element = gabs.New()
	v.base = uuid

	// connect to vbus server
	directconnect := true
	log.Printf("open connection with local nats\n")
	v.nc, err = nats.Connect(vbusURL, nats.UserInfo(uuid, localConfig.Search("key", "private").Data().(string)))
	if err != nil {
		// maybe user doesn't exist yet
		v.nc, err = nats.Connect(vbusURL, nats.UserInfo("anonymous", "anonymous"))

		if err != nil {
			log.Fatalf("Can't connect: %v\n", err)
		} else {
			directconnect = false
		}
	}

	if directconnect == false { // since we had to connect as an anonymous, we now, reconnect with true credentials
		log.Printf("publish user \n")
		v.nc.Publish("system.authorization."+hostname+".add", localConfig.Search("client").Bytes())

		v.nc.Close()
		time.Sleep(5000 * time.Millisecond)
		v.nc, err = nats.Connect(vbusURL, nats.UserInfo(uuid, localConfig.Search("key", "private").Data().(string)))
		if err != nil {
			log.Fatalf("Can't connect: %v\n", err)
		}
	}

	log.Printf("publish element\n")
	v.nc.Publish(v.base, v.element.Bytes())

	v.sub, _ = v.nc.Subscribe(id+".>", v.dbAccess)
	v.nc.Subscribe(id, v.dbAccess)

	return v, nil
}

// GetvBusIP provide vBus IP
func (v *Node) GetvBusIP() string {
	fullIP := localConfig.Search("vbus", "url").Data().(string)
	return strings.TrimPrefix(fullIP, "nats://")
}

func (v *Node) dbAccess(m *nats.Msg) {

	// first track generic discovery
	if strings.HasPrefix(v.base, m.Subject) {
		fullNode := v.Full()
		log.Printf("Request to get " + m.Subject)
		if fullNode.IsNode(m.Subject) == true {
			tmpNode, err := fullNode.Node(m.Subject)
			if err != nil {
				log.Printf("Error in get node: %v\n", err)
			} else {
				m.Respond([]byte(tmpNode.Tree()))
				log.Printf(tmpNode.Tree())
			}
		}
	} else if strings.HasPrefix(m.Subject, v.base) {
		cmd := m.Subject[strings.LastIndex(m.Subject, ".")+1:]
		path := strings.TrimPrefix(m.Subject[:strings.LastIndex(m.Subject, ".")], v.base)
		path = strings.TrimPrefix(path, ".")
		switch cmd {
		default:
			log.Printf("cmd: " + m.Subject)
			path = m.Subject
			if v.IsNode(path) == true {
				tmpNode, err := v.Node(path)
				if err != nil {
					log.Printf("Error in get node: %v\n", err)
				} else {
					m.Respond([]byte(tmpNode.Full().Tree()))
				}
			} else {
				tmpAtt, err := v.Attribute(path)
				if err != nil {
					log.Printf("Error in get attribute: %v\n", err)
				} else {
					msg, err := json.Marshal(tmpAtt.value)
					if err != nil {
						log.Printf("Error in get attribute: %v\n", err)
					} else {
						m.Respond(msg)
					}
				}
			}
		case "add":
			log.Printf("add: " + string(m.Data) + " to " + path)
			if v.IsNode(path) == true {
				tmpNode, err := v.Node(path)
				if err != nil {
					log.Printf("Error in get node: %v\n", err)
				} else {
					newData, err := gabs.ParseJSON(m.Data)
					if err == nil {
						tmpNode.element.Merge(newData)
					} else {
						log.Printf(err.Error())
					}
				}
			} else {
				log.Printf("node not existing")
			}
		case "get":
			log.Printf("get cmd: " + path)
			if stringInSlice(path, subListSet) == true {
				log.Printf("not processed here")
			} else {

				msg, err := json.Marshal(v.element.Path(path).Data())
				if err != nil {
					log.Printf("Error in get attribute: %v\n", err)
				} else {
					m.Respond(msg)
				}
			}
		case "set":
			log.Printf("set cmd: " + path + " - not supported yet")
		case "remove":
			if v.IsNode(path) == true {

				cut := strings.LastIndex(path, ".")
				if cut < 0 {
					log.Printf("cannot get Parent\n")
					m.Respond([]byte("cannot get Parent"))
				}
				tmpNode, err := v.Node(v.base[:cut])
				if err != nil {
					log.Printf("Error in get node: %v\n", err)
				} else {
					tmpNode.element.Delete(v.base[cut+1:])
				}
			} else {
				log.Printf("node not existing")
			}
		}
	}
}

// Close node
func (v *Node) Close() {
	v.nc.Drain()
	v.nc.Close()
}

// Base returns the tree base path
func (v *Node) Base() string {
	return v.base
}

// Tree returns the full tree contained by the node (string)
func (v *Node) Tree() string {
	if v.element != nil {
		return v.element.String()
	}
	return ""
}

// Map returns the node subtree list
func (v *Node) Map() map[string]*Node {
	m := make(map[string]*Node)
	for key, child := range v.element.ChildrenMap() {
		//if v.Type(key) == "object" {
		//if !child.ExistsP("schema") {
		node := &(Node{})
		node.nc = v.nc
		node.element = child
		node.base = v.base + "." + key
		m[key] = node
		//}
	}
	return m
}

// Full returns the full tree contained by the node
func (v *Node) Full() *Node {
	node := &(Node{})
	node.nc = v.nc
	node.base = v.base
	node.element = gabs.New()
	node.element.SetP(v.element.Data(), v.base)

	return node
}

// Type returns the attribute type (string)
func (a *Attribute) Type() string {
	return getAttributeType(a.value)
}

// Type returns the pointed element type (string)
func (v *Node) Type(subpath string) string {
	if subpath == "" {
		return getAttributeType(v.element.Data())
	}

	if !v.element.ExistsP(subpath) {
		return ""
	}

	return getAttributeType(v.element.Path(subpath).Data())
}

// IsNode returns true if path is a node
func (v *Node) IsNode(subpath string) bool {
	// is element contains a schema, this is a node
	if subpath == "" {
		// node root always exist
		return true
	}
	return v.element.ExistsP(subpath) // + ".schema.type")
}

// IsAttribute returns true if path is an attribute
func (v *Node) IsAttribute(subpath string) bool {
	// is element contains a schema, this is a node
	if subpath == "" {
		// node cannot be attribute
		return false
	}
	//pathType := v.element.Path("schema.properties." + subpath + ".type").Data().(string)
	//elemType := v.Type(subpath)
	//if (elemType != "object") && (elemType != "") {
	if v.element.ExistsP(subpath + ".schema") {
		return true
	}

	return false
}

// Parent returns the node's parent
func (v *Node) Parent() (*Node, error) {
	cut := strings.LastIndex(v.base, ".")

	if cut < 0 {
		return nil, errors.New("cannot get Parent")
	}
	node := &(Node{})
	node.nc = v.nc
	node.base = v.base[:cut]
	node.element = gabs.New()
	err := node.Get()
	if err != nil {
		return nil, err
	}

	return node, nil
}

// Path return the attribute full path
func (a *Attribute) Path() string {
	return a.path + "." + a.key
}

// Value return the attribute value
func (a *Attribute) Value() interface{} {
	return a.value
}

// Value return the attribute value in String
func (a *Attribute) String() string {
	value := ""
	switch a.atype {
	default:
		log.Printf("Error in get attribute: type not supported\n")
	case "boolean":
		value = strconv.FormatBool(a.value.(bool))
	case "integer":
		value = strconv.Itoa(a.value.(int))
	case "string":
		value = a.value.(string)
	case "number":
		value = strconv.FormatFloat(float64(a.value.(float32)), 'b', -1, 32)
	}

	return value
}

// Node returns the sub node requested
func (v *Node) Node(subpath string) (*Node, error) {
	if subpath == "" {
		// return self
		return v, nil
	}

	node := &(Node{})
	node.nc = v.nc
	if v.element.ExistsP(subpath) == false {
		node.base = subpath
		node.element = gabs.New()
	} else {
		node.element = v.element.Path(subpath)
		node.base = v.base + "." + subpath
	}

	return node, nil
}

// NodeToAttribute convert a node in Attribute if possible
func (v *Node) NodeToAttribute() (*Attribute, error) {
	attr := &(Attribute{})
	attr.nc = v.nc

	if v.element.Exists("schema") == false {
		return nil, errors.New("doesn't have a schema: not an attribute")
	}

	attr.atype = v.element.Path("schema.type").Data().(string)
	attr.element = v.element

	ind := strings.LastIndex(v.base, ".")
	if ind > 0 {
		attr.path = v.base[:ind]
		attr.key = v.base[ind+1:]
	} else {
		attr.key = v.base
		attr.path = ""
	}

	switch attr.atype {
	default:
		return nil, errors.New("type not supported")
	case "boolean":
		attr.value = v.element.Path("value").Data()
	case "integer":
		attr.value = v.element.Path("value").Data()
	case "string":
		attr.value = v.element.Path("value").Data()
	case "number":
		attr.value = v.element.Path("value").Data()
	}

	return attr, nil
}

// Attribute returns the sub node requested
func (v *Node) Attribute(path string) (*Attribute, error) {
	attr := &(Attribute{})
	attr.nc = v.nc

	log.Printf("looking for " + path + " in:")
	log.Printf(v.Tree())

	if v.element.ExistsP(path) == false {
		log.Printf("path not already existing, try to get it")
		// if we don't have any information about this attribute, first, get it's node
		//nodePath := path[:strings.LastIndex(path, ".")]
		//nodeKey := path[strings.LastIndex(path, "."):]
		//nodeKey = strings.TrimPrefix(nodeKey, ".")
		tmpNode, err := v.Node(path)
		if err != nil {
			return nil, errors.New("cannot retrieve Attribute Node" + err.Error())
		}
		err = tmpNode.Get()
		if err != nil {
			return nil, errors.New("cannot get Attribute Node: " + err.Error())
		}
		log.Printf(tmpNode.Tree())
		return tmpNode.NodeToAttribute()
	}

	ind := strings.LastIndex(path, ".")
	if ind > 0 {
		parent := path[:ind]
		attr.key = path[ind+1:]
		attr.path = v.base + parent

	} else {
		attr.key = path
		attr.path = v.base
	}

	if v.element.ExistsP(path+".schema") == false {
		return nil, errors.New("doesn't have a schema: not an attribute")
	}
	attr.atype = v.element.Path(path + ".schema.type").Data().(string)
	attr.element = v.element.Path(path)

	switch attr.atype {
	default:
		return nil, errors.New("type not supported")
	case "boolean":
		attr.value = v.element.Path(attr.key + ".value").Data()
	case "integer":
		attr.value = v.element.Path(attr.key + ".value").Data()
	case "string":
		attr.value = v.element.Path(attr.key + ".value").Data()
	case "number":
		attr.value = v.element.Path(attr.key + ".value").Data()
	}

	return attr, nil
}

// Method returns the sub method requested
func (v *Node) Method(subpath string) (*Method, error) {
	if subpath == "" {
		// return self
		return nil, errors.New("Node not a Method")
	}

	node := &(Method{})
	node.nc = v.nc
	if v.element.ExistsP(subpath) == false {
		node.base = subpath
		node.element = gabs.New()
	} else {
		node.element = v.element.Path(subpath)
		node.base = v.base + "." + subpath
	}

	return node, nil
}

// Discover returns all data tree from the path requested
func (v *Node) Discover(path string, filters string, timeout time.Duration) (*Node, error) {

	randpath := "SANDBOX." + genPassword()

	node := &(Node{})
	node.nc = v.nc
	node.element = gabs.New()
	node.base = path

	isempty := true

	sub, err := v.nc.Subscribe(randpath, func(m *nats.Msg) {
		newData, _ := gabs.ParseJSON([]byte(m.Data))
		node.element.Merge(newData)
		isempty = false
	})

	if err != nil {
		return nil, errors.New("Discover impossible")
	}

	v.nc.PublishRequest(path, randpath, []byte(filters))

	timer := time.NewTimer(timeout)
	<-timer.C

	sub.Unsubscribe()
	sub.Drain()

	if isempty == true {
		return nil, errors.New("Discover is empty")
	}

	return node, nil
}

func getAttributeType(value interface{}) string {

	if _, ok := value.(map[string]interface{}); ok {
		return "object"
	}
	switch value.(type) {
	default:
		return ""
	case bool:
		return "boolean"
	case int:
		return "integer"
	case string:
		return "string"
	case float32:
		return "number"
	case float64:
		return "number"
	}
}

// AddAttribute add an attribute to the current node
func (v *Node) AddAttribute(path string, value interface{}) error {
	attributeType := getAttributeType(value)
	if attributeType == "" {
		return errors.New("value not compatible")
	}

	elementJSON := gabs.New()
	elementJSON.SetP(value, path+".value")
	elementJSON.SetP(attributeType, path+".schema.type")

	v.nc.Publish(v.base+".add", elementJSON.Bytes())

	return nil
}

// AddNode add an attribute to the current node
func (v *Node) AddNode(path string, nodejson string) error {
	newNode, err := gabs.ParseJSON([]byte(nodejson))
	if err != nil {
		return errors.New("node not a json type")
	}

	elementJSON := gabs.New()
	elementJSON.SetP(newNode, path)

	v.nc.Publish(v.base+".add", elementJSON.Bytes())

	return nil
}

// AddMethod add a method to the current node
// returns the method handler
func (v *Node) AddMethod(path string, cb MethodCallback) (*Method, error) {

	elementJSON := gabs.New()
	elementJSON.SetP(nil, path+".return")
	elementJSON.SetP("object", path+".schema.type")
	elementJSON.SetP("object", path+".schema.properties.return.type")
	//v.element.Merge(elementJSON)

	//subnodes := strings.Split(path, ".")
	//v.element.SetP("object", "schema.properties."+subnodes[0]+".type")

	method := &(Method{})
	method.nc = v.nc
	method.element = elementJSON.Path(path)
	method.base = v.base + "." + path

	var err error
	method.sub, err = method.nc.Subscribe(method.base+".set", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		answer := cb(m.Data)
		if answer != nil {
			m.Respond(answer)
		}
	})
	if err != nil {
		log.Printf("Subscribe error: %v\n", err)
	}

	subListSet = append(subListSet, method.base+".set")

	v.nc.Publish(v.base+".add", elementJSON.Bytes())

	return nil, nil
}

// Set update the node info on vbus
func (v *Node) Set(nodejson string) error {
	updateNode, err := gabs.ParseJSON([]byte(nodejson))
	if err != nil {
		return errors.New("node not a json type")
	}

	v.element.Merge(updateNode)

	v.nc.Publish(v.base+".set", updateNode.Bytes())

	return nil
}

// Set update the node info on vbus
func (a *Attribute) Set(value interface{}) error {
	attributeType := getAttributeType(value)
	if attributeType == a.atype {
		return errors.New("value type incompatibility: " + a.atype + " expected")
	}

	a.element.Set(value, "value")

	msg, err := json.Marshal(value)

	if err != nil {
		return err
	}

	a.nc.Publish(a.path+"."+a.key+".value.set", msg)

	return nil
}

// Call update the method on vbus
func (m *Method) Call(msg []byte) error {
	m.nc.Publish(m.base+".set", msg)
	return nil
}

// Get request the node info on vbus
func (v *Node) Get() error {
	msg, err := v.nc.Request(v.base+".get", []byte(""), time.Second)
	if err != nil {
		return err
	}
	newData, _ := gabs.ParseJSON([]byte(msg.Data))
	v.element.Merge(newData)
	return nil
}

// Get request the node info on vbus
func (a *Attribute) Get() (interface{}, error) {
	msg, err := a.nc.Request(a.path+"."+a.key+".value.get", []byte(""), 30*time.Second)

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(msg.Data, &a.value)

	if err != nil {
		return nil, err
	}

	a.element.Set(a.value, "value")

	return a.value, nil
}

// Remove remove the node on vbus
func (v *Node) Remove() error {
	_, err := v.nc.Request(v.base+".remove", []byte(""), time.Second)
	if err != nil {
		return err
	}
	return nil
}

// SubscribeAdd subscribe to get call for that node
func (v *Node) SubscribeAdd(cb NodeCallback) error {

	if v.sub != nil {
		return errors.New("callback already existing for this Node")
	}

	var err error
	v.sub, err = v.nc.Subscribe(v.base+".add", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		newNode, err := gabs.ParseJSON(m.Data)
		if err != nil {
			log.Printf("node received not a json type")
		} else {
			v.element.Merge(newNode)
			//v.element.SetP("object", "schema.properties."+subnodes[0]+".type")
			for key := range newNode.ChildrenMap() {
				cb(key)
			}
		}
	})

	subListAdd = append(subListAdd, v.base+".add")

	return err
}

// SubscribeGet subscribe to get call for that node
func (v *Node) SubscribeGet(cb NodeCallback) error {

	if v.sub != nil {
		return errors.New("callback already existing for this Node")
	}

	var err error
	v.sub, err = v.nc.Subscribe(v.base+".get", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		answer := cb(string(m.Data))
		if answer != "" {
			m.Respond([]byte(answer))
		}
	})

	subListGet = append(subListGet, v.base+".get")

	return err
}

// SubscribeSet subscribe to set call for that node
func (v *Node) SubscribeSet(cb NodeCallback) error {

	if v.sub != nil {
		return errors.New("callback already existing for this Node")
	}

	var err error
	v.sub, err = v.nc.Subscribe(v.base+".set", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		updateNode, err := gabs.ParseJSON(m.Data)
		if err != nil {
			log.Printf("node received, not a json type")
		}
		v.element.Merge(updateNode)
		cb(string(m.Data))
	})

	subListSet = append(subListSet, v.base+".set")

	return err
}

// SubscribeRemove subscribe to all sub-node removal
func (v *Node) SubscribeRemove(cb NodeCallback) error {

	if v.sub != nil {
		return errors.New("callback already existing for this Node")
	}

	var err error
	v.sub, err = v.nc.Subscribe(v.base+".remove", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))
		answer := cb(string(m.Data))
		if answer != "" {
			m.Respond([]byte(answer))
		}
	})

	subListGet = append(subListGet, v.base+".remove")

	return err
}

// SubscribeAdd subscribe to get call for that attribute
// func (a *Attribute) SubscribeAdd(cb AttributeCallback) error {

// 	if a.sub != nil {
// 		return errors.New("callback already existing for this Attribute")
// 	}

// 	var err error
// 	a.sub, err = a.nc.Subscribe(a.path+".add", func(m *nats.Msg) {
// 		fmt.Printf("Received a message: %s\n", string(m.Data))

// 		switch t := a.atype; t {
// 		default:
// 			log.Printf("type not supported")
// 		case "boolean":
// 			a.value, err = strconv.ParseBool(string(m.Data))
// 		case "integer":
// 			a.value, err = strconv.ParseInt(string(m.Data), 10, 32)
// 		case "string":
// 			a.value = string(m.Data)
// 		case "number":
// 			a.value, err = strconv.ParseFloat(string(m.Data), 32)
// 		}

// 		if err == nil {
// 			a.parent.Set(a.value, a.key)
// 			cb(a.value)
// 		}
// 	})

// 	return err
// }

// SubscribeGet subscribe to get call for that attribute
func (a *Attribute) SubscribeGet(cb AttributeCallback) error {

	if a.sub != nil {
		return errors.New("callback already existing for this Attribute")
	}

	var err error
	a.sub, err = a.nc.Subscribe(a.path+"."+a.key+".value.get", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))

		answer := cb(m.Data)
		if answer != nil {
			msg, err := json.Marshal(answer)
			if err == nil {
				m.Respond(msg)
			}
		}
	})

	subListGet = append(subListGet, a.path+".value.get")

	return err
}

// SubscribeSet subscribe to set call for that attribute
func (a *Attribute) SubscribeSet(cb AttributeCallback) error {

	if a.sub != nil {
		return errors.New("callback already existing for this Attribute")
	}

	var err error
	a.sub, err = a.nc.Subscribe(a.path+"."+a.key+".value.set", func(m *nats.Msg) {
		fmt.Printf("Received a message: %s\n", string(m.Data))

		err := json.Unmarshal(m.Data, &a.value)

		if err == nil {
			a.element.Set(a.value, "value")
			cb(a.value)
		}
	})

	subListSet = append(subListSet, a.path+".value.set")

	return err
}

// Permission allow permission request to access specific path
func (v *Node) Permission(permission string) error {

	exist := false

	children := localConfig.S("client", "permissions", "subscribe").Children()
	for _, child := range children {
		if child.Data().(string) == permission {
			exist = true
			log.Printf("permission already existing")
		}
	}

	if exist == false {
		localConfig.ArrayAppend(permission, "client", "permissions", "subscribe")
		localConfig.ArrayAppend(permission, "client", "permissions", "publish")
		log.Printf("request permission for: " + permission)

		msg, err := v.nc.Request("system.authorization."+localConfig.S("vbus", "hostname").Data().(string)+"."+localConfig.S("client", "user").Data().(string)+".permissions.set", localConfig.Path("client.permissions").Bytes(), 5*time.Second)

		if err != nil {
			return errors.New("fail to request permission: " + err.Error())
		}

		value, err := strconv.ParseBool(string(msg.Data))
		if err != nil {
			return errors.New("permission request: wrong server answer")
		}
		if value == false {
			return errors.New("permission denied")
		}

		time.Sleep(5 * time.Second)

		ioutil.WriteFile(localConfig.S("client", "user").Data().(string)+".conf", localConfig.Bytes(), 0666)
	}

	return nil
}
