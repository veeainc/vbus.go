package vBus

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"github.com/godbus/dbus"
	"github.com/grandcat/zeroconf"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"github.com/robpike/filter"
	"github.com/sirupsen/logrus"
	"math/big"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	passwordLength = 22
)

// Represents a Json object
type JsonObj = map[string]interface{}
// Represents any Json
type JsonAny = interface{}

var log = logrus.New()


func toVbus(obj interface{}) []byte {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(obj)
	// compact json
	compacted := new(bytes.Buffer)
	_ = json.Compact(compacted, buf.Bytes())
	return compacted.Bytes()
}

func fromVbus(data []byte) (interface{}, error) {
	if data != nil {
		var input interface{}
		err := json.Unmarshal(data, &input)
		return input, err
	}
	return nil, nil
}

func mergeJsonObjs(obj ...JsonObj) JsonObj {
	res := JsonObj{}
	for _, m := range obj {
		for k, v := range m {
			res[k] = v
		}
	}
	return res
}

func isStrEmpty(str string) bool { return str == "" }

func isStrNotEmpty(str string) bool { return !isStrEmpty(str) }

// join path segment and skip empty string
func joinPath(segments ...string) string {
	return strings.Join(filter.Choose(segments, isStrNotEmpty).([]string), ".")
}

// Retrieve the hostname
func getHostname() string {
	hostnameLocal, _ := os.Hostname()
	hostname := strings.Split(hostnameLocal, ".")[0]

	dbusConn, err := dbus.SystemBus()
	if err != nil {
		log.Warn("cannot connect to dbus: ", err)
	} else {
		obj := dbusConn.Object("io.veea.VeeaHub.Info", "/io/veea/VeeaHub/Info")
		call := obj.Call("io.veea.VeeaHub.Info.Hostname", 0)
		if call.Err != nil {
			log.Warn("Failed to get hostname on dbus:", call.Err)
		}
		err = call.Store(&hostname)
		if err != nil {
			log.Warn("unable to store value: ", err)
		}
	}

	return hostname
}

func generatePassword() (string, error) {
	var ch = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!@$#%^&*()")
	b := make([]byte, passwordLength)
	max := big.NewInt(int64(len(ch)))
	for i := range b {
		ri, err := rand.Int(rand.Reader, max)
		if err != nil {
			return "", errors.Wrap(err, "Error producing random integer")
		}
		b[i] = ch[int(ri.Int64())]
	}
	return string(b), nil
}

// fileExists checks if a file exists and is not a directory before we
// try using it to prevent further errors.
func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

// Try to determine if we re running on a Veaa hub
func isHub() bool {
	dbusConn, err := dbus.SystemBus()
	if err != nil {
		return false
	}

	obj := dbusConn.Object("io.veea.VeeaHub.Info", "/io/veea/VeeaHub/Info")
	call := obj.Call("io.veea.VeeaHub.Info.Hostname", 0)
	if call.Err != nil {
		return false
	}

	return true
}

func zeroconfSearch() (url string, newHost string, e error) {
	log.Debug("find vbus on network\n")
	resolver, err := zeroconf.NewResolver(nil)
	if err != nil {
		return "", "", errors.Wrap(err, "Failed to initialize resolver")
	}

	serviceList := make(chan *zeroconf.ServiceEntry, 1)
	entries := make(chan *zeroconf.ServiceEntry)
	go func(results <-chan *zeroconf.ServiceEntry) {
		for entry := range results {
			log.Debug(entry)
			if "vBus" == entry.Instance {
				// next step compare host_name to choose the same one than the service if available
				log.Debug("vbus found !!")
				serviceList <- entry
			}
		}
		log.Debug("No more entries.")
	}(entries)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err = resolver.Browse(ctx, "_nats._tcp", "local.", entries)
	if err != nil {
		return "", "", errors.Wrap(err, "Failed to browse")
	}

	<-ctx.Done()

	select {
	case firstservice := <-serviceList:
		routesStr := "client://" + firstservice.AddrIPv4[0].String() + ":" + strconv.Itoa(firstservice.Port)
		log.Println("vbus url discovered is: " + routesStr)
		if testVbusUrl(routesStr) == true {
			url = routesStr
			log.Debug("url from discovery ok: " + routesStr + "\n")
			hostIPParsed := strings.Split(firstservice.Text[0], "=")
			if hostIPParsed[0] == "host" {
				hostIP := hostIPParsed[1]
				log.Debug("hostIP retrieved from mDns: " + hostIP)
			}
			if isHub() == false {
				// try to retrieve real VH hostname case we are not on a VH
				hostnameParsed := strings.Split(firstservice.Text[1], "=")
				if hostnameParsed[0] == "hostname" {
					newHost = hostnameParsed[1]
					log.Debug("hostname retrived from mDns: " + newHost)
				}
			}
		} else {
			log.Debug("url from discovery hs: " + routesStr + "\n")
		}
	default:
		log.Println("no service found")
	}
	return
}

// Check if the provided Vbus url is valid
func testVbusUrl(url string) bool {
	if url == "" {
		return false
	}
	conn, err := nats.Connect(url, nats.UserInfo(anonymousUser, anonymousUser))
	if err == nil {
		defer conn.Close()
		return true
	} else {
		return false
	}
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}

// Check if an interface{} is a map and contains the provided key.
func hasKey(obj interface{}, key string) bool {
	if m, ok := obj.(JsonObj); ok {
		if _, found := m[key]; found {
			return true
		}
	}
	return false
}

func isSlice(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Slice
}

func isMap(v interface{}) bool {
	return reflect.TypeOf(v).Kind() == reflect.Map
}

// Call a method from interface{} arguments.
// Assume only one value returned or none.
func invokeFunc(fn interface{}, args ...interface{}) (ret interface{}, err error) {
	// Recover in case of panic
	defer func() {
		if r := recover(); r!= nil {
			err = errors.New(r.(string))
		}
	}()

	fnVal := reflect.ValueOf(fn)
	fnType := reflect.TypeOf(fn)
	nInputArg := fnType.NumIn()

	// check args count
	if nInputArg != len(args) {
		return nil, errors.New(fmt.Sprintf("wrong number of args, expected %v, given %v", nInputArg, len(args)))
	}

	rargs := make([]reflect.Value, len(args))
	for i, a := range args {
		realArgType := fnType.In(i) // The real arg type

		if a == nil {
			rargs[i] = reflect.Zero(reflect.TypeOf((*error)(nil)).Elem()) // create a nil arg
		} else {
			rargs[i] = reflect.ValueOf(a).Convert(realArgType)
		}
	}
	returnVal := fnVal.Call(rargs)

	if len(returnVal) > 0 {
		ret = returnVal[0].Interface()
	}

	return
}

// Check if two slices are equal.
func sliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// Get the last part of a slice.
func lastString(ss []string) string {
	return ss[len(ss)-1]
}

// Split a string and return the last split.
func lastSplit(s string, split string) string {
	return lastString(strings.Split(s, split))
}

// Test if a string is in a string slice.
func stringInSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}

// Test if its a Nats wildcard path.
func isWildcardPath(parts ...string) bool {
	return stringInSlice("*", parts)
}

// Prepend a string on a slice.
func prepend(s string, ss []string) []string {
	return append([]string{s}, ss...)
}

// Convert a Json object to pretty Json.
func ToPrettyJson(obj JsonAny) string {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "    ")
	_ = enc.Encode(obj)
	return buf.String()
}

// Find a sub element in a Json object.
func getPathInObj(o JsonObj, segments ...string) JsonObj {
	root := o
	for _, segment := range segments {
		if v, ok := o[segment]; ok { // element exists
			if c, ok := v.(JsonObj); ok {
				root = c
			} else {
				return nil
			}
		} else {
			return nil
		}
	}
	return root
}













