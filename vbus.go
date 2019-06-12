package vbus

import (
	//    "fl
	//"errors"
	"context"
	"crypto/rand"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	gabs "github.com/Jeffail/gabs"
	"github.com/grandcat/zeroconf"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/bcrypt"
)

type substruct struct {
	name string
	sub  *nats.Subscription
}

type Hand struct {
	nc      *nats.Conn
	element *gabs.Container
	sublist []substruct
}

type DataHandler func(subject string, reply string, msg []byte)

const (
	// Make sure the password is reasonably long to generate enough entropy.
	PasswordLength = 22
	// Common advice from the past couple of years suggests that 10 should be sufficient.
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

func test_vbus_url(url string) bool {
	vbus_test := false
	tmp, err := nats.Connect(url, nats.UserInfo("anonymous", "anonymous"))
	if err == nil {
		vbus_test = true
		tmp.Close()
	}
	return vbus_test
}

// New - Create a new connection to vbus.
func Open(id string) (*Hand, error) {
	// create vbus element struct
	v := &(Hand{})

	// check if we already have a vbus config file
	log.Printf("check if we already have a vbus config file\n")
	rootfolder := os.Getenv("HOME")
	os.MkdirAll(rootfolder+"/vbus", os.ModePerm)
	_, err := os.Stat(rootfolder + "/vbus/" + id + ".conf")
	if err != nil {
		log.Printf("create new configuration file for " + id + "\n")
		v.element = gabs.New()
		//create user
		v.element.Set(id, "element", "path")
		v.element.Set(id, "element", "name")
		hostname, _ := os.Hostname()
		v.element.Set(hostname, "element", "host")
		v.element.Set(hostname+"."+id, "element", "uuid")
		privatekey := genPassword()
		publickey, _ := bcrypt.GenerateFromPassword([]byte(privatekey), DefaultCost)
		v.element.Set(hostname+"."+id, "auth", "user")
		v.element.Set(string(publickey), "auth", "password")
		v.element.Array("auth", "permissions", "subscribe")
		v.element.Array("auth", "permissions", "publish")
		v.element.Set(string(privatekey), "key", "private")

		log.Printf(v.element.StringIndent("", " "))
	} else {
		log.Printf("load existing configuration file for " + id + "\n")
		file, _ := ioutil.ReadFile(rootfolder + "/vbus/" + id + ".conf")
		v.element, _ = gabs.ParseJSON([]byte(file))
		log.Printf(v.element.StringIndent("", " "))
	}

	var vbus_url string
	// find vbus server  - strategy 1: get url from config file
	if vbus_url == "" {
		vbus_url_exists := v.element.Exists("vbus", "url")
		if vbus_url_exists == true {
			if test_vbus_url(v.element.Search("vbus", "url").Data().(string)) == true {
				vbus_url = v.element.Search("vbus", "url").Data().(string)
				log.Printf("url from config file ok: " + vbus_url + "\n")
			} else {
				log.Printf("url from config file hs: " + v.element.Search("vbus", "url").Data().(string) + "\n")
			}
		}
	}

	// find vbus server  - strategy 2: get url from ENV:VBUS_URL
	if vbus_url == "" {
		env_vbus_url := os.Getenv("VBUS_URL")
		if env_vbus_url != "" {
			if test_vbus_url(env_vbus_url) == true {
				vbus_url = env_vbus_url
				log.Printf("url from ENV ok: " + env_vbus_url + "\n")
			} else {
				log.Printf("url from ENV hs: " + env_vbus_url + "\n")
			}
		}
	}

	// find vbus server  - strategy 3: try default url nats://hostname:21400
	if vbus_url == "" {
		hostname, _ := os.Hostname()
		default_vbus_url := "nats://" + hostname + ".local:21400"
		if test_vbus_url(default_vbus_url) == true {
			vbus_url = default_vbus_url
			log.Printf("url from default ok: " + default_vbus_url + "\n")
		} else {
			log.Printf("url from default hs: " + default_vbus_url + "\n")
		}
	}

	// find vbus server  - strategy 4: find it using avahi
	if vbus_url == "" {
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
				if "vbus" == strings.Split(entry.Instance, "/")[0] {
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
			if test_vbus_url(routesStr) == true {
				vbus_url = routesStr
				log.Printf("url from discovery ok: " + routesStr + "\n")
			} else {
				log.Printf("url from discovery hs: " + routesStr + "\n")
			}
		default:
			log.Println("no service found")
		}
	}

	// record config file
	if vbus_url == "" {
		panic("no valid url vbus found")
	}

	v.element.Set(vbus_url, "vbus", "url")
	ioutil.WriteFile(rootfolder+"/vbus/"+id+".conf", v.element.Bytes(), 0666)

	// connect to vbus server
	directconnect := true
	log.Printf("open connection with local nats\n")
	v.nc, err = nats.Connect(v.element.Search("vbus", "url").Data().(string), nats.UserInfo(v.element.Search("element", "uuid").Data().(string), v.element.Search("key", "private").Data().(string)))
	if err != nil {
		// maybe user doesn't exist yet
		v.nc, err = nats.Connect(v.element.Search("vbus", "url").Data().(string), nats.UserInfo("anonymous", "anonymous"))

		if err != nil {
			log.Fatalf("Can't connect: %v\n", err)
		} else {
			directconnect = false
		}
	}
	//defer nc.Close()

	log.Printf("publish user \n")
	v.nc.Publish("system.auth.adduser", v.element.Search("auth").Bytes())

	if directconnect == false { // since we had to connect as an anonymous, we now, reconnect with true credentials
		v.nc.Close()
		time.Sleep(500 * time.Millisecond)
		v.nc, err = nats.Connect(v.element.Search("vbus", "url").Data().(string), nats.UserInfo(v.element.Search("element", "uuid").Data().(string), v.element.Search("key", "private").Data().(string)))
		if err != nil {
			log.Fatalf("Can't connect: %v\n", err)
		}
	}

	log.Printf("publish element\n")
	v.nc.Publish("system.db.newelement", v.element.Search("element").Bytes())

	return v, nil
}

func (v *Hand) Close() {
	v.nc.Drain()
	v.nc.Close()
}

func (v *Hand) List(filter_json []byte) []byte {
	msg, _ := v.nc.Request("system.db.getelementlist", filter_json, 100*time.Millisecond)
	//log.Printf("list\n%s\n", string(msg.Data))
	return msg.Data
}

func (v *Hand) Permission_Subscribe(permission string) {

	exist := false

	children, _ := v.element.S("auth", "permissions", "subscribe").Children()
	for _, child := range children {
		if child.Data().(string) == permission {
			exist = true
		}
	}

	if exist == false {
		v.element.ArrayAppend(permission, "auth", "permissions", "subscribe")
		log.Printf("request subscribe permission for: " + permission)
		v.nc.Publish("system.auth.addpermissions", v.element.Search("auth").Bytes())
	}
}

func (v *Hand) Permission_Publish(permission string) {

	exist := false

	children, _ := v.element.S("auth", "permissions", "publish").Children()
	for _, child := range children {
		if child.Data().(string) == permission {
			exist = true
		}
	}

	if exist == false {
		v.element.ArrayAppend(permission, "auth", "permissions", "publish")
		log.Printf("request publish permission for: " + permission)
		v.nc.Publish("system.auth.addpermissions", v.element.Search("auth").Bytes())
	}
}

func (v *Hand) Request(dest string, data []byte) []byte {
	msg, _ := v.nc.Request(dest, data, 100*time.Millisecond)
	//log.Printf("list\n%s\n", string(msg.Data))
	return msg.Data
}

func (v *Hand) Publish(dest string, message []byte) error {
	//log.Printf("send msg to %s\n", dest)
	return v.nc.Publish(dest, message)
}

func (v *Hand) Subscribe(from string, format string, cb DataHandler) error {

	if v.element.ExistsP("subscribe") == false {
		v.element.Array("subscribe")
	}

	analysesub := strings.Split(from, ".")
	methode := analysesub[len(analysesub)-1]

	new_sub := gabs.New()
	new_sub.Set(methode, "methode")
	new_sub.Set(format, "format")
	v.element.ArrayAppend(new_sub.Data(), "element", "subscribe")

	log.Printf("publish element\n")
	v.nc.Publish("system.db.newelement", v.element.Search("element").Bytes())

	//log.Printf("send msg to %s\n", dest)
	sub, err := v.nc.Subscribe(from, func(m *nats.Msg) {
		//printMsg(m)
		cb(m.Subject, m.Reply, m.Data)
	})

	newsub := substruct{from, sub}

	v.sublist = append(v.sublist, newsub)

	return err
}

func (v *Hand) Unsubscribe(from string) {

	if v.sublist != nil {

		toremove := 1

		for toremove != -1 {

			toremove = -1

			for i := 0; i < len(v.sublist); i++ {
				if v.sublist[i].name == from {
					toremove = i
				}
			}

			if toremove != -1 {
				v.sublist[toremove].sub.Unsubscribe()
				v.sublist = append(v.sublist[:toremove], v.sublist[toremove+1:]...)
			}
		}
	}
}
