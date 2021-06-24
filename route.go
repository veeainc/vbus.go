package vBus

import (
	"fmt"
	"net"
	"strconv"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"github.com/pkg/errors"
)

const (
	vBusPort = 8421
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Route - local vBus discovery
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (c *ExtendedNatsClient) userFromJWT() (string, error) {
	return nkeys.ParseDecoratedJWT([]byte(c.jwt))
}

func (c *ExtendedNatsClient) signFromJWT([]byte) ([]byte, error) {
	return []byte{}, nil
}

// test access to server
func (c *ExtendedNatsClient) testRoute(url string) *nats.Conn {
	var conn *nats.Conn
	var err error
	if url == "" {
		return nil
	}
	if c.jwt != "" {
		conn, err = nats.Connect(url, nats.UserJWT(c.userFromJWT, c.signFromJWT))
	} else {
		conn, err = nats.Connect(url, nats.UserCredentials(c.creds))
	}
	_helpersLog.Debug("client remote IP: " + conn.ConnectedAddr())
	if err == nil {
		//defer conn.Close()
		return conn
	} else {
		return nil
	}
}

// find Vbus server - strategy 0: get from argument
func (c *ExtendedNatsClient) fromHubId(config *vbusRoute) (url []string, newHost string, e error) {
	if ret := net.ParseIP(c.remoteHostname); ret != nil {
		// already an ip address
		return []string{fmt.Sprintf("nats://%s:%d", c.remoteHostname, vBusPort)}, "", nil
	} else {
		addr, err := net.LookupIP(c.remoteHostname) // resolve hostname
		if err != nil {
			return []string{}, "", errors.Wrap(err, "Cannot resolve hostname")
		}
		return []string{fmt.Sprintf("nats://%v:%d", addr[0], vBusPort)}, "", nil
	}
}

// find Vbus server - strategy 1: get url from config file
func (c *ExtendedNatsClient) fromConfigFile(config *vbusRoute) (url []string, newHost string, e error) {
	if config == nil {
		return []string{}, "", nil
	}
	return []string{config.Url}, config.Hostname, nil
}

// find vbus server  - strategy 2: get url from ENV:VBUS_URL
func (c *ExtendedNatsClient) fromEnv(config *vbusRoute) (url []string, newHost string, e error) {
	return []string{c.env[envVbusUrl]}, "", nil
}

// find vbus server  - strategy 3: try default url client://hostname.service.veeamesh.local:8421
func (c *ExtendedNatsClient) fromLocalDNS(config *vbusRoute) (url []string, newHost string, e error) {
	url = []string{"nats://" + c.hostname + ".service.veeamesh.local:" + strconv.Itoa(vBusPort)}
	addr, err := net.LookupHost(c.hostname + "-host.service.veeamesh.local")
	if err == nil && len(addr) > 0 {
		c.networkIp = addr[0]
	}
	return url, "", nil
}

// find vbus server  - strategy 4: try global (MEN) url client://vbus.service.veeamesh.local:8421
func (c *ExtendedNatsClient) fromGlobalDNS(config *vbusRoute) (url []string, newHost string, e error) {
	if c.isvh == false {
		url = []string{"nats://vbus.service.veeamesh.local:" + strconv.Itoa(vBusPort)}
		newHost = ""
		addr, err := net.LookupHost("vbus.service.veeamesh.local")
		if err == nil && len(addr) > 0 {
			newHost = getHostnameFromvBus(url[0], addr[0])
			c.networkIp = addr[0]
		}
	}
	return url, newHost, nil
}

func (c *ExtendedNatsClient) discovervBusRoute(config *vbusRoute) (client *nats.Conn, serverUrl string, newHost string, e error) {
	findServerUrlStrategies := []func(config *vbusRoute) (url []string, newHost string, e error){
		c.fromHubId,
		c.fromEnv,
		c.fromConfigFile,
		c.fromLocalDNS,
		c.fromGlobalDNS,
	}

	success := false
	var urls []string

	for _, strategy := range findServerUrlStrategies {
		if success {
			break
		}

		urls, newHost, e = strategy(config)
		for _, url := range urls {
			client = c.testRoute(url)
			if client != nil {
				_natsLog.WithFields(LF{"discover": getFunctionName(strategy), "url": url}).Info("url found")
				success = true
				serverUrl = url
				break
			} else {
				_natsLog.WithFields(LF{"discover": getFunctionName(strategy), "url": url}).Debug("cannot find a valid url")
			}
		}

		if len(urls) == 0 {
			_natsLog.WithFields(LF{"discover": getFunctionName(strategy)}).Debug("strategy returned no results")
		}
	}

	if !success {
		return nil, "", "", errors.New("cannot find a valid vBus url")
	}

	return
}
