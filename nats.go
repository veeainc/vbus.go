package vBus

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"golang.org/x/crypto/bcrypt"
)

const (
	envUserConfig = "USER_CONFIG"
	envHome       = "HOME"
	envVbusPath   = "VBUS_PATH"
	envVbusUrl    = "VBUS_URL"
	anonymousUser = "anonymous"
	defaultCost   = 11
)

var _natsLog = getNamedLogger()

type ExtendedNatsClient struct {
	isvh           bool              // true if running in a veeahub
	hostname       string            // client hostname
	remoteHostname string            // remote client server hostname
	id             string            // app identifier
	env            map[string]string // environment variables
	rootFolder     string            // config folder root
	client         *nats.Conn        // client handle
	networkIp      string            // public network ip, populated during mdns discovery
}

// A Nats callback, that take data and path segment
type NatsCallback = func(data interface{}, segments []string) interface{}

// ExtendedNatsClient options.
type natsConnectOptions struct {
	HubId      string
	Login      string
	Password   string
	Pwd        string // anonymous password
	Permission []string
}

type UserConfigStruct struct {
	Password string `json:"vBusPwd"`
}

// Check if option contains user information.
func (o natsConnectOptions) hasUser() bool {
	return o.Login != "" && o.Password != ""
}

// Check if option contains permission.
func (o natsConnectOptions) hasPermission() bool {
	if len(o.Permission) > 0 {
		return true
	}
	return false
}

// Option is a function on the options for a connection.
type natsConnectOption func(*natsConnectOptions)

// Add the hub id option.
func HubId(hubId string) natsConnectOption {
	return func(o *natsConnectOptions) {
		o.HubId = hubId
	}
}

// Connect with specified user.
func WithUser(login, pwd string) natsConnectOption {
	return func(o *natsConnectOptions) {
		o.Login = login
		o.Password = pwd
	}
}

// Add vBus password
func WithPwd(pwd string) natsConnectOption {
	return func(o *natsConnectOptions) {
		o.Pwd = pwd
	}
}

// Connect with specified permission.
func WithPermission(permission string) natsConnectOption {
	return func(o *natsConnectOptions) {
		o.Permission = append(o.Permission, permission)
	}
}

// Connect with specified permission.
func WithPermissionSlice(permissions []string) natsConnectOption {
	return func(o *natsConnectOptions) {
		for _, permission := range permissions {
			o.Permission = append(o.Permission, permission)
		}
	}
}

// Constructor when the server and the client are running on the same system (same hostname).
func NewExtendedNatsClient(appDomain, appId string) *ExtendedNatsClient {
	hostname, isvh := getHostname()
	hostname = sanitizeNatsSegment(hostname)

	client := &ExtendedNatsClient{
		isvh:           isvh,
		hostname:       hostname,
		remoteHostname: hostname,
		id:             fmt.Sprintf("%s.%s", appDomain, appId),
		env:            readEnvVar(),
		client:         nil,
	}

	client.rootFolder = client.env[envVbusPath]

	// generate a default location is not specified
	if client.rootFolder == "" {
		client.rootFolder = path.Join(client.env[envHome], "vbus")
	}

	return client
}

func readEnvVar() map[string]string {
	return map[string]string{
		envUserConfig: os.Getenv(envUserConfig),
		envHome:       os.Getenv(envHome),
		envVbusPath:   os.Getenv(envVbusPath),
		envVbusUrl:    os.Getenv(envVbusUrl),
	}
}

// Get resolved hostname.
func (c *ExtendedNatsClient) GetHostname() string {
	return c.hostname
}

// Get application id.
func (c *ExtendedNatsClient) GetId() string {
	return c.id
}

// Try to connect.
// Available options: vBus.HubId(), vBus.WithUser()
func (c *ExtendedNatsClient) Connect(options ...natsConnectOption) error {
	// retrieve options
	opts := natsConnectOptions{}
	for _, opt := range options {
		opt(&opts)
	}

	if opts.Pwd == "" {
		opts.Pwd = "anonymous"
		userConfig := "/usr/local/config/defaults/user-config.json"
		if c.env[envUserConfig] != "" {
			userConfig = c.env[envUserConfig]
		}
		_natsLog.Debug("user config file: " + userConfig)
		file, e := ioutil.ReadFile(userConfig)
		if e != nil {
			_natsLog.Debug("no existing user config file")
		} else {
			var userConfigS UserConfigStruct
			err := json.Unmarshal(file, &userConfigS)
			if err != nil {
				_natsLog.Debug("cannot parse user config file")
			} else {
				_natsLog.Debug("user config password found")
				opts.Pwd = userConfigS.Password
			}
		}
	}

	if opts.HubId != "" {
		c.remoteHostname = sanitizeNatsSegment(opts.HubId)
	} else {
		c.remoteHostname = sanitizeNatsSegment(c.hostname)
	}

	if opts.hasUser() {
		url, newHost, err := c.findVbusUrl(&configuration{}, opts.Pwd)
		if err != nil {
			return errors.Wrap(err, "cannot find vbus url")
		}

		// check if we need to update remote host
		if newHost != "" {
			c.remoteHostname = sanitizeNatsSegment(newHost)
		}

		// check that we have a real hostname
		// overwise we replace it with the remote hostname
		// if _, err := strconv.ParseInt(c.hostname, 16, 0); err == nil {
		// 	fmt.Printf("hostname: %q is a number. Probably a random\nSo replace it with the remote hostname: %s", c.hostname, c.remoteHostname)
		// 	c.hostname = c.remoteHostname
		// 	config.Client.User = fmt.Sprintf("%s.%s", c.id, c.hostname)
		// }

		// connect with provided user info
		c.client, err = nats.Connect(url,
			nats.UserInfo(opts.Login, opts.Password),
			nats.Name(opts.Login))
		return err
	} else {
		config, err := c.readOrGetDefaultConfig()
		if err != nil {
			return errors.Wrap(err, "cannot retrieve configuration")
		}

		url, newHost, err := c.findVbusUrl(config, opts.Pwd)
		if err != nil {
			return errors.Wrap(err, "cannot find vbus url")
		}

		// update the config file with the new url
		config.Vbus.Url = url

		if c.networkIp != "" {
			config.Vbus.NetworkIp = c.networkIp
		}

		// check if we need to update remote host
		if newHost != "" {
			c.remoteHostname = sanitizeNatsSegment(newHost)
		}
		config.Vbus.Hostname = c.remoteHostname

		// check that we have a real hostname
		// overwise we replace it with the remote hostname
		// if _, err := strconv.ParseInt(c.hostname, 16, 0); err == nil {
		// 	fmt.Printf("hostname: %q is a number. Probably a random\nSo replace it with the remote hostname: %s", c.hostname, c.remoteHostname)
		// 	c.hostname = c.remoteHostname
		// 	config.Client.User = fmt.Sprintf("%s.%s", c.id, c.hostname)
		// }

		if opts.hasPermission() {
			for _, permission := range opts.Permission {
				addPermission(config, permission)
			}
		}

		err = c.saveConfigFile(config)
		if err != nil {
			return errors.Wrap(err, "cannot save configuration")
		}

		// try to connect directly and push user if fail
		// connect with user in config file
		c.client, err = nats.Connect(url,
			nats.UserInfo(config.Client.User, config.Key.Private),
			nats.Name(config.Client.User))
		if err != nil {
			_natsLog.Debug("unable to connect with user in config file, adding it")

			err = c.publishUser(url, opts.Pwd, config.Client)
			if err != nil {
				return errors.Wrap(err, "cannot create user")
			}
			time.Sleep(2000 * time.Millisecond)

			// connect with user in config file
			c.client, err = nats.Connect(url,
				nats.UserInfo(config.Client.User, config.Key.Private),
				nats.Name(config.Client.User))
		} else {
			time.Sleep(1000 * time.Millisecond)
			// either we pushed the default permission with the full account publish (if)
			// or we resend it just in case... (here)

			natsPath := fmt.Sprintf("system.authorization.%s.%s.%s.permissions.set", c.remoteHostname, c.id, c.hostname)
			c.Request(natsPath, config.Client.Permissions, Timeout(10*time.Second), WithoutId(), WithoutHost())

			time.Sleep(2000 * time.Millisecond)
		}

		_natsLog.Debug("connected")
		return err
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Advanced Nats Functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Advanced Nats methods options
type advOptions struct {
	Timeout  time.Duration
	WithId   bool
	WithHost bool
}

// Option is a function on the options for a connection.
type AdvOption func(*advOptions)

// Set optional timeout
func Timeout(t time.Duration) AdvOption {
	return func(o *advOptions) {
		o.Timeout = t
	}
}

// Do not include this service id before the provided path
func WithoutId() AdvOption {
	return func(o *advOptions) {
		o.WithId = false
	}
}

// Do not include this service host before the provided path
func WithoutHost() AdvOption {
	return func(o *advOptions) {
		o.WithHost = false
	}
}

// Retrieve all options to a struct
func getAdvOptions(advOpts ...AdvOption) advOptions {
	// set default options
	opts := advOptions{
		Timeout:  1000 * time.Millisecond,
		WithHost: true,
		WithId:   true,
	}
	for _, opt := range advOpts {
		opt(&opts)
	}
	return opts
}

// Compute the path with some options
func (c *ExtendedNatsClient) getPath(base string, opts advOptions) (path string) {
	path = base
	if opts.WithHost {
		path = joinPath(c.hostname, base)
	}
	if opts.WithId {
		path = joinPath(c.id, path)
	}
	return
}

func (c *ExtendedNatsClient) Request(base string, data interface{}, advOpts ...AdvOption) (interface{}, error) {
	opts := getAdvOptions(advOpts...)
	natsPath := c.getPath(base, opts)
	msg, err := c.client.Request(natsPath, toVbus(data), opts.Timeout)
	if err != nil {
		return nil, errors.Wrap(err, "cannot send request")
	}
	return fromVbus(msg.Data)
}

func (c *ExtendedNatsClient) Publish(base string, data interface{}, advOpts ...AdvOption) error {
	opts := getAdvOptions(advOpts...)
	natsPath := c.getPath(base, opts)
	return c.client.Publish(natsPath, toVbus(data))
}

// Utility method that automatically parse subject wildcard and chevron to arguments.
// If a value is returned, it is published on the reply subject.
func (c *ExtendedNatsClient) Subscribe(base string, cb NatsCallback, advOpts ...AdvOption) (*nats.Subscription, error) {
	opts := getAdvOptions(advOpts...)
	natsPath := c.getPath(base, opts)
	// create a regex that capture wildcard and chevron in path
	regex := strings.Replace(natsPath, ".", `\.`, -1)  // escape dot
	regex = strings.Replace(regex, "*", `([^.]+)`, -1) // capture wildcard
	regex = strings.Replace(regex, ">", `(.+)`, -1)    // capture chevron
	r := regexp.MustCompile(regex)

	return c.client.Subscribe(natsPath, func(msg *nats.Msg) {
		go func(cb NatsCallback, r *regexp.Regexp, msg *nats.Msg) {
			m := r.FindStringSubmatch(msg.Subject)
			// Parse data
			data, err := fromVbus(msg.Data)
			if err != nil {
				logrus.Warnf("error while calling subscribe callback: %v", err.Error())
				return
			}

			res, err := invokeFunc(cb, data, m[1:])
			if err != nil {
				_natsLog.WithField("error", err).Warn("cannot call user callback")
				return
			}

			// if there is a reply subject, use it to send response
			if isStrNotEmpty(msg.Reply) {
				err = c.client.Publish(msg.Reply, toVbus(res))
				if err != nil {
					_natsLog.WithField("error", err).Warn("error while sending response")
					return
				}
			}
		}(cb, r, msg)
	})
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Permissions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Add permission to config file
func addPermission(config *configuration, permission string) bool {
	fileChanged := false

	if !contains(config.Client.Permissions.Subscribe, permission) {
		config.Client.Permissions.Subscribe = append(config.Client.Permissions.Subscribe, permission)
		fileChanged = true
	}

	if !contains(config.Client.Permissions.Publish, permission) {
		config.Client.Permissions.Publish = append(config.Client.Permissions.Publish, permission)
		fileChanged = true
	}

	return fileChanged
}

// Ask for the specified permission.
func (c *ExtendedNatsClient) AskPermission(permission string) (bool, error) {
	if permission == "" {
		return false, errors.New("permission path empty")
	}

	config, err := c.readOrGetDefaultConfig()
	if err != nil {
		return false, errors.Wrap(err, "cannot read config")
	}

	fileChanged := addPermission(config, permission)

	if fileChanged {
		_natsLog.Debug("permissions changed, sending them to server")

		natsPath := fmt.Sprintf("system.authorization.%s.%s.%s.permissions.set", c.remoteHostname, c.id, c.hostname)
		resp, err := c.Request(natsPath, config.Client.Permissions, Timeout(10*time.Second), WithoutId(), WithoutHost())
		if err != nil {
			return false, err
		}

		if resp.(bool) == false {
			_natsLog.Debug("cannot add permission on server")
			return false, nil
		}

		err = c.saveConfigFile(config)
		if err != nil {
			return false, errors.Wrap(err, "cannot save config")
		}

		return resp.(bool), nil
	}

	_natsLog.Debug("permissions are already ok")
	return true, nil
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Authentication
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Publish on Vbus the user described in configuration.
func (c *ExtendedNatsClient) publishUser(url, pwd string, config ClientConfig) error {
	conn, err := nats.Connect(url, nats.UserInfo(anonymousUser, pwd))
	if err != nil {
		return errors.Wrap(err, "cannot connect to client server")
	}
	defer conn.Close()

	data := toVbus(config)
	err = conn.Publish(fmt.Sprintf("system.authorization.%s.add", c.remoteHostname), data)
	if err != nil {
		return errors.Wrap(err, "error while publishing")
	}

	return nil
}

// Create a new user on vbus.
// Can be user with vBus.HubId() option.
// func (c *ExtendedNatsClient) CreateUser(userConfig ClientConfig, options ...natsConnectOption) error {
// 	// retrieve options
// 	opts := natsConnectOptions{}
// 	for _, opt := range options {
// 		opt(&opts)
// 	}

// 	if opts.HubId != "" {
// 		c.remoteHostname = sanitizeNatsSegment(opts.HubId)
// 	} else {
// 		c.remoteHostname = sanitizeNatsSegment(c.hostname)
// 	}

// 	url, _, err := c.findVbusUrl(&configuration{}) // empty configuration
// 	if err != nil {
// 		return errors.Wrap(err, "cannot find vbus url")
// 	}

// 	return c.publishUser(url, userConfig)
// }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Find server url strategies
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// find Vbus server - strategy 0: get from argument
func (c *ExtendedNatsClient) getFromHubId(config *configuration) (url []string, newHost string, e error) {
	if ret := net.ParseIP(c.remoteHostname); ret != nil {
		// already an ip address
		return []string{fmt.Sprintf("tls://%s:21400", c.remoteHostname)}, "", nil
	} else {
		addr, err := net.LookupIP(c.remoteHostname) // resolve hostname
		if err != nil {
			return []string{}, "", errors.Wrap(err, "Cannot resolve hostname")
		}
		return []string{fmt.Sprintf("tls://%v:21400", addr[0])}, "", nil
	}
}

// find Vbus server - strategy 1: get url from config file
func (c *ExtendedNatsClient) getFromConfigFile(config *configuration) (url []string, newHost string, e error) {
	return []string{config.Vbus.Url}, config.Vbus.Hostname, nil
}

// find vbus server  - strategy 2: get url from ENV:VBUS_URL
func (c *ExtendedNatsClient) getFromEnv(config *configuration) (url []string, newHost string, e error) {
	return []string{c.env[envVbusUrl]}, "", nil
}

// find vbus server  - strategy 3: try default url client://hostname.service.veeamesh.local:21400
func (c *ExtendedNatsClient) getlocalDefault(config *configuration) (url []string, newHost string, e error) {
	url = []string{"tls://" + c.hostname + ".service.veeamesh.local:21400"}
	addr, err := net.LookupHost(c.hostname + "-host.service.veeamesh.local")
	if err == nil && len(addr) > 0 {
		c.networkIp = addr[0]
	}
	return url, "", nil
}

// find vbus server  - strategy 4: find it using avahi
func (c *ExtendedNatsClient) getFromZeroconf(config *configuration) (url []string, newHost string, e error) {
	if c.isvh == false {
		url, newHost, c.networkIp, e = zeroconfSearch()
	}
	return
}

// find vbus server  - strategy 5: try global (MEN) url client://vbus.service.veeamesh.local:21400
func (c *ExtendedNatsClient) getglobalDefault(config *configuration) (url []string, newHost string, e error) {
	if c.isvh == false {
		url = []string{"tls://vbus.service.veeamesh.local:21400"}
		newHost = ""
		addr, err := net.LookupHost("vbus.service.veeamesh.local")
		if err == nil && len(addr) > 0 {
			newHost = getHostnameFromvBus(url[0], addr[0])
			c.networkIp = addr[0]
		}
	}
	return url, newHost, nil
}

func (c *ExtendedNatsClient) findVbusUrl(config *configuration, pwd string) (serverUrl string, newHost string, e error) {
	findServerUrlStrategies := []func(config *configuration) (url []string, newHost string, e error){
		//c.getFromHubId,
		c.getFromEnv,
		c.getFromConfigFile,
		c.getlocalDefault,
		//c.getFromZeroconf,
		c.getglobalDefault,
	}

	success := false
	var urls []string

	for _, strategy := range findServerUrlStrategies {
		if success {
			break
		}

		urls, newHost, e = strategy(config)
		for _, url := range urls {
			if testVbusUrl(url, pwd) {
				_natsLog.WithFields(LF{"strategy": getFunctionName(strategy), "url": url}).Info("url found")
				success = true
				serverUrl = url
				break
			} else {
				_natsLog.WithFields(LF{"strategy": getFunctionName(strategy), "url": url}).Debug("cannot find a valid url")
			}
		}

		if len(urls) == 0 {
			_natsLog.WithFields(LF{"strategy": getFunctionName(strategy)}).Debug("strategy returned no results")
		}
	}

	if !success {
		return "", "", errors.New("cannot find a valid Vbus url")
	}

	return
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// Permission configuration.
type PermConfig struct {
	Subscribe []string `json:"subscribe"`
	Publish   []string `json:"publish"`
}

// Hold user information
type ClientConfig struct {
	User        string     `json:"user"`
	Password    string     `json:"password"`
	Permissions PermConfig `json:"permissions"`
}

type keyConfig struct {
	Private string `json:"private"`
}

type vbusConfig struct {
	Url       string `json:"url"`
	NetworkIp string `json:"networkIp"`
	Hostname  string `json:"hostname"`
}

type configuration struct {
	Client ClientConfig `json:"client"`
	Key    keyConfig    `json:"key"`
	Vbus   vbusConfig   `json:"vbus"`
}

// Try to read config file.
// If not found, it returns the default configuration.
func (c *ExtendedNatsClient) readOrGetDefaultConfig() (*configuration, error) {
	if _, err := os.Stat(c.rootFolder); os.IsNotExist(err) {
		err = os.Mkdir(c.rootFolder, os.ModeDir)
		if err != nil {
			return nil, err
		}
	}

	_natsLog.WithField("filepath", c.rootFolder).Debug("check if we already have a Vbus config file")
	configFile := path.Join(c.rootFolder, c.id) + ".conf"
	if fileExists(configFile) {
		_natsLog.WithField("id", c.id).Debug("load existing configuration file")
		jsonFile, err := os.Open(configFile)
		if err != nil {
			return nil, errors.Wrap(err, "cannot open config file")
		}

		bytes, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			return nil, errors.Wrap(err, "cannot read config file")
		}

		var config configuration
		err = json.Unmarshal(bytes, &config)
		if err != nil {
			return nil, errors.Wrap(err, "cannot parse config file")
		}
		return &config, nil
	} else {
		_natsLog.WithField("id", c.id).Debug("create new configuration file")
		return c.getDefaultConfig()
	}
}

// Creates a default configuration object.
func (c *ExtendedNatsClient) getDefaultConfig() (*configuration, error) {
	_natsLog.WithField("id", c.id).Debug("create new configuration file")
	password, err := generatePassword()
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate password")
	}

	publicKey, err := bcrypt.GenerateFromPassword([]byte(password), defaultCost)
	if err != nil {
		return nil, errors.Wrap(err, "cannot generate public key")
	}

	return &configuration{
		Client: ClientConfig{
			User:     fmt.Sprintf("%s.%s", c.id, c.hostname),
			Password: string(publicKey),
			Permissions: PermConfig{
				Subscribe: []string{
					c.id,
					fmt.Sprintf("%s.>", c.id),
				},
				Publish: []string{
					c.id,
					fmt.Sprintf("%s.>", c.id),
				},
			},
		},
		Key: keyConfig{
			Private: password,
		},
	}, nil
}

// Write configuration on disk
func (c *ExtendedNatsClient) saveConfigFile(config *configuration) error {
	data := toVbus(config)
	filepath := path.Join(c.rootFolder, c.id+".conf")
	return ioutil.WriteFile(filepath, data, 0666)
}
