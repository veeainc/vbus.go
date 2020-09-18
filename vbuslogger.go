package vBus

import (
	"fmt"
	"github.com/sirupsen/logrus"
)

const (
	systemSegment       = "__system__"
	traceLevelSegment   = "trace"
	debugLevelSegment   = "debug"
	infoLevelSegment    = "info"
	warningLevelSegment = "warning"
	errorLevelSegment   = "error"
)

var _vbusloggerLog = getNamedLogger()

// Allow to log things though vBus
type RemoteLogger struct {
	client    *ExtendedNatsClient
	namespace string
	pathMask  string
}

// A remote logger that send message on nats.
func NewRemoteLogger(client *ExtendedNatsClient, namespace string) *RemoteLogger {
	if namespace != "" {
		namespace = namespace + ": "
	}

	return &RemoteLogger{
		client:    client,
		namespace: namespace,
		pathMask:  systemSegment + ".log.%s", // domain.name.hostname.__system__.log.level
	}
}

func (rl *RemoteLogger) send(msg string, levelSegment string) {
	if err := rl.client.Publish(fmt.Sprintf(rl.pathMask, levelSegment), rl.namespace+msg); err != nil {
		_vbusloggerLog.WithField("error", err.Error()).Warning("cannot send log message")
	}
}

// Trace logs a message at level Trace on vBus logger.
func (rl *RemoteLogger) Trace(args ...interface{}) {
	rl.send(fmt.Sprint(args...), traceLevelSegment)
}

// Debug logs a message at level Debug on vBus logger.
func (rl *RemoteLogger) Debug(args ...interface{}) {
	rl.send(fmt.Sprint(args...), debugLevelSegment)
}

// Info logs a message at level Info on vBus logger.
func (rl *RemoteLogger) Info(args ...interface{}) {
	rl.send(fmt.Sprint(args...), infoLevelSegment)
}

// Warn logs a message at level Warn on vBus logger.
func (rl *RemoteLogger) Warning(args ...interface{}) {
	rl.send(fmt.Sprint(args...), warningLevelSegment)
}

// Error logs a message at level Error on vBus logger.
func (rl *RemoteLogger) Error(args ...interface{}) {
	rl.send(fmt.Sprint(args...), errorLevelSegment)
}

// Tracef logs a message at level Trace on vBus logger.
func (rl *RemoteLogger) Tracef(format string, args ...interface{}) {
	rl.send(fmt.Sprintf(format, args...), traceLevelSegment)
}

// Debugf logs a message at level Debug on vBus logger.
func (rl *RemoteLogger) Debugf(format string, args ...interface{}) {
	rl.send(fmt.Sprintf(format, args...), debugLevelSegment)
}

// Infof logs a message at level Info on vBus logger.
func (rl *RemoteLogger) Infof(format string, args ...interface{}) {
	rl.send(fmt.Sprintf(format, args...), infoLevelSegment)
}

// Warnf logs a message at level Warn on vBus logger.
func (rl *RemoteLogger) Warningf(format string, args ...interface{}) {
	rl.send(fmt.Sprintf(format, args...), warningLevelSegment)
}

// Errorf logs a message at level Error on vBus logger.
func (rl *RemoteLogger) Errorf(format string, args ...interface{}) {
	rl.send(fmt.Sprintf(format, args...), errorLevelSegment)
}

type VBusHook struct {
	remoteLogger *RemoteLogger
}

func (hook *VBusHook) Fire(entry *logrus.Entry) error {
	line := entry.Message

	if prefix, ok := entry.Data["prefix"]; ok {
		line = fmt.Sprintf("%v", prefix) + ": " + line
	}

	for name, field := range entry.Data {
		if name == "prefix" {
			continue
		}

		line += " " + name + "=" + fmt.Sprintf("%v", field)
	}

	switch entry.Level {
	case logrus.PanicLevel:
		hook.remoteLogger.Error(line)
	case logrus.FatalLevel:
		hook.remoteLogger.Error(line)
	case logrus.ErrorLevel:
		hook.remoteLogger.Error(line)
	case logrus.WarnLevel:
		hook.remoteLogger.Warning(line)
	case logrus.InfoLevel:
		hook.remoteLogger.Info(line)
	case logrus.DebugLevel:
		hook.remoteLogger.Debug(line)
	case logrus.TraceLevel:
		hook.remoteLogger.Trace(line)
	default:
		return nil
	}

	return nil
}

func (hook *VBusHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

// Directly attach an existing logrus instance to this remote logger.
func (rl *RemoteLogger) AttachLogrus(logger *logrus.Entry) {
	hook := &VBusHook{remoteLogger: rl}
	logger.Logger.AddHook(hook)
}
