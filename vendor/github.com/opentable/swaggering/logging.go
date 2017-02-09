package swaggering

import "log"

type (
	// Logger is the interface that swaggering's clients expect to log to
	Logger interface {
		Info(msg string, data ...interface{})
		Debug(msg string, data ...interface{})
		Logging() bool
		Debugging() bool
	}

	// NullLogger simply swallows logging
	NullLogger struct{}

	// StdlibDebugLogger just sends its arguments to the global logger
	StdlibDebugLogger struct{}
)

// Info implements Logger
func (nl NullLogger) Info(m string, d ...interface{}) {
}

// Debug implements Logger
func (nl NullLogger) Debug(m string, d ...interface{}) {
}

// Logging implements Logger
func (nl NullLogger) Logging() bool {
	return false
}

// Debugging implements Logger
func (nl NullLogger) Debugging() bool {
	return false
}

// Info implements Logger
func (dl StdlibDebugLogger) Info(m string, d ...interface{}) {
	v := append([]interface{}{m}, d...)

	log.Print(v...)
}

// Debug implements Logger
func (dl StdlibDebugLogger) Debug(m string, d ...interface{}) {
	v := append([]interface{}{m}, d...)

	log.Print(v...)
}

// Logging implements Logger
func (dl StdlibDebugLogger) Logging() bool {
	return true
}

// Debugging implements Logger
func (dl StdlibDebugLogger) Debugging() bool {
	return true
}
