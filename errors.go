package vBus

import "fmt"

// Represents a user side error. When receive, it means that this error has been raised
// inside a used defined handler (its not an internal error).
type userError struct {
	inner error
}

func (u userError) Error() string {
	return u.inner.Error()
}

type VbusError struct {
	errorDef *ErrorDefinition
}

func (v VbusError) Error() string {
	return fmt.Sprintf("vbus error: %s (%d) - %v", v.errorDef.message, v.errorDef.code, v.errorDef.detail)
}

