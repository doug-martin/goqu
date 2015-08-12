package goqu

import "fmt"

func newEncodeError(message string, args ...interface{}) error {
	return EncodeError{err: "goqu: " + fmt.Sprintf(message, args...)}
}

func NewGoquError(message string, args ...interface{}) error {
	return GoquError{err: "goqu: " + fmt.Sprintf(message, args...)}
}

type EncodeError struct {
	error
	err string
}

func (me EncodeError) Error() string {
	return me.err
}

type GoquError struct {
	err string
}

func (me GoquError) Error() string {
	return me.err
}
