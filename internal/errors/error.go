package errors

import "fmt"

type Error struct {
	err string
}

func New(message string, args ...interface{}) error {
	return Error{err: "goqu: " + fmt.Sprintf(message, args...)}
}

func (e Error) Error() string {
	return e.err
}

type EncodeError struct {
	error
	err string
}

func NewEncodeError(t interface{}) error {
	return Error{err: "goqu_encode_error: " + fmt.Sprintf("Unable to encode value %+v", t)}
}

func (e EncodeError) Error() string {
	return e.err
}
