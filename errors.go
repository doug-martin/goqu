package gql

import "fmt"

func newEncodeError(message string, args ...interface{}) error {
	return EncodeError{err: "gql: " + fmt.Sprintf(message, args...)}
}

func NewGqlError(message string, args ...interface{}) error {
	return GqlError{err: "gql: " + fmt.Sprintf(message, args...)}
}

func NewGqlQueryError(message string, args ...interface{}) error {
	return GqlQueryError{err: "gql: " + fmt.Sprintf(message, args...)}
}

type EncodeError struct {
	error
	err string
}

func (me EncodeError) Error() string {
	return me.err
}

type GqlError struct {
	err string
}

func (me GqlError) Error() string {
	return me.err
}

type GqlQueryError struct {
	err string
}

func (me GqlQueryError) Error() string {
	return me.err
}
