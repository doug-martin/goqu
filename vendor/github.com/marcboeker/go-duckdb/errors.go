package duckdb

import "C"
import (
	"errors"
	"fmt"
	"strings"
)

func getError(errDriver error, err error) error {
	if err == nil {
		return fmt.Errorf("%s: %w", driverErrMsg, errDriver)
	}
	return fmt.Errorf("%s: %w: %s", driverErrMsg, errDriver, err.Error())
}

func duckdbError(err *C.char) error {
	return fmt.Errorf("%s: %w", duckdbErrMsg, errors.New(C.GoString(err)))
}

func castError(actual string, expected string) error {
	return fmt.Errorf("%s: cannot cast %s to %s", castErrMsg, actual, expected)
}

func structFieldError(actual string, expected string) error {
	return fmt.Errorf("%s: expected %s, got %s", structFieldErrMsg, expected, actual)
}

func columnCountError(actual int, expected int) error {
	return fmt.Errorf("%s: expected %d, got %d", columnCountErrMsg, expected, actual)
}

func unsupportedTypeError(name string) error {
	return fmt.Errorf("%s: %s", unsupportedTypeErrMsg, name)
}

func invalidatedAppenderError(err error) error {
	if err == nil {
		return errors.New(invalidatedAppenderMsg)
	}
	return fmt.Errorf("%w: %s", err, invalidatedAppenderMsg)
}

func tryOtherFuncError(hint string) error {
	return fmt.Errorf("%s: %s", tryOtherFuncErrMsg, hint)
}

func addIndexToError(err error, idx int) error {
	return fmt.Errorf("%w: %s: %d", err, indexErrMsg, idx)
}

func interfaceIsNilError(interfaceName string) error {
	return fmt.Errorf("%s: %s", interfaceIsNilErrMsg, interfaceName)
}

func duplicateNameError(name string) error {
	return fmt.Errorf("%s: %s", duplicateNameErrMsg, name)
}

const (
	driverErrMsg           = "database/sql/driver"
	duckdbErrMsg           = "duckdb error"
	castErrMsg             = "cast error"
	structFieldErrMsg      = "invalid STRUCT field"
	columnCountErrMsg      = "invalid column count"
	unsupportedTypeErrMsg  = "unsupported data type"
	invalidatedAppenderMsg = "appended data has been invalidated due to corrupt row"
	tryOtherFuncErrMsg     = "please try this function instead"
	indexErrMsg            = "index"
	unknownTypeErrMsg      = "unknown type"
	interfaceIsNilErrMsg   = "interface is nil"
	duplicateNameErrMsg    = "duplicate name"
)

var (
	errAPI        = errors.New("API error")
	errVectorSize = errors.New("data chunks cannot exceed duckdb's internal vector size")

	errParseDSN   = errors.New("could not parse DSN for database")
	errOpen       = errors.New("could not open database")
	errSetConfig  = errors.New("could not set invalid or local option for global database config")
	errInvalidCon = errors.New("not a DuckDB driver connection")
	errClosedCon  = errors.New("closed connection")

	errAppenderCreation         = errors.New("could not create appender")
	errAppenderClose            = errors.New("could not close appender")
	errAppenderDoubleClose      = fmt.Errorf("%w: already closed", errAppenderClose)
	errAppenderAppendRow        = errors.New("could not append row")
	errAppenderAppendAfterClose = fmt.Errorf("%w: appender already closed", errAppenderAppendRow)
	errAppenderFlush            = errors.New("could not flush appender")

	errUnsupportedMapKeyType = errors.New("MAP key type not supported")
	errEmptyName             = errors.New("empty name")
	errInvalidDecimalWidth   = fmt.Errorf("the DECIMAL with must be between 1 and %d", MAX_DECIMAL_WIDTH)
	errInvalidDecimalScale   = errors.New("the DECIMAL scale must be less than or equal to the width")
	errSetSQLNULLValue       = errors.New("cannot write to a NULL column")

	errScalarUDFCreate          = errors.New("could not create scalar UDF")
	errScalarUDFNoName          = fmt.Errorf("%w: missing name", errScalarUDFCreate)
	errScalarUDFIsNil           = fmt.Errorf("%w: function is nil", errScalarUDFCreate)
	errScalarUDFNoExecutor      = fmt.Errorf("%w: executor is nil", errScalarUDFCreate)
	errScalarUDFInputTypeIsNil  = fmt.Errorf("%w: input type is nil", errScalarUDFCreate)
	errScalarUDFResultTypeIsNil = fmt.Errorf("%w: result type is nil", errScalarUDFCreate)
	errScalarUDFResultTypeIsANY = fmt.Errorf("%w: result type is ANY, which is not supported", errScalarUDFCreate)
	errScalarUDFCreateSet       = fmt.Errorf("could not create scalar UDF set")
	errScalarUDFAddToSet        = fmt.Errorf("%w: could not add the function to the set", errScalarUDFCreateSet)

	errProfilingInfoEmpty = errors.New("no profiling information available for this connection")

	// Errors not covered in tests.
	errConnect      = errors.New("could not connect to database")
	errCreateConfig = errors.New("could not create config for database")
)

type ErrorType int

const (
	ErrorTypeInvalid              ErrorType = iota // invalid type
	ErrorTypeOutOfRange                            // value out of range error
	ErrorTypeConversion                            // conversion/casting error
	ErrorTypeUnknownType                           // unknown type error
	ErrorTypeDecimal                               // decimal related
	ErrorTypeMismatchType                          // type mismatch
	ErrorTypeDivideByZero                          // divide by 0
	ErrorTypeObjectSize                            // object size exceeded
	ErrorTypeInvalidType                           // incompatible for operation
	ErrorTypeSerialization                         // serialization
	ErrorTypeTransaction                           // transaction management
	ErrorTypeNotImplemented                        // method not implemented
	ErrorTypeExpression                            // expression parsing
	ErrorTypeCatalog                               // catalog related
	ErrorTypeParser                                // parser related
	ErrorTypePlanner                               // planner related
	ErrorTypeScheduler                             // scheduler related
	ErrorTypeExecutor                              // executor related
	ErrorTypeConstraint                            // constraint related
	ErrorTypeIndex                                 // index related
	ErrorTypeStat                                  // stat related
	ErrorTypeConnection                            // connection related
	ErrorTypeSyntax                                // syntax related
	ErrorTypeSettings                              // settings related
	ErrorTypeBinder                                // binder related
	ErrorTypeNetwork                               // network related
	ErrorTypeOptimizer                             // optimizer related
	ErrorTypeNullPointer                           // nullptr exception
	ErrorTypeIO                                    // IO exception
	ErrorTypeInterrupt                             // interrupt
	ErrorTypeFatal                                 // Fatal exceptions are non-recoverable, and render the entire DB in an unusable state
	ErrorTypeInternal                              // Internal exceptions indicate something went wrong internally (i.e. bug in the code base)
	ErrorTypeInvalidInput                          // Input or arguments error
	ErrorTypeOutOfMemory                           // out of memory
	ErrorTypePermission                            // insufficient permissions
	ErrorTypeParameterNotResolved                  // parameter types could not be resolved
	ErrorTypeParameterNotAllowed                   // parameter types not allowed
	ErrorTypeDependency                            // dependency
	ErrorTypeHTTP
	ErrorTypeMissingExtension // Thrown when an extension is used but not loaded
	ErrorTypeAutoLoad         // Thrown when an extension is used but not loaded
	ErrorTypeSequence
	ErrorTypeInvalidConfiguration // An invalid configuration was detected (e.g. a Secret param was missing, or a required setting not found)
)

var errorPrefixMap = map[string]ErrorType{
	"Invalid Error":                ErrorTypeInvalid,
	"Out of Range Error":           ErrorTypeOutOfRange,
	"Conversion Error":             ErrorTypeConversion,
	"Error":                        ErrorTypeUnknownType,
	"Decimal Error":                ErrorTypeDecimal,
	"Mismatch Type Error":          ErrorTypeMismatchType,
	"Divide by Zero Error":         ErrorTypeDivideByZero,
	"Object Size Error":            ErrorTypeObjectSize,
	"Invalid type Error":           ErrorTypeInvalidType,
	"Serialization Error":          ErrorTypeSerialization,
	"TransactionContext Error":     ErrorTypeTransaction,
	"Not implemented Error":        ErrorTypeNotImplemented,
	"Expression Error":             ErrorTypeExpression,
	"Catalog Error":                ErrorTypeCatalog,
	"Parser Error":                 ErrorTypeParser,
	"Planner Error":                ErrorTypePlanner,
	"Scheduler Error":              ErrorTypeScheduler,
	"Executor Error":               ErrorTypeExecutor,
	"Constraint Error":             ErrorTypeConstraint,
	"Index Error":                  ErrorTypeIndex,
	"Stat Error":                   ErrorTypeStat,
	"Connection Error":             ErrorTypeConnection,
	"Syntax Error":                 ErrorTypeSyntax,
	"Settings Error":               ErrorTypeSettings,
	"Binder Error":                 ErrorTypeBinder,
	"Network Error":                ErrorTypeNetwork,
	"Optimizer Error":              ErrorTypeOptimizer,
	"NullPointer Error":            ErrorTypeNullPointer,
	"IO Error":                     ErrorTypeIO,
	"INTERRUPT Error":              ErrorTypeInterrupt,
	"FATAL Error":                  ErrorTypeFatal,
	"INTERNAL Error":               ErrorTypeInternal,
	"Invalid Input Error":          ErrorTypeInvalidInput,
	"Out of Memory Error":          ErrorTypeOutOfMemory,
	"Permission Error":             ErrorTypePermission,
	"Parameter Not Resolved Error": ErrorTypeParameterNotResolved,
	"Parameter Not Allowed Error":  ErrorTypeParameterNotAllowed,
	"Dependency Error":             ErrorTypeDependency,
	"HTTP Error":                   ErrorTypeHTTP,
	"Missing Extension Error":      ErrorTypeMissingExtension,
	"Extension Autoloading Error":  ErrorTypeAutoLoad,
	"Sequence Error":               ErrorTypeSequence,
	"Invalid Configuration Error":  ErrorTypeInvalidConfiguration,
}

type Error struct {
	Type ErrorType
	Msg  string
}

func (e *Error) Error() string {
	return e.Msg
}

func (e *Error) Is(err error) bool {
	if other, ok := err.(*Error); ok {
		return other.Msg == e.Msg
	}
	return false
}

func getDuckDBError(errMsg string) error {
	errType := ErrorTypeInvalid
	// find the end of the prefix ("<error-type> Error: ")
	if idx := strings.Index(errMsg, ": "); idx != -1 {
		if typ, ok := errorPrefixMap[errMsg[:idx]]; ok {
			errType = typ
		}
	}
	return &Error{
		Type: errType,
		Msg:  errMsg,
	}
}
