package occamy_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Graphmasters/occamy"
)

func TestNewError(t *testing.T) {
	const kind = occamy.ErrKindInvalidHeader
	innerErr := errors.New("error")
	err := occamy.NewError(kind, innerErr)
	assertError(t, err, "NewError must return error")
	assertNotEqual(t, err, innerErr, "NewError must return error different to inputted error")

	_, ok := err.(*occamy.Error)
	assertTrue(t, ok, "NewError should return an error that can be type converted to *occamy.Error")
}

func TestNewError_nil(t *testing.T) {
	err := occamy.NewError(occamy.ErrKindUnknownTaskError, nil)
	assertNoError(t, err, "NewError should return nil error when passed nil error")
}

func TestNewErrorf(t *testing.T) {
	const kind = occamy.ErrKindInvalidHeader
	err := occamy.NewErrorf(kind, "error")
	assertError(t, err, "NewError must return error")

	_, ok := err.(*occamy.Error)
	assertTrue(t, ok, "NewError should return an error that can be type converted to *occamy.Error")
}

func TestError_Unwrap(t *testing.T) {
	innerErr := errors.New("error")
	err := occamy.NewError(occamy.ErrKindUnknownTaskError, innerErr)
	assertError(t, err, "NewError must return error")
	assertNotEqual(t, err, innerErr, "NewError must return error different to inputted error")

	assertEqual(t, err.(*occamy.Error).Unwrap(), innerErr, "error must unwrap to inner error")
	assertNotEqual(t, err.(*occamy.Error).Unwrap(), errors.New("a different error"), "error must not unwrap to a different error")
	assertNotEqual(t, err.(*occamy.Error).Unwrap(), err, "error must not unwrap to itself")
}

func TestError_Unwrap_IsError(t *testing.T) {
	// This tests that builtin errors.Is(...) works as expected on coded errors.
	innerErr := errors.New("inner_error")
	err := occamy.NewError(occamy.ErrKindUnknownHandlerError, innerErr)

	assertTrue(t, errors.Is(err, err), "errors.Is(...) when ")
	assertTrue(t, errors.Is(err, innerErr), "err must ")
	assertFalse(t, errors.Is(err, errors.New("a different error")), "errors.Is(...) is expected to return false on a different error")
}

func TestExtractErrorKind(t *testing.T) {
	const (
		innerKind occamy.ErrKind = "inner_kind"
		outerKind occamy.ErrKind = "outer_kind"
		emptyKind occamy.ErrKind = ""
	)

	err := occamy.NewError(innerKind, fmt.Errorf("error"))
	status, ok := occamy.ExtractErrorKind(err)
	assertTrue(t, ok, "extraction after NewError failed")
	assertEqual(t, innerKind, status, "wrong kind returned after NewError")

	err = fmt.Errorf("wrapped: %w", err)
	status, ok = occamy.ExtractErrorKind(err)
	assertTrue(t, ok, "extraction after wrapping failed")
	assertEqual(t, innerKind, status, "wrong kind returned after wrapping")

	err = occamy.NewError(outerKind, err)
	status, ok = occamy.ExtractErrorKind(err)
	assertTrue(t, ok, "extraction after NewError on occamy error failed")
	assertEqual(t, outerKind, status, "wroong extraction after NewError on occamy error")

	status, ok = occamy.ExtractErrorKind(nil)
	assertFalse(t, ok, "extraction on nil error unexpected succeeded")
	assertEqual(t, emptyKind, status, "extraction on nil error should return empty kind")

	status, ok = occamy.ExtractErrorKind(fmt.Errorf("error"))
	assertFalse(t, ok, "extraction on non-occamy error unexpected succeeded")
	assertEqual(t, emptyKind, status, "extraction on non-occamy error should return empty kind")
}
