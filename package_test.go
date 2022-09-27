package occamy_test

import (
	"os"
	"reflect"
	"testing"
)

func assertEqual(t *testing.T, expected, actual interface{}, msg string) bool {
	if reflect.DeepEqual(expected, actual) {
		return true
	}

	t.Errorf("%s: expected and actual values did not match", msg)
	return false
}

func assertNotEqual(t *testing.T, expected, actual interface{}, msg string) bool {
	if !reflect.DeepEqual(expected, actual) {
		return true
	}

	t.Errorf("%s: expected and actual values matched", msg)
	return false
}

func assertError(t *testing.T, err error, msg string) bool {
	if err != nil {
		return true
	}

	t.Errorf("%s: error was expected to be nil", msg)
	return false
}

func assertNoError(t *testing.T, err error, msg string) bool {
	if err == nil {
		return true
	}

	t.Errorf("%s: error was expected to be nil: %v", msg, err)
	return false
}

func assertTrue(t *testing.T, value bool, msg string) bool {
	if value {
		return true
	}

	t.Errorf("%s: value was expected to be true", msg)
	return false
}

func assertFalse(t *testing.T, value bool, msg string) bool {
	if !value {
		return true
	}

	t.Errorf("%s: value was expected to be false", msg)
	return false
}

func enforce(result bool) {
	if !result {
		os.Exit(1)
	}
}
