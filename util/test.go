package util

import (
	"strconv"
	"testing"
)

const testCount = 1000

func RepeatTest(testFunc func(*testing.T)) func(*testing.T) {
	return func(t *testing.T) {
		for i := 1; i <= testCount; i++ {
			t.Run(strconv.Itoa(i), testFunc)
		}
	}
}
