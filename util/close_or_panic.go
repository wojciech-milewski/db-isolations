package util

import "io"

func CloseOrPanic(closer io.Closer) {
	err := closer.Close()
	PanicIfNotNil(err)
}
