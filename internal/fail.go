package internal

import (
	"fmt"
	"os"
)

func Fail(s string, a ...interface{}) {
	_, _ = fmt.Fprintf(os.Stderr, s, a...)
	os.Exit(1)
}
