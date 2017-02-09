package swaggering

import (
	"fmt"
	"log"
)

func ResolveService(context *Context) {
	log.Print("Resolving types")

	log.Print(context.swaggers)

	context.Resolve()
}

func logErr(err error, format string, args ...interface{}) {
	if err != nil {
		args = append(args, err)
		log.Output(2, fmt.Sprintf(format, args...))
	}
}
