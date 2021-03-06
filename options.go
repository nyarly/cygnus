package main

import (
	"fmt"
	"log"

	"github.com/SeeSpotRun/coerce"
	docopt "github.com/docopt/docopt-go"
)

type options struct {
	URL                                     string
	printHeaders, printActive, printPending bool
	noPrintHeaders, noPrintActive           bool
	printInactiveTasks, printStatus         bool
	printDockerImage                        bool
	env                                     []string
	x                                       int
	debug                                   bool
}

const docstring = `Scan a Singularity and return data
Usage: cygnus [options] [(--env=<env>)...] <url>

Options:
	-H, --no-print-headers       Don't print the header prologue
	-A, --no-print-active        Do not print the active deploys
	-K, --print-inactive-tasks   Include inactive tasks in output
	-p, --print-pending          Also include pending deploys
	-s, --print-status           Include the task status
	--debug                      Print debugging information
	--env=<env>                  Environment variables to queury
	--print-docker-image         Include the docker image in output
	-x <num>                     Use environment default <num>

Environment defaults are sets of useful environment variables, collected over
time by users of the tool.
-x 1: TASK_HOST, PORT0
`

func parseOpts() *options {
	parsed, err := docopt.Parse(docstring, nil, true, "", false)
	if err != nil {
		log.Fatal(err)
	}

	opts := options{}
	err = coerce.Struct(&opts, parsed, "-%s", "--%s", "<%s>")
	if err != nil {
		log.Fatal(err)
	}

	opts.printHeaders = !opts.noPrintHeaders
	opts.printActive = !opts.noPrintActive

	switch opts.x {
	case 1:
		fmt.Println("Using TASK_HOST and PORT0")
		opts.env = []string{"TASK_HOST", "PORT0"}
	}

	return &opts
}
