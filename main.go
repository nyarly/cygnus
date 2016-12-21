package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"text/tabwriter"

	"github.com/SeeSpotRun/coerce"
	docopt "github.com/docopt/docopt-go"
	singularity "github.com/opentable/go-singularity"
	dtos "github.com/opentable/go-singularity/dtos"
)

type options struct {
	URL                                     string
	printHeaders, printActive, printPending bool
	noPrintHeaders, noPrintActive           bool
	env                                     []string
	x                                       int
}

const docstring = `Scan a Singularity and return data
Usage: cygnus [options] [(--env=<env>)...] <url>

Options:
	-H, --no-print-headers  Don't print the header prologue
	-A, --no-print-active   Do not print the active deploys
	-p, --print-pending     Also include pending deploys
	--env=<env>             Environment variables to queury
	-x <num>                Use environment default <num>

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

func main() {
	opts := parseOpts()

	client := singularity.NewClient(opts.URL)
	tasksList, err := client.GetActiveTasks()
	if err != nil {
		log.Fatal(err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	if opts.printHeaders {
		writer.Write([]byte(strings.Join(append([]string{`Request ID`, `Deploy ID`}, headerNames(opts)...), "\t")))
		writer.Write([]byte{'\n'})
	}

	lines := make(chan []string, 10)
	wait := new(sync.WaitGroup)

	go tabRows(writer, wait, lines)

	for _, task := range tasksList {
		go taskLine(task, client, opts, wait, lines)
	}

	wait.Wait()
	writer.Flush()
}

func taskLine(task *dtos.SingularityTask, client *singularity.Client, opts *options, wait *sync.WaitGroup, lines chan []string) {
	wait.Add(1)
	id := task.TaskId
	if id == nil {
		log.Printf("Missing ID for task %#v", task)
		wait.Add(-1)
		return
	}

	var err error

	for i := 0; i < 3; i++ {
		taskHistory, err := client.GetHistoryForTask(id.Id)
		if err == nil {
			task = taskHistory.Task
			break
		}
	}
	if err != nil {
		log.Print(err)
		wait.Add(-1)
		return
	}

	mesos := task.MesosTask
	if mesos == nil {
		log.Printf("Missing mesos task info for %#v", task)
		wait.Add(-1)
		return
	}

	cmd := mesos.Command
	if cmd == nil {
		log.Printf("No command for task %#v", mesos)
		wait.Add(-1)
		return
	}
	env := cmd.Environment
	if env == nil {
		log.Printf("No enviroment for task %#v / %#v", mesos, cmd)
		wait.Add(-1)
		return
	}

	lines <- append([]string{id.RequestId, id.DeployId}, taskValues(opts, env)...)
}

func taskValues(opts *options, env *dtos.Environment) []string {
	vals := []string{}
	vars := map[string]string{}

	for _, v := range env.Variables {
		vars[v.Name] = v.Value
	}

	for _, e := range opts.env {
		if v, ok := vars[e]; ok {
			vals = append(vals, v)
		} else {
			vals = append(vals, "")
		}
	}
	return vals
}

func fetchDeploys(reqP *dtos.SingularityRequestParent, client *singularity.Client, opts *options, wait *sync.WaitGroup, lines chan []string) {
	wait.Add(2)
	reqX := reqP.Request
	if reqX == nil {
		log.Printf("Missing request for RequestParent %#v", reqP)
		wait.Add(-2)
		return
	}
	req, err := client.GetRequest(reqX.Id)
	if err != nil {
		log.Printf("Err getting deploy info: %v", err)
		wait.Add(-2)
		return
	}

	if active := req.ActiveDeploy; active != nil && opts.printActive {
		lines <- append([]string{reqX.Id, active.Id}, depValues(opts, "active", active)...)
	} else {
		wait.Add(-1)
	}

	if pending := req.PendingDeploy; pending != nil && opts.printPending {
		lines <- append([]string{reqX.Id, pending.Id}, depValues(opts, "pending", pending)...)
	} else {
		wait.Add(-1)
	}
}

func tabRows(writer *tabwriter.Writer, wait *sync.WaitGroup, lines chan []string) {
	for {
		line := <-lines
		writer.Write([]byte(strings.Join(line, "\t")))
		writer.Write([]byte{'\n'})
		wait.Done()
	}
}

func headerNames(opts *options) []string {
	if !(opts.printPending && opts.printActive) {
		return opts.env
	}
	return append([]string{"State"}, opts.env...)
}

func depValues(opts *options, marker string, dep *dtos.SingularityDeploy) []string {
	vals := []string{}
	if opts.printPending && opts.printActive {
		vals = append(vals, marker)
	}
	for _, e := range opts.env {
		if v, ok := dep.Env[e]; ok {
			vals = append(vals, v)
		} else {
			vals = append(vals, "")
		}
	}
	return vals
}
