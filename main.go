package main

import (
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"text/tabwriter"

	singularity "github.com/opentable/go-singularity"
	dtos "github.com/opentable/go-singularity/dtos"
)

var debugLog = log.New(ioutil.Discard, "", 0)

func debug(fmt string, vars ...interface{}) {
	debugLog.Printf(fmt, vars...)
}

type taskDesc struct {
	*dtos.SingularityTaskId
	*dtos.SingularityTask
}

func main() {
	opts := parseOpts()
	if opts.debug {
		debugLog.SetOutput(os.Stderr)
		debugLog.SetFlags(log.Lshortfile | log.Ltime)
	}

	client := singularity.NewClient(opts.URL)
	debug("Getting active tasks")
	tasksList, err := client.GetActiveTasks()
	debug("tasksList count: %d", len(tasksList))
	debug("tasksList: %#v", tasksList)
	debug("err: %v", err)
	if err != nil {
		log.Fatal(err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)

	if opts.printHeaders {
		writer.Write([]byte(strings.Join(append([]string{`Request ID`, `Deploy ID`}, headerNames(opts)...), "\t")))
		writer.Write([]byte{'\n'})
	}

	lines := make(chan *taskDesc, 10)
	wait := new(sync.WaitGroup)

	database := newDB()
	defer database.close()

	go tabRows(writer, wait, opts, database, lines)

	for _, task := range tasksList {
		wait.Add(1)
		debug("Starting line for %#v", task)
		go taskLine(task, client, wait, lines)
	}

	wait.Wait()
	writer.Flush()
}

func taskLine(task *dtos.SingularityTask, client *singularity.Client, wait *sync.WaitGroup, lines chan *taskDesc) {
	id := task.TaskId
	if id == nil {
		log.Printf("Missing ID for task %#v", task)
		wait.Add(-1)
		return
	}

	var err error

	for i := 0; i < 3; i++ {
		debug("Getting history: %v", id.Id)
		taskHistory, err := client.GetHistoryForTask(id.Id)
		debug("taskHistory: %#v", taskHistory)
		debug("err: %v", err)

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

	lines <- &taskDesc{id, task}
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

func (td *taskDesc) Env() *dtos.Environment {
	mesos := td.SingularityTask.MesosTask
	if mesos == nil {
		return nil
	}

	cmd := mesos.Command
	if cmd == nil {
		return nil
	}
	return cmd.Environment
}

func (td *taskDesc) rowString(opts *options) string {
	return strings.Join(append([]string{td.SingularityTaskId.RequestId, td.SingularityTaskId.DeployId}, taskValues(opts, td.Env())...), "\t") + "\n"
}

func tabRows(writer *tabwriter.Writer, wait *sync.WaitGroup, opts *options, db *database, lines chan *taskDesc) {
	for {
		line := <-lines
		writer.Write([]byte(line.rowString(opts)))
		db.addTask(line)
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
