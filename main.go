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
	*dtos.SingularityRequestParent
	*dtos.SingularityTaskHistoryUpdate
}

func main() {
	opts := parseOpts()
	if opts.debug {
		debugLog.SetOutput(os.Stderr)
		debugLog.SetFlags(log.Lshortfile | log.Ltime)
	}

	client := singularity.NewClient(opts.URL)

	debug("Getting all requests")
	reqList, err := client.GetRequests()
	if err != nil {
		log.Fatal(err)
	}
	debug("reqList count: %d", len(reqList))

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

	seen := map[string]struct{}{}

	for n, req := range reqList {
		debug("req %d: %#v", n, req)
		if opts.printInactiveTasks {
			histo, _ := client.GetTaskHistoryForRequest(req.Request.Id, 10, 1)
			seen = getTasks(client, histo, lines, reqList, seen, wait)
		}

		histo, _ := client.GetTaskHistoryForActiveRequest(req.Request.Id)
		seen = getTasks(client, histo, lines, reqList, seen, wait)
	}

	wait.Wait()
	writer.Flush()
}

func getTasks(client *singularity.Client, histo dtos.SingularityTaskIdHistoryList, lines chan *taskDesc, reqList dtos.SingularityRequestParentList, seen map[string]struct{}, wait *sync.WaitGroup) map[string]struct{} {
	for _, hist := range histo {
		if _, have := seen[hist.TaskId.Id]; have {
			continue
		}
		seen[hist.TaskId.Id] = struct{}{}

		wait.Add(1)
		debug("Starting line for %#v", hist.TaskId)
		go getTask(hist.TaskId, reqList, client, wait, lines)
	}
	return seen
}

func getTask(id *dtos.SingularityTaskId, reqs dtos.SingularityRequestParentList, client *singularity.Client, wait *sync.WaitGroup, lines chan *taskDesc) {
	var task *dtos.SingularityTask
	if id == nil {
		log.Printf("Missing ID for task %#v", task)
		wait.Add(-1)
		return
	}

	var err error

	var taskHistory *dtos.SingularityTaskHistory
	var lastUpdate *dtos.SingularityTaskHistoryUpdate

	for i := 0; i < 3; i++ {
		debug("Getting history: %v", id.Id)
		taskHistory, err = client.GetHistoryForTask(id.Id)
		debug("taskHistory: %#v", taskHistory)
		if len(taskHistory.TaskUpdates) > 0 {
			lastUpdate = taskHistory.TaskUpdates[0]
		}

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

	for _, upd := range taskHistory.TaskUpdates {
		if upd.Timestamp > lastUpdate.Timestamp {
			lastUpdate = upd
		}
	}
	debug("last update: %#v", lastUpdate)

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

	var taskReq *dtos.SingularityRequestParent

	for _, req := range reqs {
		if req.Request.Id == id.RequestId {
			taskReq = req
			break
		}
	}

	lines <- &taskDesc{id, task, taskReq, lastUpdate}
}

func taskValues(opts *options, td *taskDesc) []string {
	vals := []string{}
	vars := map[string]string{}

	env := td.Env()

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

	if opts.printStatus {
		status := "UNKNOWN"
		if td.SingularityTaskHistoryUpdate != nil {
			status = string(td.SingularityTaskHistoryUpdate.TaskState)
		}
		vals = append(vals, status)
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
	return strings.Join(append([]string{td.SingularityTaskId.RequestId, td.SingularityTaskId.DeployId}, taskValues(opts, td)...), "\t") + "\n"
}

func tabRows(writer *tabwriter.Writer, wait *sync.WaitGroup, opts *options, db *database, lines chan *taskDesc) {
	for {
		line := <-lines
		if printable(line, opts) {
			writer.Write([]byte(line.rowString(opts)))
		}
		db.addTask(line)
		wait.Done()
	}
}

func printable(desc *taskDesc, opts *options) bool {
	debug("printable: %t %v %s", opts.printInactiveTasks,
		desc.SingularityTaskHistoryUpdate,
		desc.SingularityTaskHistoryUpdate.TaskState)
	return opts.printInactiveTasks ||
		desc.SingularityTaskHistoryUpdate == nil ||
		desc.SingularityTaskHistoryUpdate.TaskState ==
			dtos.SingularityTaskHistoryUpdateExtendedTaskStateTASK_RUNNING
}

func headerNames(opts *options) []string {
	headers := []string{}

	if opts.printPending || opts.printActive {
		headers = append(headers, "State")
	}
	headers = append(headers, opts.env...)
	if opts.printStatus {
		headers = append(headers, "Task Status")
	}
	return headers
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
