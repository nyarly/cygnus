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
	*dtos.DockerInfo
	url string
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

	lines := make(chan *taskDesc, 20)
	wait := new(sync.WaitGroup)

	database := newDB()
	defer database.close()

	go tabRows(writer, wait, opts, database, lines)

	seen := map[string]struct{}{}

	for n, req := range reqList {
		debug("req %d: %#v", n, req)
		if opts.printInactiveTasks {
			histo, _ := client.GetTaskHistoryForRequest(req.Request.Id, 10, 1)
			seen = getTasks(opts.URL, client, histo, lines, reqList, seen, wait)
		}

		histo, _ := client.GetTaskHistoryForActiveRequest(req.Request.Id)
		seen = getTasks(opts.URL, client, histo, lines, reqList, seen, wait)
	}

	wait.Wait()
	writer.Flush()
}

func getTasks(url string, client *singularity.Client, histo dtos.SingularityTaskIdHistoryList, lines chan *taskDesc, reqList dtos.SingularityRequestParentList, seen map[string]struct{}, wait *sync.WaitGroup) map[string]struct{} {
	for _, hist := range histo {
		if _, have := seen[hist.TaskId.Id]; have {
			continue
		}
		seen[hist.TaskId.Id] = struct{}{}

		wait.Add(1)
		debug("Starting line for %#v", hist.TaskId)
		go getTask(url, hist.TaskId, reqList, client, wait, lines)
	}
	return seen
}

func getTask(url string, id *dtos.SingularityTaskId, reqs dtos.SingularityRequestParentList, client *singularity.Client, wait *sync.WaitGroup, lines chan *taskDesc) {
	var task *dtos.SingularityTask
	if id == nil {
		log.Printf("Missing ID for task %#v", task)
		wait.Add(-1)
		return
	}

	var err error

	var taskHistory *dtos.SingularityTaskHistory
	var lastUpdate *dtos.SingularityTaskHistoryUpdate
	var dockerInfo *dtos.DockerInfo

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
	debug("task request: %#v", task)

	mesos := task.MesosTask
	if mesos == nil {
		log.Printf("Missing mesos task info for %#v", task)
		wait.Add(-1)
		return
	}
	debug("mesos task info %#v", mesos)
	debug("mesos task info container %#v", mesos.Container)
	debug("mesos task info docker %#v", mesos.Container.Docker)

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

	c := mesos.Container
	if c != nil {
		dockerInfo = c.Docker
	}

	var taskReq *dtos.SingularityRequestParent

	for _, req := range reqs {
		if req.Request.Id == id.RequestId {
			taskReq = req
			break
		}
	}

	lines <- &taskDesc{id, task, taskReq, lastUpdate, dockerInfo, url}
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
		go func(line *taskDesc) {
			wait.Add(1)
			db.addTask(line)
			wait.Done()
		}(line)
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
	if opts.printDockerImage {
		headers = append(headers, "Docker Image")
	}
	return headers
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
	if opts.printDockerImage {
		if td.DockerInfo == nil {
			vals = append(vals, "<? none ?>")
		} else {
			vals = append(vals, td.DockerInfo.Image)
		}
	}

	return vals
}
