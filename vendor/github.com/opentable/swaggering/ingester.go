package swaggering

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"
)

type ServiceJSON struct {
	Apis []*ServiceApiJSON
}

type ServiceApiJSON struct {
	Path, Desc string
}

func ProcessService(dir string, ingester *Context) {
	ingester.ProcessServiceDir(dir)
}

func (ingester *Context) ProcessServiceDir(dir string) {
	fullpath := filepath.Join(dir, "service.json")

	apis := &ServiceJSON{}

	loadJSONfromPath(fullpath, apis)

	fileRE := regexp.MustCompile(`^/(\w+).{format}$`)

	for _, api := range apis.Apis {
		smi := fileRE.FindStringSubmatchIndex(api.Path)
		file := []byte("")
		file = fileRE.ExpandString(file, "$1.json", api.Path, smi)

		ingester.ingestApifromPath(filepath.Join(dir, string(file)))
	}
}

func loadJSONfromPath(fullpath string, into interface{}) {
	data, err := os.Open(fullpath)
	if err != nil {
		log.Print("Trouble with", fullpath, ":", err)
		return
	}

	loadJSON(fullpath, data, into)
}

func loadJSON(fullpath string, data io.Reader, into interface{}) {
	dec := json.NewDecoder(data)

	if err := dec.Decode(into); err == io.EOF {
		log.Fatal("Trouble with empty", fullpath, ":", err)
		return
	} else if err != nil {
		log.Print("Trouble parsing", fullpath, ":", err)
		return
	}
}

func (ctx *Context) ingestApifromPath(fullpath string) {
	log.Print("Processing:", fullpath)

	data, err := os.Open(fullpath)
	if err != nil {
		log.Print("Trouble with", fullpath, ":", err)
		return
	}
	ctx.IngestApi(fullpath, data)
}

func (ctx *Context) IngestApi(aspath string, data io.Reader) {
	ctx.swaggers = append(ctx.swaggers, &Swagger{})
	swagger := ctx.swaggers[len(ctx.swaggers)-1]

	loadJSON(aspath, data, swagger)
}
