package swaggering

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"text/template"

	"golang.org/x/tools/godoc/vfs"
	"golang.org/x/tools/imports"
)

//go:generate inlinefiles --vfs=Templates --glob=* ./templates vfs_templates.go

// Renderer is responsible for actually turning the parsed API into go code
type Renderer struct {
	targetDir                   string
	modelTmpl, apiTmpl, dtoTmpl *template.Template
}

// NewRenderer creates a new renderer
func NewRenderer(tgt string) (renderer *Renderer) {
	renderer = &Renderer{targetDir: tgt}

	renderer.apiTmpl = template.Must(loadTemplate(Templates, "api", "api.tmpl"))
	opT := renderer.apiTmpl.New("operation")
	template.Must(loadTemplateInto(Templates, opT, "operation.tmpl"))
	tyT := opT.New("type")
	template.Must(loadTemplateInto(Templates, tyT, "type.tmpl"))

	renderer.modelTmpl = template.Must(loadTemplate(Templates, "model", "model.tmpl"))

	return
}

// RenderService performs the rendering of a service
func RenderService(target string, ingester *Context) {
	self := NewRenderer(target)
	for _, model := range ingester.models {
		if model.GoUses {
			log.Print("Model: ", model.GoName)
			path := filepath.Join("dtos", snakeCase(model.GoName))
			err := self.renderModel(path, model)
			if err != nil {
				log.Print(err)
			}
		}
	}

	for _, api := range ingester.apis {
		log.Print("API: ", api.Path)
		err := self.renderAPI(apiPath(api.Path), api)
		if err != nil {
			log.Print(err)
		}
	}
}

func templateSource(fs vfs.Opener, fName string) (string, error) {
	tmplFile, err := fs.Open(fName)
	if err != nil {
		return "", err
	}
	tmplB := &bytes.Buffer{}
	_, err = tmplB.ReadFrom(tmplFile)
	return tmplB.String(), err
}

func loadTemplate(fs vfs.Opener, tName, fName string) (*template.Template, error) {
	return loadTemplateInto(fs, template.New(tName), fName)
}

func loadTemplateInto(fs vfs.Opener, tpl *template.Template, fName string) (*template.Template, error) {
	src, err := templateSource(fs, fName)
	if err != nil {
		return nil, err
	}
	return tpl.Parse(src)
}

func apiPath(urlPath string) string {
	slashes := regexp.MustCompile("/+")
	unders := regexp.MustCompile("^_+")
	brackets := regexp.MustCompile("[}{]")

	urlPath = slashes.ReplaceAllString(urlPath, "_")
	urlPath = unders.ReplaceAllString(urlPath, "")
	urlPath = brackets.ReplaceAllString(urlPath, "")

	return urlPath
}

func snakeCase(symbol string) string {
	write := make([]byte, 0, len(symbol)*2)
	read := []byte(symbol)
	re := regexp.MustCompile("[A-Z]+")

	for {
		found := re.FindIndex(read)

		if found == nil {
			write = append(write, bytes.ToLower(read)...)
			return string(write)
		}

		write = append(write, read[0:found[0]]...)
		if found[0] != 0 {
			write = append(write, byte("_"[0]))
		}
		write = append(write, bytes.ToLower(read[found[0]:found[1]])...)
		read = read[found[1]:]
	}
}

func (r *Renderer) renderModel(path string, model *Model) error {
	return renderOut(r.targetDir, path, r.modelTmpl, model)
}

func (r *Renderer) renderAPI(path string, api *Api) error {
	return renderOut(r.targetDir, path, r.apiTmpl, api)
}

func renderOut(dir, path string, tmpl *template.Template, context interface{}) error {
	fullpath := filepath.Join(dir, path) + ".go"
	fb, err := renderCode(fullpath, tmpl, context)
	if err == nil {
		err = writeCode(fullpath, fb)
	}
	return err
}

func writeCode(fullpath string, formattedBytes []byte) error {
	log.Print("Rendering to ", fullpath)
	if len(formattedBytes) == 0 {
		log.Print("Empty!")
		return nil
	}

	targetFile, err := os.Create(fullpath)
	defer targetFile.Close()
	if err != nil {
		return fmt.Errorf("Problem creating file %s: %v", fullpath, err)
	}

	targetFile.Write(formattedBytes)

	return nil
}

func renderCode(fullpath string, tmpl *template.Template, context interface{}) (fb []byte, err error) {
	targetBuf := bytes.Buffer{}

	err = tmpl.Execute(&targetBuf, context)
	if err != nil {
		return nil, fmt.Errorf("Problem rendering %s : %v", tmpl.Name(), err)
	}

	fb, err = imports.Process(fullpath, targetBuf.Bytes(), nil)
	if err != nil {
		return targetBuf.Bytes(), fmt.Errorf("Problem formatting %s : %v\n%s", tmpl.Name(), err, targetBuf.Bytes())
	}

	return fb, err
}
