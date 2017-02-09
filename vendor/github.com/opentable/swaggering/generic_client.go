package swaggering

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strings"
)

type (
	// Requester defines the interface that Swaggering uses to
	// make actual HTTP requests of the API server
	Requester interface {
		// DTORequest performs an HTTP request and populates a DTO based on the response
		DTORequest(dto DTO, method, path string, pathParams, queryParams urlParams, body ...DTO) error

		// Request performs an HTTP request and returns the body of the response
		Request(method, path string, pathParams, queryParams urlParams, body ...DTO) (io.ReadCloser, error)
	}

	// GenericClient is a generic client for Swagger described services
	GenericClient struct {
		BaseURL string
		Logger  Logger
		HTTP    http.Client
	}

	// ReqError represents failures from requests
	ReqError struct {
		Method, Path string
		Message      string
		Status       int
		Body         bytes.Buffer
	}

	urlParams map[string]interface{}
)

func (e *ReqError) Error() string {
	return fmt.Sprintf("%s %s => %s: \n%s", e.Method, e.Path, e.Message, e.Body.String())
}

// DTORequest performs an HTTP request and populates a DTO based on the response
func (gc *GenericClient) DTORequest(pop DTO, method, path string, pathParams, queryParams urlParams, body ...DTO) (err error) {
	resBody, err := gc.Request(method, path, pathParams, queryParams, body...)
	if err != nil {
		return
	}
	err = pop.Populate(resBody)
	return
}

// Request performs an HTTP request and returns the body of the response
func (gc *GenericClient) Request(method, path string, pathParams, queryParams urlParams, body ...DTO) (resBody io.ReadCloser, err error) {
	req, err := gc.buildRequest(method, path, pathParams, queryParams, body...)
	if err != nil {
		return
	}
	res, err := gc.HTTP.Do(req)
	if err != nil {
		return
	}
	if res.StatusCode > 299 {
		rerr := &ReqError{
			Status:  res.StatusCode,
			Message: res.Status,
			Method:  method,
			Path:    path,
			Body:    bytes.Buffer{},
		}
		rerr.Body.ReadFrom(res.Body)
		res.Body.Close()
		err = rerr
		return
	}
	if gc.Logger.Debugging() {
		buf := bytes.Buffer{}
		buf.ReadFrom(res.Body)
		gc.Logger.Debug("response", map[string]interface{}{"url": req.URL, "body": buf.String()})
		resBody = ioutil.NopCloser(&buf)
	} else {
		resBody = res.Body
	}
	return
}

func (gc *GenericClient) buildRequest(method, path string, pathParams, queryParams urlParams, bodies ...DTO) (req *http.Request, err error) {
	url, err := url.Parse(gc.BaseURL)
	if err != nil {
		return
	}

	path, err = pathRender(path, pathParams)
	if err != nil {
		return
	}
	url.Path = strings.Join([]string{strings.TrimRight(url.Path, "/"), strings.TrimLeft(path, "/")}, "/")

	gc.Logger.Debug("URL", map[string]interface{}{"url": url})

	if len(bodies) > 0 {
		req, err = gc.buildBodyRequest(method, url.String(), bodies[0])
	} else {
		req, err = gc.buildBodilessRequest(method, url.String())
	}
	return
}

func (gc *GenericClient) buildBodilessRequest(method, path string) (req *http.Request, err error) {
	gc.Logger.Debug("request [no body]", map[string]interface{}{"method": method, "path": path})
	req, err = http.NewRequest(method, path, nil)
	return
}

func (gc *GenericClient) buildBodyRequest(method, path string, bodyObj DTO) (req *http.Request, err error) {
	buf := bytes.Buffer{}
	enc := json.NewEncoder(&buf)
	enc.Encode(bodyObj) // XXX Here, consider a goroutine and a PipeWriter
	gc.Logger.Debug("request", map[string]interface{}{"method": method, "path": path, "body": buf.String()})
	req, err = http.NewRequest(method, path, &buf)
	req.Header.Add("Content-Type", "application/json")
	return
}

func pathRender(path string, params urlParams) (res string, err error) {
	parmRE := regexp.MustCompile(`{([^}]+)}`)
	building := make([]byte, 0, 150)
	pathBytes := []byte(path)

	start := 0
	indices := parmRE.FindAllSubmatchIndex(pathBytes, -1)
	for _, matches := range indices {
		building = append(building, pathBytes[start:matches[0]]...)
		start = matches[len(matches)-1] + 1
		parmName := pathBytes[matches[2]:matches[3]]
		val, ok := params[string(parmName)]
		if !ok {
			err = fmt.Errorf("Path parameter %q not provided for %s", parmName, path)
			return
		}
		building = append(building, fmt.Sprintf("%v", val)...)
	}
	building = append(building, pathBytes[start:]...)
	res = string(building)

	return
}

func (gc *GenericClient) buildURL(subpath string) (url string) {
	return strings.Join([]string{gc.BaseURL, subpath}, "/")
}
