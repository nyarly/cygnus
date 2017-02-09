package swaggering

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

type (
	// DTO is an interface for a generic data transfer object.
	DTO interface {
		Populate(io.ReadCloser) error
		Absorb(DTO) error
		FormatText() string
		FormatJSON() string
	}

	// Fielder is an interface for an object with optional fields
	// This is the most surprising aspect of swaggering, but obvious on reflection.
	// JSON interfaces often treat the _absence_ of a field as very different from
	// its presence, regardless of the value of the field. { name: "Judson" } is
	// semantically different from { name: "Judson", job: undefined }.
	// It's important to distinguish absence from zero, therefore.
	Fielder interface {
		FieldsPresent() []string
		GetField(string) (interface{}, error)
		SetField(string, interface{}) error
		ClearField(string) error
		LoadMap(map[string]interface{}) error
	}

	// StringList - it's a list, now with 100% more string
	StringList []string
)

// Populate loads a StringList from json
func (list *StringList) Populate(jsonReader io.ReadCloser) (err error) {
	return ReadPopulate(jsonReader, list)
}

// FormatText formats a StringList as text
func (list *StringList) FormatText() string {
	return strings.Join(*list, "\n")
}

// FormatJSON formats a StringList to JSON
func (list *StringList) FormatJSON() string {
	return FormatJSON(list)
}

// Absorb implements DTO for StringList
func (list *StringList) Absorb(other DTO) error {
	if like, ok := other.(*StringList); ok {
		*list = *like
		return nil
	}

	return fmt.Errorf("A StringList cannot absorb from a %T (%v)", other, other)

}

// ReadPopulate reads from jsonReader in order to fill in target
func ReadPopulate(jsonReader io.ReadCloser, target interface{}) (err error) {
	data := make([]byte, 0, 1024)
	chunk := make([]byte, 1024)
	for {
		var count int
		count, err = jsonReader.Read(chunk)
		data = append(data, chunk[:count]...)

		if err == io.EOF {
			jsonReader.Close()
			break
		}
		if err != nil {
			return
		}
	}

	if len(data) == 0 {
		err = nil
		return
	}

	/*
		pretty := &bytes.Buffer{}
		json.Indent(pretty, data, "", "  ")
		fmt.Println(pretty.String())
	*/
	err = json.Unmarshal(data, target)
	return
}

// MarshalJSON marshals a Fielder to JSON, omitting fields that aren't present
func MarshalJSON(dto Fielder) (buf []byte, err error) {
	data := make(map[string]interface{})
	for _, name := range dto.FieldsPresent() {
		data[name], _ = dto.GetField(name)
	}
	return json.Marshal(data)
}

// LoadMap loads a map of values into a Fielder
func LoadMap(dto Fielder, from map[string]interface{}) (Fielder, error) {
	return dto, dto.LoadMap(from)
}

// FormatText formats a DTO
func FormatText(dto interface{}) string {
	return fmt.Sprintf("%+v", dto)
}

// FormatJSON formats a dto as JSON
func FormatJSON(dto interface{}) string {
	str, err := json.Marshal(dto)
	if err != nil {
		return "&lt;<XXXX>>"
	}
	buf := bytes.Buffer{}
	json.Indent(&buf, str, "", "  ")
	return buf.String()
}

// PresenceFromMap takes a map from names to bools and returns the names that
// are "present"
func PresenceFromMap(m map[string]bool) []string {
	var presence []string
	for name, present := range m {
		if present {
			presence = append(presence, name)
		}
	}
	return presence
}

// LoadMapIntoDTO loads a map of key/values into a DTO, setting their presence
// as they're loaded
func LoadMapIntoDTO(from map[string]interface{}, dto Fielder) error {
	errs := make([]string, 0)
	for name, value := range from {
		if err := dto.SetField(name, value); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

// vim: set ft=go:
