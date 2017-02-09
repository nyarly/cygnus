package swaggering

type (
	Swagger struct {
		BasePath, ResourcePath string
		Apis                   []Api
		Models                 map[string]*Model
	}

	Api struct {
		Path, Description string
		BasePackageName   string
		PackageImportName string
		Operations        []*Operation
	}

	Parameter struct {
		ParamType, Name         string
		Required, AllowMultiple bool
		Collection
	}

	ResponseMessage struct {
		Code                   int
		Message, ResponseModel string
		model                  *Model
	}

	Enum struct {
		Name   string
		Values []string
	}
)
