package swaggering

// DataType represents an abstract datatype as described by swagger
type DataType struct {
	GoTypePrefix, GoPackage, GoBaseType string
	GoTypeInvalid, GoModel              bool
	Type, Format                        string
	Ref                                 string `json:"$ref"`
	Enum                                []string
	EnumDesc                            Enum
}

func (dt *DataType) findGoType(context *Context) (err error) {
	return findGoType(context, dt, dt)
}
