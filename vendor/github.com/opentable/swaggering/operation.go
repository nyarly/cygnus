package swaggering

// Operation represents an operation on an API in swagger
type Operation struct {
	Nickname, Method, Path, Deprecated string
	GoMethodName                       string
	HasBody, DTORequest                bool
	Parameters                         []*Parameter
	ResponseMessages                   []*ResponseMessage
	Collection
}

// MakesResult indicates that the result value for this operation should be
// allocated in Go using make() as opposed to new()
func (op *Operation) MakesResult() bool {
	return (op.GoTypePrefix == "" && op.GoBaseType != "swaggering.StringList")
}

func (op *Operation) findGoType(context *Context) (err error) {
	switch op.Type {
	case "void":
		op.GoBaseType = ""
	case "array":
		op.Collection.findGoType(context)
		op.DTORequest = true

	case "":
		// Singularity's swagger has some bugs...
		op.Type = "array"
		op.Collection.findGoType(context)
		op.DTORequest = true

	case "string", "bool", "integer", "number":
		typeName, err := op.goPrimitiveType()
		op.setGoType(typeName, err)
	default:
		err = context.modelFor(op.Type, &op.DataType)
		op.DTORequest = true
	}

	return
}
