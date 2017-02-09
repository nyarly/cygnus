package swaggering

// Collection represents a list-type from swagger
type Collection struct {
	DataType
	Items       DataType
	UniqueItems bool
}

func (c *Collection) findGoType(context *Context) (err error) {
	if c.Type == "array" {
		err = findGoType(context, &c.Items, &c.DataType)
		if err == nil {
			if c.GoModel {
				c.GoTypePrefix = ""
				c.GoPackage = "dtos"
				c.GoBaseType = c.GoBaseType + "List"
			} else if c.GoBaseType == "string" {
				c.GoBaseType = "StringList"
				c.GoPackage = "swaggering"
				c.GoTypePrefix = ""
				c.GoModel = false
			} else {
				c.GoTypePrefix = "[]" + c.GoTypePrefix
			}
		}
	} else {
		err = c.DataType.findGoType(context)
	}
	return
}
