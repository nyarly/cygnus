package swaggering

import (
	"fmt"
	"log"
)

type Context struct {
	packageName, importName string
	swaggers                []*Swagger
	apis                    []*Api
	models                  []*Model
	openModels              []*Model
}

func NewContext(packageName, importName string) (context *Context) {
	context = &Context{packageName: packageName, importName: importName}
	context.swaggers = make([]*Swagger, 0)

	return
}

func (context *Context) Resolve() {
	for _, swagger := range context.swaggers {
		for adx := range swagger.Apis {
			context.apis = append(context.apis, &swagger.Apis[adx])
		}
		for _, model := range swagger.Models {
			context.models = append(context.models, model)
		}
	}

	log.Printf("  Found %d apis", len(context.apis))
	log.Printf("  Found %d models", len(context.models))

	context.openModels = make([]*Model, 0, len(context.models))

	context.resolveApis()
	context.resolveModels()
}

func (context *Context) resolveModels() {
	var cur *Model

	for len(context.openModels) > 0 {
		cur, context.openModels = context.openModels[0], context.openModels[1:]
		if cur.GoUses {
			continue
		}
		context.resolveModel(cur)
	}
}

func (context *Context) resolveModel(model *Model) {
	model.GoUses = true
	model.GoName = model.Id
	model.GoPackage = "dtos"
	for name, prop := range model.Properties {
		context.resolveProperty(name, prop)
		if prop.EnumDesc.Name != "" {
			prop.GoBaseType = string(append([]byte(model.Id), prop.GoBaseType...))
			prop.EnumDesc.Name = prop.GoBaseType
			exists := false
			for _, e := range model.Enums {
				if e.Name == prop.EnumDesc.Name {
					exists = true
					break
				}
			}

			if !exists {
				model.Enums = append(model.Enums, prop.EnumDesc)
			}
		}
	}
}

func (context *Context) resolveProperty(name string, prop *Property) {
	prop.SwaggerName = name
	prop.GoName = capitalize(name)
	prop.findGoType(context)
}

func (context *Context) resolveApis() {
	var err error

	for _, api := range context.apis {
		api.BasePackageName = context.packageName
		api.PackageImportName = context.importName
		for _, op := range api.Operations {
			op.Path = api.Path
			op.GoMethodName = capitalize(op.Nickname)
			err = op.findGoType(context)
			logErr(err, "Operation %s invalid: %v", op.Nickname)

			for _, parm := range op.Parameters {
				err = parm.findGoType(context)
				logErr(err, "Operation %s invalid: parameter %s: %v", op.Nickname, parm.Name)

				if parm.GoTypeInvalid {
					op.GoTypeInvalid = true
				}

				if parm.Name == "body" {
					op.HasBody = true
				}
			}
		}
	}

}

func (context *Context) modelFor(typeName string, to *DataType) (err error) {
	err = context.modelUsed(typeName)
	to.GoModel = true
	to.GoPackage = "dtos"
	to.GoTypePrefix = "*"
	to.setGoType(typeName, err)
	return
}

func (context *Context) modelUsed(name string) (err error) {
	for _, model := range context.models {
		if model.Id == name {
			if !model.GoUses {
				context.openModels = append(context.openModels, model)
			}
			return
		}

	}
	err = fmt.Errorf("Model %q doesn't appear in known models.", name)
	return
}
