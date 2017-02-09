package swaggering

func Process(importName, packageName, serviceSource, renderTarget string) {
	context := NewContext(packageName, importName)

	ProcessService(serviceSource, context)
	ResolveService(context)
	RenderService(renderTarget, context)
}
