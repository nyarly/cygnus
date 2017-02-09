package swaggering

// Property represents a field in a swagger model
type Property struct {
	SwaggerName, GoName string
	Collection
}
