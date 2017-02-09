package swaggering

// Model represents a Swagger model
type Model struct {
	Id, Description, Discriminator string
	GoPackage, GoName              string
	GoUses                         bool
	Required, SubTypes             []string
	Properties                     map[string]*Property
	Enums                          []Enum
}
