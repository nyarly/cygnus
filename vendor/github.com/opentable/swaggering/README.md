# Swaggering
Swagger 1.2 code generator

(For Swagger 2.0, please see https://github.com/go-swagger/go-swagger)

Swaggering is a library for generating code based on Swagger 1.2 JSON description files.

You'll very likely want to look at opentable/swagger-client-maker to actually make use of this code,
but you're encouraged to review and reuse this library.

# Note

My experience has been that Swagger JSON descriptions can be flaky.
Swaggering does its best to generate code anyway -
when there are references to models that don't exist in the API
Swaggering templates in struct fields but comments them out.
I've found the best thing is to amend the JSON rather than the resulting code.

# Building

Note that the default templates live in defaultApi.tmpl and defaultModel.tmpl -
if you update those files, you need to run `go generate` to update templates.go.

# Future Work

Maybe it'd be best to do all this with gofmt stuff instead of templates.
c.f. https://github.com/sasha-s/go-inline for ideas.
