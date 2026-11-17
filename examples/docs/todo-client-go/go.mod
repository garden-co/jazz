module github.com/garden-co/jazz/examples/docs/todo-client-go

go 1.22

require github.com/garden-co/jazz/packages/jazz-go/v2 v2.0.0

require github.com/ebitengine/purego v0.8.4 // indirect

replace github.com/garden-co/jazz/packages/jazz-go/v2 => ../../../packages/jazz-go

require github.com/garden-co/jazz/examples/docs/todo-client-go/recipes/required-references v0.0.0

replace github.com/garden-co/jazz/examples/docs/todo-client-go/recipes/required-references => ./recipes/required-references
