#phony dependency task that does nothing
#"make executable" does not run if there is a ./executable directory, unless the task has a dependency
phony:

lint:
	golangci-lint run
