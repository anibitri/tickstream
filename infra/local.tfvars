# LocalStack (used by `make up`). Inside Docker Compose the endpoint is
# http://localstack:4566; the Makefile overrides it when needed.
env                 = "local"
localstack_endpoint = "http://localhost:4566"
