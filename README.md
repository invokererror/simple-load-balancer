# simple-load-balancer
A simple load balancer in Go, based on [https://github.com/kasvith/simplelb].

Added code for setting up backends such that you can `curl http://localhost:3030` (assuming default load balancer address) and see logs flowing. The address for backends are hardcoded, though.

# build instructions
in the terminal, at the right working directory, type:

```
go build main.go
```

This should generate a binary called "main". Then

```
./main --backends=http://localhost:60000,http://localhost:60001
```

You can use `curl` to make an HTTP request to the load balancer.
