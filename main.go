package main

import "go-cqrs-chat-example/cmd"

// TODO paginate in responses with keyset for chat
// TODO add timestamp for chat participant and order last n participants by it
func main() {
	cmd.Execute()
}
