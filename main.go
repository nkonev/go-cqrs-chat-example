package main

import "go-cqrs-chat-example/cmd"

// TODO paginate in responses with keyset for chat
// TODO check for existence in chat edit, message edit and skip if no exists
func main() {
	cmd.Execute()
}
