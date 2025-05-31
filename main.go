package main

import "go-cqrs-chat-example/cmd"

// TODO blog (title, short_description, full_description)
// TODO paginate over chat participant ids (CommonProjection.GetParticipantIds()), chat ids (CommonProjection.GetChatIds())
// TODO paginate in responses with keyset for chat
func main() {
	cmd.Execute()
}
