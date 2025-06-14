package cmd

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/handlers"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.uber.org/fx"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestUnreads(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user3 int64 = 3
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		user3Chats, err := restClient.GetChatsByUserId(ctx, user3, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user3Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2, user3})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2, user3}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		user3ChatsNew, err := restClient.GetChatsByUserId(ctx, user3, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew))
		chat1OfUser3 := user3ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser3.Title)
		assert.Equal(t, int64(1), chat1OfUser3.UnreadMessages)

		err = restClient.ReadMessage(ctx, user2, chat1Id, message1.Id)
		assert.NoError(t, err, "error in reading message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser22 := user2ChatsNew2[0]
		assert.Equal(t, int64(0), chat1OfUser22.UnreadMessages)

		user3ChatsNew2, err := restClient.GetChatsByUserId(ctx, user3, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew2))
		chat1OfUser32 := user3ChatsNew2[0]
		assert.Equal(t, int64(1), chat1OfUser32.UnreadMessages)

		const message2Text = "new message 2"
		_, err = restClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		assert.NoError(t, err, "error in creating message")

		const message3Text = "new message 3"
		messageId3, err := restClient.CreateMessage(ctx, user1, chat1Id, message3Text)
		assert.NoError(t, err, "error in creating message")
		assert.True(t, messageId3 > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew3, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew3))
		chat1OfUser23 := user2ChatsNew3[0]
		assert.Equal(t, int64(2), chat1OfUser23.UnreadMessages)

		user3ChatsNew3, err := restClient.GetChatsByUserId(ctx, user3, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew3))
		chat1OfUser33 := user3ChatsNew3[0]
		assert.Equal(t, int64(3), chat1OfUser33.UnreadMessages)

		err = restClient.DeleteMessage(ctx, user1, chat1Id, messageId3)
		assert.NoError(t, err, "error in delete message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew4, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew4))
		chat1OfUser24 := user2ChatsNew4[0]
		assert.Equal(t, int64(1), chat1OfUser24.UnreadMessages)

		user3ChatsNew4, err := restClient.GetChatsByUserId(ctx, user3, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew4))
		chat1OfUser34 := user3ChatsNew4[0]
		assert.Equal(t, int64(2), chat1OfUser34.UnreadMessages)
	})
}

func TestPinChat(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = restClient.PinChat(ctx, user1, chat1Id, true)
		assert.NoError(t, err, "error in pinning chats")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, true, chat1OfUser1New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser2New2 := user2ChatsNew2[0]
		assert.Equal(t, false, chat1OfUser2New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2New2.Title)
		assert.Equal(t, int64(1), chat1OfUser2New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2New2.LastMessageContent)
	})

}

func TestDeleteChat(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = restClient.DeleteChat(ctx, chat1Id)
		assert.NoError(t, err, "error in removing chats")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user1ChatsNew2))

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))
	})

}

func TestAddParticipant(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, int64(1), chat1OfUser1.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1.ParticipantIds)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2.ParticipantIds)

		const chat1NewName = "new chat 1 renamed"
		err = restClient.EditChat(ctx, user1, chat1NewName, false)
		assert.NoError(t, err, "error in changing chat")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser1New2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser1New2.ParticipantIds)

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser2New2 := user2ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser2New2.Title)
		assert.Equal(t, int64(1), chat1OfUser2New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2New2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2New2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2New2.ParticipantIds)
	})
}

func TestDeleteParticipant(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, int64(1), chat1OfUser1.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1.ParticipantIds)

		user2Chats, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = restClient.AddChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in adding participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1, user2}, chat1Participants)

		user2ChatsNew, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2.ParticipantIds)

		err = restClient.DeleteChatParticipants(ctx, chat1Id, []int64{user2})
		assert.NoError(t, err, "error in removing chat participants")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := restClient.GetChatsByUserId(ctx, user2, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))

		chat1Participants2, err := restClient.GetChatParticipants(ctx, chat1Id)
		assert.NoError(t, err, "error in chat participants")
		assert.Equal(t, []int64{user1}, chat1Participants2)

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, int64(1), chat1OfUser1New2.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1New2.ParticipantIds)
	})
}

func TestEditMessage(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		assert.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"
		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")

		const message2Text = "new message 2"
		message2Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, message2Text, *chat1OfUser1.LastMessageContent)

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 2, len(chat1Messages))
		message1 := chat1Messages[0]
		message2 := chat1Messages[1]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
		assert.Equal(t, message2Id, message2.Id)
		assert.Equal(t, message2Text, message2.Content)

		const message1TextNew = "new message 1 edited"
		err = restClient.EditMessage(ctx, user1, chat1Id, message1.Id, message1TextNew)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew))
		chat1OfUser1New := user1ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser1New.Title)
		assert.Equal(t, int64(0), chat1OfUser1New.UnreadMessages)
		assert.Equal(t, message2Text, *chat1OfUser1New.LastMessageContent)

		chat1MessagesNew, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 2, len(chat1MessagesNew))
		message1New := chat1MessagesNew[0]
		message2New := chat1MessagesNew[1]
		assert.Equal(t, message1Id, message1New.Id)
		assert.Equal(t, message1TextNew, message1New.Content)
		assert.Equal(t, message2Id, message2New.Id)
		assert.Equal(t, message2Text, message2New.Content)

		const message2TextNew = "new message 1 edited"
		err = restClient.EditMessage(ctx, user1, chat1Id, message2.Id, message2TextNew)
		assert.NoError(t, err, "error in creating message")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := restClient.GetChatsByUserId(ctx, user1, nil)
		assert.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message2TextNew, *chat1OfUser1New2.LastMessageContent)

		chat1MessagesNew2, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		assert.NoError(t, err, "error in getting messages")
		assert.Equal(t, 2, len(chat1MessagesNew2))
		message1New2 := chat1MessagesNew2[0]
		message2New2 := chat1MessagesNew2[1]
		assert.Equal(t, message1Id, message1New2.Id)
		assert.Equal(t, message1TextNew, message1New2.Content)
		assert.Equal(t, message2Id, message2New2.Id)
		assert.Equal(t, message2TextNew, message2New2.Content)
	})
}

func TestBlog(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)

		err = restClient.EditChat(ctx, user1, chat1Name, false)
		assert.NoError(t, err)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		const message1Text = "new message 1"
		message1Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		assert.NoError(t, err, "error in creating message")

		const message2Text = "new message 2"
		message2Id, err := restClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		assert.NoError(t, err, "error in creating message")

		err = restClient.MakeMessageBlogPost(ctx, chat1Id, message1Id)
		assert.NoError(t, err, "error in making message blog post")
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		blogs, err := restClient.SearchBlogs(ctx)
		assert.NoError(t, err, "error in searching blog posts")
		assert.Equal(t, 1, len(blogs))
		assert.Equal(t, chat1Id, blogs[0].Id)
		assert.Equal(t, chat1Name, blogs[0].Title)

		comments, err := restClient.SearchBlogComments(ctx, chat1Id)
		assert.NoError(t, err, "error in searching blog comments")
		assert.Equal(t, 1, len(comments))
		assert.Equal(t, message2Id, comments[0].Id)
		assert.Equal(t, message2Text, comments[0].Content)
	})
}

func TestChatPaginate(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		dba *db.DB,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const num = 1000
		const chatPrefix = "generated_chat"

		ctx := context.Background()

		var lastChatId int64
		var err error
		for i := 1; i <= num; i++ {
			lastChatId, err = restClient.CreateChat(ctx, user1, chatPrefix+utils.ToString(i))
			assert.NoError(t, err, "error in creating chat")
			assert.True(t, lastChatId > 0)
		}
		waitForChatExists(lgr, dba, lastChatId)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// get initial page
		query1 := url.Values{
			handlers.SizeParam: []string{utils.ToString(40)},
		}
		resp1, err := restClient.GetChatsByUserId(ctx, user1, &query1)
		assert.NoError(t, err)
		assert.Equal(t, 40, len(resp1))
		assert.Equal(t, "generated_chat1000", resp1[0].Title)
		assert.Equal(t, "generated_chat999", resp1[1].Title)
		assert.Equal(t, "generated_chat998", resp1[2].Title)
		assert.Equal(t, "generated_chat961", resp1[39].Title)

		lastPinned := resp1[len(resp1)-1].Pinned
		lastId := resp1[len(resp1)-1].Id
		lastLastUpdateDateTime := resp1[len(resp1)-1].UpdateDateTime

		// get second page
		query2 := url.Values{
			handlers.SizeParam: []string{utils.ToString(40)},

			handlers.PinnedParam:             []string{utils.ToString(lastPinned)},
			handlers.LastUpdateDateTimeParam: []string{lastLastUpdateDateTime.Format(time.RFC3339Nano)},
			handlers.ChatIdParam:             []string{utils.ToString(lastId)},
		}
		resp2, err := restClient.GetChatsByUserId(ctx, user1, &query2)
		assert.NoError(t, err)
		assert.Equal(t, 40, len(resp2))
		assert.Equal(t, "generated_chat960", resp2[0].Title)
		assert.Equal(t, "generated_chat959", resp2[1].Title)
		assert.Equal(t, "generated_chat958", resp2[2].Title)
		assert.Equal(t, "generated_chat921", resp2[39].Title)
	})
}

func TestMessagePaginate(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.RestClient,
		saramaClient sarama.Client,
		dba *db.DB,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const chat1Name = "new chat 1"
		const num = 500

		ctx := context.Background()

		chat1Id, err := restClient.CreateChat(ctx, user1, chat1Name)
		assert.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		const messagePrefix = "generated_message"

		var lastMessageId int64
		for i := 1; i <= num; i++ {
			lastMessageId, err = restClient.CreateMessage(ctx, user1, chat1Id, messagePrefix+utils.ToString(i))
			assert.NoError(t, err, "error in creating message")
		}
		waitForMessageExists(lgr, dba, chat1Id, lastMessageId)
		assert.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// get first page
		query1 := url.Values{
			handlers.SizeParam:          []string{utils.ToString(3)},
			handlers.StartingFromItemId: []string{utils.ToString(6)},
		}
		resp1, err := restClient.GetMessages(ctx, user1, chat1Id, &query1)
		assert.NoError(t, err)

		assert.Equal(t, 3, len(resp1))
		assert.True(t, strings.HasPrefix(resp1[0].Content, "generated_message7")) // different from chat because of different way of generating test data
		assert.True(t, strings.HasPrefix(resp1[1].Content, "generated_message8"))
		assert.True(t, strings.HasPrefix(resp1[2].Content, "generated_message9"))
		assert.Equal(t, int64(7), resp1[0].Id)
		assert.Equal(t, int64(8), resp1[1].Id)
		assert.Equal(t, int64(9), resp1[2].Id)

		lastId := resp1[len(resp1)-1].Id

		// get second page
		query2 := url.Values{
			handlers.SizeParam:          []string{utils.ToString(3)},
			handlers.StartingFromItemId: []string{utils.ToString(lastId)},
		}
		resp2, err := restClient.GetMessages(ctx, user1, chat1Id, &query2)
		assert.NoError(t, err)

		assert.Equal(t, 3, len(resp2))
		assert.True(t, strings.HasPrefix(resp2[0].Content, "generated_message10"))
		assert.True(t, strings.HasPrefix(resp2[1].Content, "generated_message11"))
		assert.True(t, strings.HasPrefix(resp2[2].Content, "generated_message12"))
		assert.Equal(t, int64(10), resp2[0].Id)
		assert.Equal(t, int64(11), resp2[1].Id)
		assert.Equal(t, int64(12), resp2[2].Id)
	})
}
