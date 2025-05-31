package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
	"slices"
)

type ChatHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
}

func NewChatHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
) *ChatHandler {
	return &ChatHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
	}
}

func (ch *ChatHandler) CreateChat(g *gin.Context) {
	ccd := new(ChatCreateDto)

	err := g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ChatCreateDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		Title:          ccd.Title,
		ParticipantIds: ccd.ParticipantIds,
	}

	if !slices.Contains(cc.ParticipantIds, userId) {
		cc.ParticipantIds = append(cc.ParticipantIds, userId)
	}

	chatId, err := cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	m := IdResponse{Id: chatId}

	g.JSON(http.StatusOK, m)
}

func (ch *ChatHandler) EditChat(g *gin.Context) {
	ccd := new(ChatEditDto)

	err := g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ChatEditDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatEdit{
		AdditionalData:      cqrs.GenerateMessageAdditionalData(),
		ChatId:              ccd.Id,
		Title:               ccd.Title,
		ParticipantIdsToAdd: ccd.ParticipantIds,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatEdit command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) DeleteChat(g *gin.Context) {

	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) PinChat(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	p := g.Query("pin")

	pin := utils.GetBoolean(p)

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatPin{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		Pin:            pin,
		ParticipantId:  userId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatPin command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) SearchChats(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	chats, err := ch.commonProjection.GetChats(g.Request.Context(), userId)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, chats)
}
