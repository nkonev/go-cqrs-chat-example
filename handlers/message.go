package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"log/slog"
	"net/http"
)

type MessageHandler struct {
	slogLogger       *slog.Logger
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
}

func NewMessageHandler(
	slogLogger *slog.Logger,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
) *MessageHandler {
	return &MessageHandler{
		slogLogger:       slogLogger,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
	}
}

func (mc *MessageHandler) CreateMessage(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mcd := new(MessageCreateDto)

	err = g.Bind(mcd)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding MessageCreateDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		Content:        mcd.Content,
		OwnerId:        userId,
	}

	mid, err := cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error sending MessageCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	m := IdResponse{Id: mid}

	g.JSON(http.StatusOK, m)
}

func (mc *MessageHandler) EditMessage(g *gin.Context) {
	cid := g.Param("id")
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(MessageEditDto)

	err = g.Bind(ccd)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding MessageEditDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageEdit{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		MessageId:      ccd.Id,
		ChatId:         chatId,
		Content:        ccd.Content,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, userId)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error sending MessageEdit command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) DeleteMessage(g *gin.Context) {
	cid := g.Param("id")
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param("messageId")
	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		MessageId:      messageId,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, userId)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error sending MessageDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) ReadMessage(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param("messageId")

	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MessageRead{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		MessageId:      messageId,
		ParticipantId:  userId,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error sending MessageRead command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) SearchMessages(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	messages, err := mc.commonProjection.GetMessages(g.Request.Context(), chatId)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), mc.slogLogger).Error("Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, messages)
}
