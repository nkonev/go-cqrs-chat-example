package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
)

type MessageHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
}

func NewMessageHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
) *MessageHandler {
	return &MessageHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
	}
}

func (mc *MessageHandler) CreateMessage(g *gin.Context) {
	cid := g.Param(ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mcd := new(MessageCreateDto)

	err = g.Bind(mcd)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding MessageCreateDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		Content:        mcd.Content,
		OwnerId:        userId,
	}

	mid, wasAdded, err := cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error sending MessageCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if !wasAdded {
		g.Status(http.StatusTeapot)
		return
	}

	m := IdResponse{Id: mid}

	g.JSON(http.StatusOK, m)
}

func (mc *MessageHandler) EditMessage(g *gin.Context) {
	cid := g.Param(ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(MessageEditDto)

	err = g.Bind(ccd)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding MessageEditDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageEdit{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		MessageId:      ccd.Id,
		ChatId:         chatId,
		Content:        ccd.Content,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, userId)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error sending MessageEdit command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) DeleteMessage(g *gin.Context) {
	cid := g.Param(ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(MessageIdParam)
	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		MessageId:      messageId,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, userId)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error sending MessageDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) ReadMessage(g *gin.Context) {
	cid := g.Param(ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(MessageIdParam)

	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
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
		mc.lgr.WithTrace(g.Request.Context()).Error("Error sending MessageRead command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) MakeBlogPost(g *gin.Context) {
	cid := g.Param(ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(MessageIdParam)

	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MakeMessageBlogPost{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		MessageId:      messageId,
		BlogPost:       true,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error sending MakeMessageBlogPost command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) SearchMessages(g *gin.Context) {
	cid := g.Param(ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	size := utils.FixSizeString(g.Query(SizeParam))
	reverse := utils.GetBoolean(g.Query(ReverseParam))
	startingFromItemIdString := g.Query(StartingFromItemId)
	var startingFromItemId *int64
	if startingFromItemIdString != "" {
		startingFromItemId2, err := utils.ParseInt64(startingFromItemIdString) // exclusive
		if err != nil {
			mc.lgr.WithTrace(g.Request.Context()).Error("Error parsing startingFromItemId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		startingFromItemId = &startingFromItemId2
	}
	includeStartingFrom := utils.GetBoolean(g.Query(IncludeStartingFromParam))

	messages, err := mc.commonProjection.GetMessages(g.Request.Context(), chatId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		mc.lgr.WithTrace(g.Request.Context()).Error("Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, messages)
}
