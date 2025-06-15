package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
	"slices"
	"time"
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
		Blog:                ccd.Blog,
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

	cid := g.Param(ChatIdParam)

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
	cid := g.Param(ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	p := g.Query(PinParam)

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

	size := utils.FixSizeString(g.Query(SizeParam))
	reverse := utils.GetBoolean(g.Query(ReverseParam))

	pinned := utils.GetBooleanNullable(g.Query(PinnedParam))
	lastUpdateDateTime := utils.GetTimeNullable(g.Query(LastUpdateDateTimeParam))
	id := utils.ParseInt64Nullable(g.Query(ChatIdParam))
	startingFromItemId := ch.convertChatId(pinned, lastUpdateDateTime, id)

	includeStartingFrom := utils.GetBoolean(g.Query(IncludeStartingFromParam))

	chats, err := ch.commonProjection.GetChats(g.Request.Context(), userId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, chats)
}

func (ch *ChatHandler) convertChatId(pinned *bool, lastUpdateDateTime *time.Time, id *int64) *cqrs.ChatId {
	if pinned == nil || lastUpdateDateTime == nil || id == nil {
		return nil
	}
	return &cqrs.ChatId{
		Pinned:             *pinned,
		LastUpdateDateTime: *lastUpdateDateTime,
		Id:                 *id,
	}
}
