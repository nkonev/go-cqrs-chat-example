package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
)

type ParticipantHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
}

func NewParticipantHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
) *ParticipantHandler {
	return &ParticipantHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
	}
}

func (ch *ParticipantHandler) AddParticipant(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(ParticipantAddDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ParticipantAddDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantAdd{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ParticipantAdd command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) DeleteParticipant(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(ParticipantDeleteDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ParticipantDeleteDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ParticipantDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) GetParticipants(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	participantsPage := utils.FixPageString(g.Query("page"))
	participantsSize := utils.FixSizeString(g.Query("size"))
	participantsOffset := utils.GetOffset(participantsPage, participantsSize)

	participants, err := ch.commonProjection.GetParticipantIdsForExternal(g.Request.Context(), chatId, participantsSize, participantsOffset)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting participants", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, participants)
}
