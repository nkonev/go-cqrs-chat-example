package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"log/slog"
	"net/http"
)

type ParticipantHandler struct {
	slogLogger       *slog.Logger
	eventBus         *cqrs.PartitionAwareEventBus
	commonProjection *cqrs.CommonProjection
}

func NewParticipantHandler(
	slogLogger *slog.Logger,
	eventBus *cqrs.PartitionAwareEventBus,
	commonProjection *cqrs.CommonProjection,
) *ParticipantHandler {
	return &ParticipantHandler{
		slogLogger:       slogLogger,
		eventBus:         eventBus,
		commonProjection: commonProjection,
	}
}

func (ch *ParticipantHandler) AddParticipant(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(ParticipantAddDto)

	err = g.Bind(ccd)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error binding ParticipantAddDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantAdd{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.commonProjection)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error sending ParticipantAdd command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) DeleteParticipant(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(ParticipantDeleteDto)

	err = g.Bind(ccd)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error binding ParticipantDeleteDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.commonProjection)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error sending ParticipantDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) GetParticipants(g *gin.Context) {
	cid := g.Param("id")

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	participants, err := ch.commonProjection.GetParticipants(g.Request.Context(), chatId)
	if err != nil {
		logger.LogWithTrace(g.Request.Context(), ch.slogLogger).Error("Error getting participants", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, participants)
}
