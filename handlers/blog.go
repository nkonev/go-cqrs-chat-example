package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
)

type BlogHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
}

func NewBlogHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
) *BlogHandler {
	return &BlogHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
	}
}

func (ch *BlogHandler) SearchBlogs(g *gin.Context) {
	chats, err := ch.commonProjection.GetBlogs(g.Request.Context())
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting blogs", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, chats)
}

func (ch *BlogHandler) GetBlog(g *gin.Context) {
	cid := g.Param("id")

	blogId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding blogId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	blog, err := ch.commonProjection.GetBlog(g.Request.Context(), blogId)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting blog", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if blog == nil {
		g.Status(http.StatusNoContent)
		return
	}

	g.JSON(http.StatusOK, blog)
}

func (ch *BlogHandler) SearchComments(g *gin.Context) {
	cid := g.Param("id")

	blogId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding blogId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	chats, err := ch.commonProjection.GetComments(g.Request.Context(), blogId)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting blog comments", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, chats)
}
