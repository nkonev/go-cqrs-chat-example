package handlers

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.uber.org/fx"
	"log/slog"
	"net/http"
	"time"
)

func bindHttpHandlers(
	ginRouter *gin.Engine,
	chatHandler *ChatHandler,
	participantHandler *ParticipantHandler,
	messageHandler *MessageHandler,
) {
	ginRouter.POST("/chat", chatHandler.CreateChat)
	ginRouter.PUT("/chat", chatHandler.EditChat)
	ginRouter.DELETE("/chat/:id", chatHandler.DeleteChat)
	ginRouter.PUT("/chat/:id/pin", chatHandler.PinChat)
	ginRouter.GET("/chat/search", chatHandler.SearchChats)

	ginRouter.PUT("/chat/:id/participant", participantHandler.AddParticipant)
	ginRouter.DELETE("/chat/:id/participant", participantHandler.DeleteParticipant)
	ginRouter.GET("/chat/:id/participants", participantHandler.GetParticipants)

	ginRouter.POST("/chat/:id/message", messageHandler.CreateMessage)
	ginRouter.PUT("/chat/:id/message", messageHandler.EditMessage)
	ginRouter.DELETE("/chat/:id/message/:messageId", messageHandler.DeleteMessage)
	ginRouter.PUT("/chat/:id/message/:messageId/read", messageHandler.ReadMessage)
	ginRouter.GET("/chat/:id/message/search", messageHandler.SearchMessages)

	ginRouter.GET("/internal/health", func(g *gin.Context) {
		g.Status(http.StatusOK)
	})
}

func getUserId(g *gin.Context) (int64, error) {
	uh := g.Request.Header.Get("X-UserId")
	return utils.ParseInt64(uh)
}

func ConfigureHttpServer(
	cfg *config.AppConfig,
	slogLogger *slog.Logger,
	lc fx.Lifecycle,
	chatHandler *ChatHandler,
	participantHandler *ParticipantHandler,
	messageHandler *MessageHandler,
) *http.Server {
	// https://gin-gonic.com/en/docs/examples/graceful-restart-or-stop/
	gin.SetMode(gin.ReleaseMode)
	ginRouter := gin.New()
	ginRouter.Use(otelgin.Middleware(app.TRACE_RESOURCE))
	ginRouter.Use(StructuredLogMiddleware(slogLogger))
	ginRouter.Use(WriteTraceToHeaderMiddleware())
	ginRouter.Use(gin.Recovery())

	bindHttpHandlers(ginRouter, chatHandler, participantHandler, messageHandler)

	httpServer := &http.Server{
		Addr:           cfg.HttpServerConfig.Address,
		Handler:        ginRouter.Handler(),
		ReadTimeout:    cfg.HttpServerConfig.ReadTimeout,
		WriteTimeout:   cfg.HttpServerConfig.WriteTimeout,
		MaxHeaderBytes: cfg.HttpServerConfig.MaxHeaderBytes,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping http server")

			if err := httpServer.Shutdown(context.Background()); err != nil {
				slogLogger.Error("Error shutting http server", "err", err)
			}
			return nil
		},
	})

	return httpServer
}
func StructuredLogMiddleware(slogLogger *slog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		traceId := logger.GetTraceId(c.Request.Context())

		// Start timer
		start := time.Now()

		// Process Request
		c.Next()

		// Stop timer
		end := time.Now()

		duration := end.Sub(start)

		entries := []any{
			"client_ip", c.ClientIP(),
			"duration", duration,
			"method", c.Request.Method,
			"path", c.Request.RequestURI,
			"status", c.Writer.Status(),
			"referrer", c.Request.Referer(),
			logger.LogFieldTraceId, traceId,
		}

		if c.Writer.Status() >= 500 {
			slogLogger.Error("Request", entries...)
		} else {
			slogLogger.Info("Request", entries...)
		}
	}
}

func WriteTraceToHeaderMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		traceId := logger.GetTraceId(c.Request.Context())

		c.Writer.Header().Set("trace-id", traceId)

		// Process Request
		c.Next()

	}
}

func RunHttpServer(
	slogLogger *slog.Logger,
	httpServer *http.Server,
) {
	go func() {
		err := httpServer.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			slogLogger.Info("Http server is closed")
		} else if err != nil {
			slogLogger.Error("Got http server error", "err", err)
		}
	}()
}
