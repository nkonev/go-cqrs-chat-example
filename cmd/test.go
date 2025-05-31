package cmd

import (
	"context"
	"github.com/stretchr/testify/assert"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/handlers"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestMain(m *testing.M) {
	setup()
	retCode := m.Run()
	shutdown()
	os.Exit(retCode)
}

func setup() {

}

func shutdown() {

}

func resetInfra(lgr *logger.LoggerWrapper) {
	appFx := fx.New(
		fx.Supply(
			lgr,
		),
		fx.Provide(
			config.CreateTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunDeleteTopic,
			db.RunMigrations,
			kafka.RunCreateTopic,
			app.Shutdown,
		),
	)
	appFx.Run()
}

func runTestFunc(lgr *logger.LoggerWrapper, t *testing.T, testFunc interface{}) {
	var s fx.Shutdowner
	appTestFx := fxtest.New(
		t,
		fx.Supply(lgr),
		fx.Logger(lgr),
		fx.Populate(&s),
		fx.Provide(
			config.CreateTestTypedConfig,
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureKafkaMarshaller,
			cqrs.ConfigureWatermillLogger,
			cqrs.ConfigurePublisher,
			cqrs.ConfigureCqrsRouter,
			cqrs.ConfigureCqrsMarshaller,
			cqrs.ConfigureEventBus,
			cqrs.ConfigureEventProcessor,
			cqrs.ConfigureCommonProjection,
			handlers.NewChatHandler,
			handlers.NewParticipantHandler,
			handlers.NewMessageHandler,
			handlers.ConfigureHttpServer,
			kafka.ConfigureSaramaClient,
			client.NewRestClient,
		),
		fx.Invoke(
			cqrs.RunCqrsRouter,
			handlers.RunHttpServer,
			waitForHealthCheck,
			testFunc,
		),
	)
	defer appTestFx.RequireStart().RequireStop()
	assert.NoError(t, s.Shutdown(), "error in app shutdown")
}

func startAppFull(t *testing.T, testFunc interface{}) {
	baseLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
	lgr := logger.NewLogger(baseLogger)

	resetInfra(lgr)

	runTestFunc(lgr, t, testFunc)
}

func waitForHealthCheck(lgr *logger.LoggerWrapper, restClient *client.RestClient, cfg *config.AppConfig) {
	ctx := context.Background()

	i := 0
	const maxAttempts = 60
	success := false
	for ; i <= maxAttempts; i++ {
		err := restClient.HealthCheck(ctx)
		if err != nil {
			lgr.Info("Awaiting while chat have been started")
			time.Sleep(time.Second * 1)
			continue
		} else {
			success = true
			break
		}
	}
	if !success {
		panic("Cannot await for chat will be started")
	}
	lgr.Info("chat have started")
}
