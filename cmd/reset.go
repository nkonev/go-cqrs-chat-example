/*
Copyright © 2025 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"go.uber.org/fx"
	"os"

	"github.com/spf13/cobra"
)

// resetCmd represents the reset command
var resetCmd = &cobra.Command{
	Use:   "reset",
	Short: "Reset offsets and storage",
	Long:  `Reset offsets in Kafka for configured topic and consumer group, drops all the tables, sequences from PostgreSQL and creates empty ones with help of migration.`,
	Run: func(cmd *cobra.Command, args []string) {
		RunReset()
	},
}

func init() {
	rootCmd.AddCommand(resetCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// resetCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// resetCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}

func RunReset() {
	cfg, err := config.CreateTypedConfig()
	if err != nil {
		panic(err)
	}
	baseLogger := logger.NewBaseLogger(os.Stdout, cfg)
	lgr := logger.NewLogger(baseLogger)

	lgr.Info("Start reset command")

	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.Logger(lgr),
		fx.Provide(
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureCommonProjection,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunResetPartitions,
			db.RunMigrations,
			kafka.RunCreateTopic,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit reset command")
}
