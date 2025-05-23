package kafka

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"github.com/IBM/sarama"
	"github.com/Jeffail/gabs/v2"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/utils"
	"go.uber.org/fx"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"
)

func ConfigureKafkaAdmin(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	lc fx.Lifecycle,
) (sarama.ClusterAdmin, error) {
	kafkaAdminConfig := sarama.NewConfig()
	kafkaAdminConfig.Version = sarama.V4_0_0_0

	kafkaAdmin, err := sarama.NewClusterAdmin(cfg.KafkaConfig.BootstrapServers, kafkaAdminConfig)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping kafka admin")

			if err := kafkaAdmin.Close(); err != nil {
				slogLogger.Error("Error shutting down kafka admin", "err", err)
			}
			return nil
		},
	})

	return kafkaAdmin, nil
}

func RunCreateTopic(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	retention := cfg.KafkaConfig.Retention
	topicName := cfg.KafkaConfig.Topic
	slogLogger.Info("Creating topic", "topic", topicName)

	err := kafkaAdmin.CreateTopic(topicName, &sarama.TopicDetail{
		NumPartitions:     cfg.KafkaConfig.NumPartitions,
		ReplicationFactor: cfg.KafkaConfig.ReplicationFactor,
		ConfigEntries: map[string]*string{
			// https://kafka.apache.org/documentation/#topicconfigs_retention.ms
			"retention.ms": &retention,
		},
	}, false)
	if errors.Is(err, sarama.ErrTopicAlreadyExists) {
		slogLogger.Info("Topic is already exists", "topic", topicName)
	} else if err != nil {
		return err
	} else {
		slogLogger.Info("Topic was successfully created", "topic", topicName)
	}

	return nil
}

func RunDeleteTopic(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	slogLogger.Warn("Removing topic", "topic", cfg.KafkaConfig.Topic)
	err := kafkaAdmin.DeleteTopic(cfg.KafkaConfig.Topic)
	if err != nil {
		if errors.Is(err, sarama.ErrUnknownTopicOrPartition) {
			slogLogger.Warn("Topic does not exists", "topic", cfg.KafkaConfig.Topic)
		} else {
			return err
		}
	}
	slogLogger.Warn("Topic was removed", "topic", cfg.KafkaConfig.Topic)
	return nil
}

func RunResetPartitions(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	kafkaAdmin sarama.ClusterAdmin,
) error {
	slogLogger.Info("Start reset partitions")

	err := kafkaAdmin.DeleteConsumerGroup(cfg.KafkaConfig.ConsumerGroup)

	if err != nil {
		if strings.Contains(err.Error(), "The group id does not exist") {
			slogLogger.Info("There is no consumer group", "consumer_group", cfg.KafkaConfig.ConsumerGroup)
		} else {
			return err
		}
	}

	slogLogger.Info("Finished reset partitions")

	return nil
}

func ConfigureSaramaClient(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	lc fx.Lifecycle,
) (sarama.Client, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0

	client, err := sarama.NewClient(cfg.KafkaConfig.BootstrapServers, config)
	if err != nil {
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx0 context.Context) error {
			slogLogger.Info("Stopping kafka client")
			ce := client.Close()
			slogLogger.Info("Kafka client stopped", "err", ce)

			return nil
		},
	})

	return client, nil
}

func WaitForAllEventsProcessed(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	saramaClient sarama.Client,
	lc fx.Lifecycle,
) error {
	stoppingCtx, cancelFunc := context.WithCancel(context.Background())

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping waiter")
			cancelFunc()
			return nil
		},
	})

	du := cfg.CqrsConfig.CheckAreEventsProcessedInterval

	for {
		slogLogger.Info("Checking for the current offsets will be equal to the latest ones for all partitions")
		isEnd, errE := isEndOnAllPartitions(slogLogger, cfg, saramaClient)
		if errE != nil {
			slogLogger.Error("Error during checking isEndOnAllPartitions", "err", errE)
			return errE
		}
		if isEnd {
			slogLogger.Info("All the events was processed")
			cancelFunc()
		} else {
			slogLogger.Info("The current offsets still aren't equal to the latest ones")
		}

		if errors.Is(stoppingCtx.Err(), context.Canceled) {
			slogLogger.Info("Exiting from waiter")
			break
		} else {
			slogLogger.Info("Will wait before the next check iteration", "duration", du)
			time.Sleep(du)
		}
	}

	return nil
}

func getMaxOffsets(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	client sarama.Client,
) ([]int64, error) {
	maxOffsets := make([]int64, cfg.KafkaConfig.NumPartitions)

	for i := range cfg.KafkaConfig.NumPartitions {
		offset, err := client.GetOffset(cfg.KafkaConfig.Topic, i, sarama.OffsetNewest)
		if err != nil {
			return maxOffsets, err
		}
		maxOffsets[i] = offset
		slogLogger.Debug("Got max", "partition", i, "offset", offset)
	}
	return maxOffsets, nil
}

func isEndOnAllPartitions(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	client sarama.Client,
) (bool, error) {

	maxOffsets, err := getMaxOffsets(slogLogger, cfg, client)
	if err != nil {
		if errors.Is(err, sarama.ErrNotLeaderForPartition) {
			return false, nil
		}
		return false, err
	}

	// check are all 0
	allZero := true
	for p := range maxOffsets {
		if maxOffsets[p] != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return true, nil
	}

	offsetManager, err := sarama.NewOffsetManagerFromClient(cfg.KafkaConfig.ConsumerGroup, client)
	if err != nil {
		return false, err
	}
	defer offsetManager.Close()

	givenOffsets := make([]int64, cfg.KafkaConfig.NumPartitions)
	for i := range cfg.KafkaConfig.NumPartitions {
		partitionManager, err := offsetManager.ManagePartition(cfg.KafkaConfig.Topic, i)
		if err != nil {
			if errors.Is(err, sarama.ErrIncompleteResponse) {
				slogLogger.Info("Skipping partition", "partition", i)
				return false, nil
			}
			return false, err
		}
		defer partitionManager.AsyncClose() // faster

		offs, _ := partitionManager.NextOffset()
		if err != nil {
			return false, err
		}
		givenOffsets[i] = offs
		slogLogger.Debug("Got given", "partition", i, "offset", offs)
	}

	hasOneInitialized := false
	for i := range cfg.KafkaConfig.NumPartitions {
		if givenOffsets[i] == -1 {
			continue
		} else {
			hasOneInitialized = true

			if maxOffsets[i] != givenOffsets[i] {
				return false, nil
			}
		}
	}

	return hasOneInitialized, nil
}

const KeyKey = "key"
const ValueKey = "value"
const MetadataKey = "metadata"
const MetadataOffsetKey = "offset"
const MetadataPartitionKey = "partition"
const HeadersKey = "headers"

func Export(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	saramaClient sarama.Client,
) error {

	maxOffsets, err := getMaxOffsets(slogLogger, cfg, saramaClient)
	if err != nil {
		return err
	}

	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0

	newConsumer, err := sarama.NewConsumer(cfg.KafkaConfig.BootstrapServers, config)
	if err != nil {
		return err
	}
	defer newConsumer.Close()

	var writer io.Writer
	var f *os.File
	if cfg.CqrsConfig.ExportConfig.File == "stdout" {
		writer = os.Stdout
	} else {
		f, err = os.Create(cfg.CqrsConfig.ExportConfig.File)
		if err != nil {
			return err
		}
		writer = f
	}
	if f != nil {
		defer f.Close()
	}

	for i := range cfg.KafkaConfig.NumPartitions {
		partitionMaxOffset := maxOffsets[i]
		if partitionMaxOffset == 0 {
			slogLogger.Info("Skipping partition because absence of messages", "partition", i)
			continue
		}

		slogLogger.Info("Reading partition and it's max offset", "partition", i, "offset", partitionMaxOffset)

		partitionConsumer, err := newConsumer.ConsumePartition(cfg.KafkaConfig.Topic, i, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		defer partitionConsumer.Close()

		for kafkaMessage := range partitionConsumer.Messages() {
			jsonObj := gabs.New()
			_, err = jsonObj.SetP(kafkaMessage.Offset, MetadataKey+"."+MetadataOffsetKey)
			if err != nil {
				return err
			}
			_, err = jsonObj.SetP(kafkaMessage.Partition, MetadataKey+"."+MetadataPartitionKey)
			if err != nil {
				return err
			}

			parsedKey := string(kafkaMessage.Key)
			parsedValue, err := gabs.ParseJSON(kafkaMessage.Value)
			if err != nil {
				return err
			}

			for _, h := range kafkaMessage.Headers {
				parsedHeaderKey := string(h.Key)
				parsedHeaderValue := string(h.Value)

				_, err = jsonObj.Set(parsedHeaderValue, HeadersKey, parsedHeaderKey)
				if err != nil {
					return err
				}
			}

			_, err = jsonObj.Set(parsedKey, KeyKey)
			if err != nil {
				return err
			}

			_, err = jsonObj.Set(parsedValue, ValueKey)
			if err != nil {
				return err
			}

			_, err = fmt.Fprintln(writer, jsonObj.String())
			if err != nil {
				return err
			}

			if kafkaMessage.Offset >= partitionMaxOffset-1 {
				slogLogger.Info("Reached max offset, closing partitionConsumer", "partition", i)
				break
			}
		}

		slogLogger.Info("Finish reading partition", "partition", i)
	}
	return nil
}

func Import(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
) error {
	config := sarama.NewConfig()
	config.Version = sarama.V4_0_0_0
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cfg.KafkaConfig.BootstrapServers, config)
	if err != nil {
		return err
	}
	defer producer.Close()

	var reader io.Reader
	var f *os.File
	if cfg.CqrsConfig.ExportConfig.File == "stdin" {
		reader = os.Stdin
	} else {
		f, err = os.Open(cfg.CqrsConfig.ExportConfig.File)
		if err != nil {
			return err
		}
		reader = f
	}
	if f != nil {
		defer f.Close()
	}

	scanner := bufio.NewScanner(reader)
	i := 0
	for scanner.Scan() {
		i++
		str := scanner.Text()
		jsonObj, err := gabs.ParseJSON([]byte(str))
		if err != nil {
			return fmt.Errorf("Error on reading line %v: %w", i, err)
		}

		kd := jsonObj.S(KeyKey).Data()
		aKey, okk := kd.(string)
		if !okk {
			return fmt.Errorf("Error on parsing key on reading line %v from %v", i, kd)
		}

		aValue := jsonObj.S(ValueKey).Bytes()
		aPartition := jsonObj.S(MetadataKey, MetadataPartitionKey).String()
		partition, err := utils.ParseInt64(aPartition)
		if err != nil {
			return fmt.Errorf("Error on parsing partition on reading line %v: %w", i, err)
		}

		msg := &sarama.ProducerMessage{
			Topic:     cfg.KafkaConfig.Topic,
			Key:       sarama.ByteEncoder(aKey),
			Value:     sarama.ByteEncoder(aValue),
			Partition: int32(partition),
		}

		for headerKey, headerValue := range jsonObj.S(HeadersKey).ChildrenMap() {
			hd := headerValue.Data()
			hds, okhv := hd.(string)
			if !okhv {
				return fmt.Errorf("Error on parsing header value on reading line %v from %v for key %v", i, hd, headerKey)
			}
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   []byte(headerKey),
				Value: []byte(hds),
			})
		}

		_, _, err = producer.SendMessage(msg)
		if err != nil {
			return fmt.Errorf("Error on sending message from line %v: %w", i, err)
		}
	}

	slogLogger.Info("Import was successfully finished")
	return nil
}
