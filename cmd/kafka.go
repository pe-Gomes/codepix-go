/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"os"

	"github.com/pe-gomes/codepix-go/application/kafka"
	"github.com/pe-gomes/codepix-go/infra/db"
	"github.com/spf13/cobra"

	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

// kafkaCmd represents the kafka command
var kafkaCmd = &cobra.Command{
	Use:   "kafka",
	Short: "Start consuming transactions using Apache Kafka",
	Run: func(cmd *cobra.Command, args []string) {
		database := db.ConnectDB(os.Getenv("env"))
		deliveryChan := make(chan cKafka.Event)
		producer := kafka.NewKafkaProducer()

		// kafka.Publish("Hello, Kafka", "test", producer, deliveryChan)
		go kafka.DeliveryReport(deliveryChan)

		kafkaProcessor := kafka.NewKafkaProcessor(database, producer, deliveryChan)
		kafkaProcessor.Consume()
	},
}

func init() {
	rootCmd.AddCommand(kafkaCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// kafkaCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// kafkaCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
