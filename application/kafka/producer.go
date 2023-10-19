package kafka

import (
	"fmt"
	"os"

	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func NewKafkaProducer() *cKafka.Producer {
	configMap := &cKafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
	}

	p, err := cKafka.NewProducer(configMap)

	if err != nil {
		panic(err)
	}

	return p
}

func Publish(msg string, topic string, producer *cKafka.Producer, deliveryChannel chan cKafka.Event) error {
	message := &cKafka.Message{
		TopicPartition: cKafka.TopicPartition{Topic: &topic, Partition: cKafka.PartitionAny},
		Value:          []byte(msg),
	}

	err := producer.Produce(message, deliveryChannel)
	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChannel chan cKafka.Event) {
	for e := range deliveryChannel {

		switch ev := e.(type) {
		case *cKafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Delivery failed", ev.TopicPartition)
			} else {
				fmt.Println("Delivered message to: ", ev.TopicPartition)
			}
		}
	}
}
