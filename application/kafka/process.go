package kafka

import (
	"fmt"
	"os"

	cKafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jinzhu/gorm"
	"github.com/pe-gomes/codepix-go/application/factory"
	appmodel "github.com/pe-gomes/codepix-go/application/model"
	"github.com/pe-gomes/codepix-go/application/usecases"
	"github.com/pe-gomes/codepix-go/domain/model"
)

type KafkaProcessor struct {
	Database     *gorm.DB
	Producer     *cKafka.Producer
	DeliveryChan chan cKafka.Event
}

func NewKafkaProcessor(database *gorm.DB, producer *cKafka.Producer, deliveryChan chan cKafka.Event) *KafkaProcessor {
	return &KafkaProcessor{
		Database:     database,
		Producer:     producer,
		DeliveryChan: deliveryChan,
	}
}

func (k *KafkaProcessor) Consume() {
	configMap := &cKafka.ConfigMap{
		"bootstrap.servers": os.Getenv("kafkaBootstrapServers"),
		"group.id":          os.Getenv("kafkaConsumerGroupId"),
		"auto.offset.reset": "earliest",
	}

	consumer, err := cKafka.NewConsumer(configMap)

	if err != nil {
		panic(err)
	}

	// TODO: kafka topics
	topics := []string{os.Getenv("kafkaTransactionTopic"), os.Getenv("kafkaTransactionConfirmationTopic")}
	consumer.SubscribeTopics(topics, nil)

	fmt.Println("Kafka consumer has been started")

	for {
		msg, err := consumer.ReadMessage(-1)
		if err == nil {
			k.processMessage(msg)
		}
	}
}

func (k *KafkaProcessor) processMessage(msg *cKafka.Message) {
	transactionsTopic := "transactions"
	transactionsConfirmationTopic := "transaction_confirmation"

	switch topic := *msg.TopicPartition.Topic; topic {
	case transactionsTopic:
		k.processMessage(msg)
	case transactionsConfirmationTopic:
		k.processTransactionConfirmation(msg)
	default:
		fmt.Println("Not a valid topic", string(msg.Value))
	}
}

func (k *KafkaProcessor) processTransaction(msg *cKafka.Message) error {
	transaction := appmodel.NewTransaction()

	err := transaction.ParseJson(msg.Value)
	if err != nil {
		return err
	}

	transactionUseCase := factory.TransactionUseCaseFactory(k.Database)

	createdTransaction, err := transactionUseCase.Register(
		transaction.AccountID,
		transaction.Amount,
		transaction.PixKeyTo,
		transaction.PixKeyKindTo,
		transaction.Description,
	)

	if err != nil {
		fmt.Println("error registering transaction", err)
		return err
	}

	topic := "bank" + createdTransaction.PixKeyTo.Account.Bank.Code
	transaction.ID = createdTransaction.ID
	transaction.Status = model.TransactionPending

	transactionJson, err := transaction.ToJson()
	if err != nil {
		return err
	}

	err = Publish(string(transactionJson), topic, k.Producer, k.DeliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaProcessor) processTransactionConfirmation(msg *cKafka.Message) error {
	transaction := appmodel.NewTransaction()

	err := transaction.ParseJson(msg.Value)
	if err != nil {
		return err
	}

	transactionUseCase := factory.TransactionUseCaseFactory(k.Database)

	if transaction.Status == model.TransactionConfirmed {
		err = k.confirmTransaction(transaction, transactionUseCase)
		if err != nil {
			return err
		}
	} else if transaction.Status == model.TransactionCompleted {
		_, err = transactionUseCase.Complete(transaction.ID)

		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (k *KafkaProcessor) confirmTransaction(transaction *appmodel.Transaction, transactionUseCase usecases.TransactionUseCase) error {
	confirmTransaction, err := transactionUseCase.Confirm(transaction.ID)

	if err != nil {
		return err
	}

	topic := "bank" + confirmTransaction.AccountFrom.Bank.Code
	transactionJson, err := transaction.ToJson()

	if err != nil {
		return err
	}

	err = Publish(string(transactionJson), topic, k.Producer, k.DeliveryChan)
	if err != nil {
		return err
	}
	return nil
}
