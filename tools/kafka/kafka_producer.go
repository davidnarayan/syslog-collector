package main

import (
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	kafka "github.com/shopify/sarama"
)

var (
	kafkaBrokers = flag.String("kafka", "localhost:9092", "Kafka brokers")
	msgCount     = flag.Int("n", 1, "Number of messages to send")
)

var (
	topic         = "syslog.raw"
	consumerGroup = "test_group"
	timeout       = 30 * time.Second
)

func startKafkaProducer(brokers []string) {
	client, err := kafka.NewClient("TestClient", brokers, nil)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Connected to Kafka broker", brokers)
	}

	defer client.Close()

	producer, err := kafka.NewProducer(client, nil)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Producer ready")
	}

	defer producer.Close()

	for i := 1; i <= *msgCount; i++ {
		producer.Input() <- &kafka.MessageToSend{
			Topic: "syslog.raw",
			Key:   nil,
			Value: kafka.StringEncoder(fmt.Sprintf("Message %d", i)),
		}
	}

	log.Printf("Sent %d messages", *msgCount)
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	startKafkaProducer(brokers)
}
