package main

import (
	"flag"
	"log"
	"strings"
	"time"

	kafka "github.com/shopify/sarama"
)

var (
	kafkaBrokers = flag.String("kafka", "localhost:9092", "Kafka brokers")
	topic        = flag.String("topic", "syslog.raw", "Subscribe to this Kafka topic")
	name         = flag.String("name", "test_group", "Consumer group name")
	timeout      = flag.Duration("timeout", 30*time.Second, "Read timeout")
)

func startKafkaConsumer(brokers []string, topic, consumerGroup string, timeout time.Duration) {
	client, err := kafka.NewClient("TestClient", brokers, nil)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Connected to Kafka broker", brokers)
	}

	defer client.Close()

	consumer, err := kafka.NewConsumer(client, topic, 0, consumerGroup, kafka.NewConsumerConfig())

	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Consumer ready: group=%s topic=%s", consumerGroup, topic)
	}

	defer consumer.Close()

	var n int

consumerLoop:
	for {
		select {
		case event := <-consumer.Events():
			if event.Err != nil {
				log.Fatal(event.Err)
			}
			n++
			log.Printf("Received message: %+v", event)
			log.Printf(string(event.Value))
		case <-time.After(timeout):
			log.Printf("Timed out after %s", timeout)
			break consumerLoop
		}
	}

	log.Printf("Received %d messages", n)
}

func main() {
	flag.Parse()
	brokers := strings.Split(*kafkaBrokers, ",")
	startKafkaConsumer(brokers, *topic, *name, *timeout)
}
