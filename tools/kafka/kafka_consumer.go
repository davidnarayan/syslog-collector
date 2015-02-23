package main

import (
	"flag"
	"log"
	"regexp"
	"strings"
	"time"

	kafka "github.com/shopify/sarama"
)

var (
	kafkaBrokers = flag.String("kafka", "localhost:9092", "Kafka brokers")
	topic        = flag.String("topic", "syslog.raw", "Subscribe to this Kafka topic")
	name         = flag.String("name", "test_group", "Consumer group name")
	timeout      = flag.Duration("timeout", 30*time.Second, "Read timeout")
	regex        = flag.String("regex", "", "Filter messages using a regular expression")
	debug        = flag.Bool("debug", false, "Enable debug messages")
)

func startKafkaConsumer(brokers []string, topic, consumerGroup string, timeout time.Duration, re *regexp.Regexp) {
	client, err := kafka.NewClient("TestClient", brokers, nil)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Println("Connected to Kafka broker", brokers)
	}

	defer client.Close()

	master, err := kafka.NewConsumer(client, nil)

	if err != nil {
		log.Fatal(err)
	} else {
		log.Printf("Master consumer ready: group=%s topic=%s", consumerGroup,
			topic)
	}

	consumer, err := master.ConsumePartition(topic, 0, nil)

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

			if *debug {
				log.Printf("Received message: %+v", event)
			}

			s := string(event.Value)

			if re != nil && re.MatchString(s) {
				log.Println(s)
			} else {
				log.Println(s)
			}

			/*
				if strings.Contains(s, "DB2") {
					log.Println(s)
				}
			*/
			//log.Printf(string(event.Value))
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
	var re *regexp.Regexp

	if *regex != "" {
		var err error
		re, err = regexp.Compile(*regex)

		if err != nil {
			log.Printf("Unable to compile regex: %s", *regex)
		}
	}

	startKafkaConsumer(brokers, *topic, *name, *timeout, re)
}
