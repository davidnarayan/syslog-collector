// Syslog to Kafka bridge

package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/davecheney/profile"
	"github.com/davidnarayan/go-flake"
	kafka "github.com/shopify/sarama"
)

//-----------------------------------------------------------------------------

// Command line flags
var (
	listen       = flag.String("listen", ":5140", "Listen on this address")
	kafkaBrokers = flag.String("kafka", "localhost:9092", "Kafka brokers (comma separated)")
	debug        = flag.Bool("debug", false, "Enable debug mode")

	// Profiling
	cpuprofile = flag.Bool("cpuprofile", false, "Enable CPU profiling")
	memprofile = flag.Bool("memprofile", false, "Enable memory profiling")
)

// Kafka topic for publishing events
const KafkaTopic = "events"

// MetricsInterval is the interval at which to write metrics
const MetricsInterval = 30 * time.Second

// BufSizeUdp is the amount of data to read from a UDP connection
const BufSizeUdp = 1024

// Syslog severities
const (
	SevEmergency = iota
	SevAlert
	SevCritical
	SevError
	SevWarning
	SevNotice
	SevInfo
	SevDebug
)

//-----------------------------------------------------------------------------
// Servers

// Message
type Message struct {
	Timestamp time.Time `json:"timestamp"`
	Id        string    `json:"id"`
	Host      string    `json:"host"`
	Severity  int       `json:"severity"`
	Payload   string    `json:"message"`
}

// Internal metrics
type stats struct {
	received    uint64
	dropped     uint64
	parsed      uint64
	sent        uint64
	connections int64
}

// server
type server struct {
	Addr   string
	Proto  string
	Inputs chan *kafka.MessageToSend
	Flake  *flake.Flake

	stats  stats
	statsc chan stats
}

// Handle an event message
func (s *server) handleMessage(buf []byte, raddr net.Addr) {
	atomic.AddUint64(&s.stats.received, 1)
	m, err := s.parseMessage(buf)

	if err != nil {
		if *debug {
			log.Println("DEBUG: Error parsing message: ", err)
			fmt.Printf("%v", string(buf))
		}

		atomic.AddUint64(&s.stats.dropped, 1)

		return
	}

	host, _, err := net.SplitHostPort(raddr.String())

	if err != nil {
		if *debug {
			log.Println("DEBUG: Error parsing host from remote addr: ", err)
		}
	}

	m.Host = host
	msg, err := s.encodeMessage(m)

	if err != nil {
		if *debug {
			log.Println("DEBUG: Error encoding message: ", err)
			fmt.Printf("%v", string(buf))
		}

		atomic.AddUint64(&s.stats.dropped, 1)

		return
	}

	atomic.AddUint64(&s.stats.parsed, 1)

	if *debug {
		log.Printf("DEBUG: Sending message to Kafka: %+v", msg)
	}

	s.Inputs <- msg
}

// parsePriority checks the message for a valid priority
func (s *server) parsePriority(buf []byte) error {
	// The '<' character is ASCII 60 and MUST occur at the start of the
	// message
	if buf[0] != 60 {
		return errors.New("priority is missing opening angle bracket)")
	}

	// The '>' character is ASCII 62 and MUST occur no more than 6 bytes from
	// the start of the message
	if buf[3] != 62 && buf[4] != 62 && buf[5] != 62 {
		return errors.New("priority is missing closing angle bracket")
	}

	return nil
}

// parseMessage parses the raw message and turns it into a Message struct
func (s *server) parseMessage(buf []byte) (*Message, error) {
	// Messages should have a syslog priority
	err := s.parsePriority(buf)

	if err != nil {
		return nil, err
	}

	id := s.Flake.NextId()
	payload := string(bytes.TrimRight(buf, "\x00"))

	m := &Message{
		Timestamp: time.Now(),
		Id:        fmt.Sprint(s.Proto, "-", id.String()),
		Severity:  SevDebug,
		Payload:   payload,
	}

	return m, nil
}

// Encode message as a JSON object
func (s *server) encodeMessage(m *Message) (*kafka.MessageToSend, error) {
	b, err := json.Marshal(m)

	if err != nil {
		return nil, fmt.Errorf("json.Marshal error: %s", err.Error())
	}

	msg := &kafka.MessageToSend{
		Topic: KafkaTopic,
		Key:   nil,
		Value: kafka.StringEncoder(string(b)),
	}

	return msg, nil
}

// logMetrics periodically logs metrics about messages handled
func (s *server) logMetrics(interval time.Duration) {
	c := time.Tick(interval)

	go func() {
		for {
			select {
			case <-c:
				log.Printf("%s stats: %+v", s.Proto, s.stats)
				/*
					atomic.StoreUint64(&s.stats.received, 0)
					atomic.StoreUint64(&s.stats.dropped, 0)
					atomic.StoreUint64(&s.stats.parsed, 0)
					atomic.StoreUint64(&s.stats.sent, 0)
				*/
			}
		}
	}()
}

func (s *server) initialize(proto, addr string) error {
	s.Proto = proto
	s.Addr = addr
	s.Inputs = make(chan *kafka.MessageToSend)
	f, err := flake.New()

	if err != nil {
		return fmt.Errorf("unable to create Id generator: %s", err.Error())
	}

	s.Flake = f

	return nil
}

// UdpServer is a server that accepts messages on a UDP socket
type UdpServer struct {
	server
}

// NewUdpServer creates a new UdpServer instance
func NewUdpServer(addr string) (*UdpServer, error) {
	s := &UdpServer{}
	err := s.initialize("udp", addr)

	if err != nil {
		return nil, err
	}

	return s, nil
}

// Listen creates a UDP listener
func (s *UdpServer) Listen() error {
	l, err := net.ResolveUDPAddr("udp", s.Addr)

	if err != nil {
		return err
	}

	conn, err := net.ListenUDP("udp", l)

	if err != nil {
		return err
	}

	log.Println("Listening on UDP", s.Addr)

	for {
		b := make([]byte, BufSizeUdp)
		n, raddr, err := conn.ReadFromUDP(b)

		// TODO: handle error
		if err != nil {
			continue
		}

		if *debug {
			log.Printf("DEBUG: Received UDP message: bytes=%d client=%s", n, raddr)
		}

		go s.handleMessage(b, raddr)
	}
}

// TcpServer is a server that accepts messages on a TCP socket
type TcpServer struct {
	server
}

// NewTcpServer creates a new TcpServer instance
func NewTcpServer(addr string) (*TcpServer, error) {
	s := &TcpServer{}
	err := s.initialize("tcp", addr)

	if err != nil {
		return nil, err
	}

	return s, nil
}

// Listen creates a TCP listener
func (s *TcpServer) Listen() error {
	l, err := net.Listen("tcp", s.Addr)

	if err != nil {
		return err
	}

	defer l.Close()
	log.Printf("Listening on TCP %s\n", l.Addr())

	for {
		conn, err := l.Accept()

		if err != nil {

			continue
		}

		go s.handleConnection(conn)
	}
}

// handleConnection handles a single TCP client connection
func (s *TcpServer) handleConnection(conn net.Conn) {
	atomic.AddInt64(&s.stats.connections, 1)
	defer atomic.AddInt64(&s.stats.connections, -1)
	defer conn.Close()

	r := bufio.NewReader(conn)

	for {
		line, err := r.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				break
			} else {

			}
		}

		if *debug {
			log.Printf("DEBUG: Received TCP message: bytes=%d client=%s",
				len(line), conn.RemoteAddr())
		}

		s.handleMessage(line, conn.RemoteAddr())
	}
}

// startProducer starts a Kafka Producer for publishing messages
func (s *server) startProducer(brokers []string) error {
	client, err := kafka.NewClient("logger", brokers, kafka.NewClientConfig())

	if err != nil {
		return err
	}

	defer client.Close()

	log.Println("Connected to Kafka brokers:", brokers)

	producer, err := kafka.NewProducer(client, nil)

	if err != nil {
		return err
	}

	defer producer.Close()

	for {
		select {
		case msg := <-s.Inputs:
			producer.Input() <- msg
			atomic.AddUint64(&s.stats.sent, 1)

			if *debug {
				log.Printf("message queued: %+v", msg)
			}

		case err := <-producer.Errors():
			if err != nil {
				log.Println("Kafka producer error:", err)
			}
		}
	}
}

//-----------------------------------------------------------------------------

// Kafka Brokers
func getBrokers() []string {
	brokers := strings.Split(*kafkaBrokers, ",")

	// Override with environment variable
	env_brokers := os.Getenv("KAFKA_BROKERS")

	if env_brokers != "" {
		if *debug {
			log.Println("Setting Kafka brokers from environment:", env_brokers)
		}

		brokers = strings.Split(env_brokers, ",")
	}

	return brokers
}

func main() {
	flag.Parse()
	log.Println("Starting syslog collector")
	var inputs []chan *kafka.MessageToSend

	// Enable Profiling
	if *cpuprofile {
		defer profile.Start(profile.CPUProfile).Stop()
	} else if *memprofile {
		defer profile.Start(profile.MemProfile).Stop()
	}

	env_debug := os.Getenv("SYSLOG_COLLECTOR_DEBUG")

	if env_debug == "1" {
		*debug = true
	}

	// Kafka brokers
	brokers := getBrokers()

	// TCP Listener
	go func() {
		s, err := NewTcpServer(*listen)

		if err != nil {
			log.Fatal("Unable to create TCP server", err)
		}

		inputs = append(inputs, s.Inputs)

		go func() {
			err := s.startProducer(brokers)

			if err != nil {
				log.Fatal(err)
			}
		}()

		s.logMetrics(MetricsInterval)
		log.Fatal(s.Listen())
	}()

	// UDP Listener
	go func() {
		s, err := NewUdpServer(*listen)

		if err != nil {
			log.Fatal("Unable to create UDP server", err)
		}

		inputs = append(inputs, s.Inputs)

		go func() {
			err := s.startProducer(brokers)

			if err != nil {
				log.Fatal(err)
			}
		}()

		s.logMetrics(MetricsInterval)
		log.Fatal(s.Listen())
	}()

	// Wait forever
	select {}
}
