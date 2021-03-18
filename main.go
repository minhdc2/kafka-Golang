package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"
)

// the topic and broker address are initialized as constants
const (
	topic          = "my-kafka-topic2" //"message-log"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
)

func main() {
	// create a new context
	ctx := context.Background()
	// produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking
	go produce(ctx)
	consume(ctx)
}

func produce(ctx context.Context) {

	fmt.Println("Input message's key ID: ")
	// Take input message's key ID from user
	scanner := bufio.NewScanner(os.Stdin)
	var input1 string
	j := 0

	for scanner.Scan() {
		j++
		input1 = scanner.Text()
		if j > 0 {
			break
		}
	}

	i := input1

	fmt.Println("Input message's value: ")
	// scanner := bufio.NewScanner(os.Stdin)
	var input2 string
	k := 0

	for scanner.Scan() {
		k++
		input2 = scanner.Text()
		if k > 0 {
			break
		}
	}

	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address},
		Topic:   topic,
	})

	// each kafka message has a key and value. The key is used
	// to decide which partition (and consequently, which broker)
	// the message gets published on
	err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(i),
		Value: []byte(input2),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}

	// log a confirmation once the message is written
	fmt.Println("writes:", i)
}

func consume(ctx context.Context) {
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker1Address, broker2Address},
		Topic:   topic,
		GroupID: "my-group",
		// this will start consuming messages from the earliest available
		StartOffset: kafka.FirstOffset,
		// if you set it to `kafka.LastOffset` it will only consume new messages
	})

	// the `ReadMessage` method blocks until we receive the next event
	msg, err := r.ReadMessage(ctx)
	if err != nil {
		panic("could not read message " + err.Error())
	}
	// after receiving the message, log its value
	fmt.Println("received: ", string(msg.Value))
}
