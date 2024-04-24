package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

var (
	host         = "localhost"
	port         = 5672
	user         = "test_user"
	password     = "testuser"
	vhost        = "test_vhost"
	exchange     = "test_exchange"
	exchangeType = "direct"
	queue        = "test_queue"
	routingKey   = "test_key"
	queueAck     = true
	defaultDelay = 0
)

func createConnection() (*amqp.Connection, error) {
	connString := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		user, password, host, port, vhost)
	return amqp.Dial(connString)
}

func createExchangeAndQueue(ch *amqp.Channel) error {
	err := ch.ExchangeDeclare(exchange, exchangeType,
		false, false, false, false, nil)
	if err != nil {
		return err
	}

	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		return err
	}

	return ch.QueueBind(queue, routingKey, exchange, false, nil)
}

func publishMessages(ch *amqp.Channel, numMessages int, delay time.Duration) error {
	for i := 0; i < numMessages; i++ {
		message := map[string]interface{}{
			"message_number": i + 1,
			"content":        fmt.Sprintf("Message number %d", i+1),
		}

		jsonBody, err := json.Marshal(message)
		if err != nil {
			return err
		}

		err = ch.Publish(exchange, routingKey,
			false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        jsonBody,
			})
		if err != nil {
			return err
		}
		fmt.Println("Published message: ", string(jsonBody))
		time.Sleep(delay)
	}
	return nil
}

func consumeMessages(ch *amqp.Channel, numMessages int, delay time.Duration) error {
	for i := 0; i < numMessages; i++ {
		msg, ok, err := ch.Get(queue, true)
		if err != nil {
			return err
		}
		if ok {
			fmt.Println("Received message:", string(msg.Body))
		} else {
			fmt.Println("No more messages in the queue.")
			break
		}
		time.Sleep(delay)
	}
	return nil
}

func main() {
	if len(os.Args) < 3 || (os.Args[1] != "publish" && os.Args[1] != "get") {
		fmt.Println("Usage:", os.Args[0],
			"<publish|get> <num_messages> [delay]")
		return
	}

	numMessages, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal("Error: num_messages must be an integer")
	}

	var delay time.Duration
	if len(os.Args) > 3 {
		delaySeconds, err := strconv.ParseFloat(os.Args[3], 64)
		if err != nil {
			log.Fatal("Error: delay must be a float")
		}
		delay = time.Duration(delaySeconds * float64(time.Second))
	} else {
		delay = time.Duration(defaultDelay) * time.Second
	}

	conn, err := createConnection()
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	err = createExchangeAndQueue(ch)
	if err != nil {
		log.Fatalf("Failed to create exchange and queue: %v", err)
	}

	switch os.Args[1] {
	case "publish":
		err = publishMessages(ch, numMessages, delay)
		if err != nil {
			log.Fatalf("Failed to publish messages: %v", err)
		}
	case "get":
		err = consumeMessages(ch, numMessages, delay)
		if err != nil {
			log.Fatalf("Failed to consume messages: %v", err)
		}
	}
}
