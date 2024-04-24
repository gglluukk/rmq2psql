package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/streadway/amqp"
)

var (
	host     = "localhost"
	rmqPort  = 5672
	sqlPort  = 5432
	user     = "test_user"
	password = "testuser"
	vhost    = "test_vhost"
	database = vhost
	queue    = "test_queue"
	table    = queue
)

type Message struct {
	MessageNumber int    `json:"message_number"`
	Content       string `json:"content"`
}

func reader(messageCh chan Message, wg *sync.WaitGroup, maxReads int) {
	defer close(messageCh)
	defer wg.Done()

	connString := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		user, password, host, rmqPort, vhost)
	rmqConn, err := amqp.Dial(connString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rmqConn.Close()

	rmqCh, err := rmqConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer rmqCh.Close()

	_, err = rmqCh.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		log.Fatalf("Failed to create queue: %v", err)
	}

	var message Message
	for i := 0; i < maxReads; i++ {
		msg, ok, err := rmqCh.Get(queue, true)
		if err != nil {
			log.Printf("Error while getting message: %v", err)
			continue
		}
		if ok {
			err := json.Unmarshal(msg.Body, &message)
			if err != nil {
				log.Printf("Error parsing message: %v", err)
				continue
			}
			messageCh <- message
		} else {
			log.Println("No more messages in the queue.")
			break
		}
	}
}

func writer(messageCh chan Message, wg *sync.WaitGroup, maxBulks int) {
	defer wg.Done()

	connString := fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		user, password, host, sqlPort, database)
	pgxConn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		log.Fatalf("Failed to connect to PostgreSQL: %v", err)
	}
	defer pgxConn.Close(context.Background())

	messages := make([]Message, 0)
	for message := range messageCh {
		messages = append(messages, message)
		if len(messages) == maxBulks {
			if err := bulkInsert(pgxConn, &messages); err != nil {
				log.Printf("Error in bulk insert: %v", err)
			}
			messages = messages[:0]
		}
	}

	if len(messages) > 0 {
		if err := bulkInsert(pgxConn, &messages); err != nil {
			log.Printf("Error in bulk insert: %v", err)
		}
	}
}

func bulkInsert(pgxConn *pgx.Conn, messages *[]Message) error {
	var values string
	var params []interface{}

	for i := range *messages {
		values += fmt.Sprintf("($%d, $%d),", i*2+1, i*2+2)
		params = append(params, (*messages)[i].Content,
			(*messages)[i].MessageNumber)
	}
	values = values[:len(values)-1]

	query := fmt.Sprintf("INSERT INTO %s (content, message_number) "+
		"VALUES %s", table, values)
	_, err := pgxConn.Exec(context.Background(), query, params...)

	return err
}

func main() {
	maxBulks := flag.Int("max-bulks", 100, "max bulks for batch insert")
	maxReads := flag.Int("max-reads", 1000, "max messages to read")
	flag.Parse()

	if len(os.Args) < 2 {
		fmt.Println("Usage:", os.Args[0],
			"--max-bulks <max_bulks> --max-reads <num_messages>")
		return
	}

	var wg sync.WaitGroup
	messageCh := make(chan Message)

	wg.Add(2)

	go reader(messageCh, &wg, *maxReads)

	go writer(messageCh, &wg, *maxBulks)

	wg.Wait()
}
