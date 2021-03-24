package main

import (
	"bufio"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/segmentio/kafka-go"
)

// the topic and broker address are initialized as constants
const (
	topic          = "my-kafka-topic2" //"message-log"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
)

func main() {
	// 1. connection parameters
	server := "5ZW6IG6TGSZI99G"
	user := "sa"
	password := ""
	port := 1433

	connString := sql_con_str(server, user, password, port)

	// 2. sql statement
	stmt := `SELECT 2+1
			SELECT 1/0`

	// 3. get error from sql query
	error := get_error(stmt, connString)

	// 4.1 create a new context
	ctx := context.Background()

	// 4.2 produce messages in a new go routine, since
	// both the produce and consume functions are
	// blocking
	go produce(ctx, error)
	consume(ctx)
}

func sql_con_str(server string, user string, password string, port int) string {
	flag.Parse()
	// connect to sql server
	connString := fmt.Sprintf("server=%s;user id=%s;password=%s;port=%d", server, user, password, port)
	return connString
}

func get_error(stmt string, connString string) string {
	// log connection event (success/fail ?)
	conn, err := sql.Open("mssql", connString)
	if err != nil {
		log.Fatal("Open connection failed:", err.Error())
	}
	defer conn.Close()

	// run a sql query assigning values in each columns to declared variables
	try_catch_err := fmt.Sprintf(
		`BEGIN TRY
			%s
		END TRY
		BEGIN CATCH
				TRUNCATE TABLE MASTER..ERROR_LOGS
				INSERT INTO MASTER..ERROR_LOGS(TIME_STAMP, QUERY, MESSAGE_, LINE, PROCEDURE_)
				SELECT
				GETDATE() AS TIME_STAMP,
				'%s' AS QUERY,
				ERROR_MESSAGE() AS [MESSAGE],
				ERROR_LINE() - 1 AS [LINE],
				ERROR_PROCEDURE() AS [PROCEDURE]
		END CATCH`, stmt, stmt)

	// fmt.Println(try_catch_err)
	conn.Query(try_catch_err)

	try_catch_err = fmt.Sprintf(`SELECT 
								ISNULL(TIME_STAMP,''), ISNULL(QUERY,''), ISNULL(MESSAGE_,'')
								, ISNULL(LINE - 1,''), ISNULL(PROCEDURE_ ,'')
								FROM MASTER..ERROR_LOGS`)
	rows, err := conn.Query(try_catch_err)

	var (
		timestamp string
		query     string
		message   string
		line      string
		procedure string
		notif     []string
	)

	for rows.Next() {
		err := rows.Scan(&timestamp, &query, &message, &line, &procedure)
		if err != nil {
			log.Fatal(err)
		}
		// print out values in each row on screen
		notif_ := fmt.Sprintf("%s | %s | %s | %s | %s\n", timestamp, query, message, line, procedure)
		notif = append(notif, notif_)
	}

	return notif[0]
}

func produce(ctx context.Context, error string) {

	fmt.Println("Input message's key ID: ")
	// Receive input message's key ID
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

	// Receive input message's value
	input2 := error

	// intialize the writer with the broker addresses, and the topic
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker1Address, broker2Address},
		Topic:   topic,
	})

	// each kafka message has a key and value. The key is used
	// to decide which partition (and consequently, which broker)
	// the message gets published on
	err := w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(input1),
		Value: []byte(input2),
	})
	if err != nil {
		panic("could not write message " + err.Error())
	}

	// log a confirmation once the message is written
	fmt.Println("writes:", input1)
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
		// assign the logger to the reader
	})

	// the `ReadMessage` method blocks until we receive the next event
	msg, err := r.ReadMessage(ctx)
	if err != nil {
		panic("could not read message " + err.Error())
	}
	// after receiving the message, log its value
	fmt.Println("received: ", string(msg.Value))
}
