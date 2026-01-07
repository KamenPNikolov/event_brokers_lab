package main

import (
	"event_brokers_lab/common"
	"log"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func mustEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	url := mustEnv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	exchange := mustEnv("RABBIT_EXCHANGE", "events.x")
	routing := mustEnv("RABBIT_ROUTING", "user.updated")

	conn, err := amqp.Dial(url)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}

	i := 0
	for {
		key := []string{"user.1", "user.2", "user.3"}[i%3]
		e := common.NewEvent("UserUpdated", key, map[string]string{"i": time.Now().Format(time.RFC3339Nano)})
		b, err := common.Encode(e)
		if err != nil {
			log.Printf("encode error: %v", err)
			continue
		}

		err = ch.Publish(exchange, routing, false, false, amqp.Publishing{
			ContentType: "application/json",
			Body:        b,
			MessageId:   e.ID,
			Timestamp:   time.Now(),
		})
		if err != nil {
			log.Printf("publish error: %v", err)
		} else {
			log.Printf("published id=%s routing=%s", e.ID, routing)
		}
		i++
		time.Sleep(700 * time.Millisecond)
	}
}
