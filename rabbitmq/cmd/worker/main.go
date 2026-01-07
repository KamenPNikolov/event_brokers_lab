package main

import (
	"event_brokers_lab/common"
	"log"
	"math/rand"
	"os"
	"strconv"
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
	rand.Seed(time.Now().UnixNano())

	url := mustEnv("RABBIT_URL", "amqp://guest:guest@localhost:5672/")
	exchange := mustEnv("RABBIT_EXCHANGE", "events.x")

	queueMain := mustEnv("RABBIT_QUEUE", "worker.q")
	queueRetry := queueMain + ".retry"
	queueDLQ := queueMain + ".dlq"

	prefetch, _ := strconv.Atoi(mustEnv("RABBIT_PREFETCH", "20"))

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

	// Main exchange
	if err := ch.ExchangeDeclare(exchange, "topic", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}

	// Dead-letter exchange
	dlx := exchange + ".dlx"
	if err := ch.ExchangeDeclare(dlx, "direct", true, false, false, false, nil); err != nil {
		log.Fatal(err)
	}

	// Main queue
	_, err = ch.QueueDeclare(queueMain, true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Retry queue: TTL then route back to main exchange/routing key
	_, err = ch.QueueDeclare(queueRetry, true, false, false, false, amqp.Table{
		"x-message-ttl":             int32(2000), // 2s delay
		"x-dead-letter-exchange":    exchange,
		"x-dead-letter-routing-key": "user.updated",
	})
	if err != nil {
		log.Fatal(err)
	}

	// DLQ
	_, err = ch.QueueDeclare(queueDLQ, true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	// Bindings
	if err := ch.QueueBind(queueMain, "user.updated", exchange, false, nil); err != nil {
		log.Fatal(err)
	}
	if err := ch.QueueBind(queueRetry, "retry", dlx, false, nil); err != nil {
		log.Fatal(err)
	}
	if err := ch.QueueBind(queueDLQ, "dlq", dlx, false, nil); err != nil {
		log.Fatal(err)
	}

	// Backpressure
	if err := ch.Qos(prefetch, 0, false); err != nil {
		log.Fatal(err)
	}

	msgs, err := ch.Consume(queueMain, "", false, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Rabbit worker consuming queue=%s prefetch=%d", queueMain, prefetch)

	for d := range msgs {
		e, err := common.Decode(d.Body)
		if err != nil {
			log.Printf("bad msg: %v", err)
			_ = d.Ack(false)
			continue
		}

		e.Attempts++
		fail := rand.Intn(10) < 3

		if fail && e.Attempts < 5 {
			log.Printf("FAIL id=%s attempt=%d -> publish to retry queue (TTL)", e.ID, e.Attempts)
			b, _ := common.Encode(e)
			_ = ch.Publish(dlx, "retry", false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        b,
				MessageId:   e.ID,
				Timestamp:   time.Now(),
			})
			_ = d.Ack(false)
			continue
		}

		if fail && e.Attempts >= 5 {
			log.Printf("DLQ id=%s attempt=%d -> publish to dlq", e.ID, e.Attempts)
			b, _ := common.Encode(e)
			_ = ch.Publish(dlx, "dlq", false, false, amqp.Publishing{
				ContentType: "application/json",
				Body:        b,
				MessageId:   e.ID,
				Timestamp:   time.Now(),
			})
			_ = d.Ack(false)
			continue
		}

		log.Printf("OK id=%s attempt=%d -> ACK", e.ID, e.Attempts)
		_ = d.Ack(false)
	}
}
