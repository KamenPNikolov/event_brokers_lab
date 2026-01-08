package main

import (
	"context"
	"event_brokers_lab/common"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

func mustEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func main() {
	rand.Seed(time.Now().UnixNano())

	broker := mustEnv("KAFKA_BROKER", "localhost:9092")
	topic := mustEnv("KAFKA_TOPIC", "events")
	dlqTopic := mustEnv("KAFKA_DLQ_TOPIC", "events.dlq")
	group := mustEnv("KAFKA_GROUP", "worker-group")

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		GroupID:  group,
		MinBytes: 1e3,
		MaxBytes: 10e6,
	})
	defer r.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    dlqTopic,
		Balancer: &kafka.Hash{},
	})
	defer dlqWriter.Close()

	log.Printf("Kafka worker consuming topic=%s group=%s", topic, group)

	for {
		m, err := r.FetchMessage(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		e, err := common.Decode(m.Value)
		if err != nil {
			log.Printf("bad message decode: %v", err)
			_ = r.CommitMessages(context.Background(), m)
			continue
		}

		e.Attempts++
		fail := rand.Intn(10) < 3

		if fail && e.Attempts < 5 {
			d := common.Backoff(e.Attempts - 1)
			log.Printf("FAIL id=%s attempt=%d -> backoff %s (no commit)", e.ID, e.Attempts, d)
			time.Sleep(d)
			// No commit => redelivery (at-least-once); beware "stuck partition" in real systems
			continue
		}

		if fail && e.Attempts >= 5 {
			log.Printf("DLQ id=%s attempt=%d -> write dlq and commit", e.ID, e.Attempts)
			b, _ := common.Encode(e)
			_ = dlqWriter.WriteMessages(context.Background(), kafka.Message{
				Key:   []byte(e.Key),
				Value: b,
				Time:  time.Now(),
			})
			_ = r.CommitMessages(context.Background(), m)
			continue
		}

		log.Printf("OK id=%s attempt=%d -> commit", e.ID, e.Attempts)
		_ = r.CommitMessages(context.Background(), m)
	}
}
