package main

import (
	"context"
	"event_brokers_lab/common"
	"log"
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
	broker := mustEnv("KAFKA_BROKER", "localhost:9092")
	topic := mustEnv("KAFKA_TOPIC", "events")

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{broker},
		Topic:    topic,
		Balancer: &kafka.Hash{}, // key-based partitioning
	})
	defer w.Close()

	i := 0
	for {
		key := []string{"user-1", "user-2", "user-3"}[i%3]
		e := common.NewEvent("UserUpdated", key, map[string]string{"i": time.Now().Format(time.RFC3339Nano)})
		b, err := common.Encode(e)
		if err != nil {
			log.Printf("encode error: %v", err)
			continue
		}

		err = w.WriteMessages(context.Background(), kafka.Message{
			Key:   []byte(e.Key),
			Value: b,
			Time:  time.Now(),
		})
		if err != nil {
			log.Printf("write error: %v", err)
		} else {
			log.Printf("published id=%s key=%s", e.ID, e.Key)
		}

		i++
		time.Sleep(700 * time.Millisecond)
	}
}
