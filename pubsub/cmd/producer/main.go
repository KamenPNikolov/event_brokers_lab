package main

import (
	"context"
	"event_brokers_lab/common"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func mustEnv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func topicFullName(projectID, topicID string) string {
	// Pub/Sub resource name format:
	// projects/{project}/topics/{topic}
	return "projects/" + projectID + "/topics/" + topicID
}

func ensureTopic(ctx context.Context, client *pubsub.Client, projectID, topicID string) error {
	name := topicFullName(projectID, topicID)

	_, err := client.TopicAdminClient.GetTopic(ctx, &pubsubpb.GetTopicRequest{Topic: name})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}

	_, err = client.TopicAdminClient.CreateTopic(ctx, &pubsubpb.Topic{Name: name})
	if status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

func main() {
	ctx := context.Background()

	// For emulator, set:
	// PUBSUB_EMULATOR_HOST=localhost:8085
	projectID := mustEnv("PUBSUB_PROJECT", "demo-project")
	topicID := mustEnv("PUBSUB_TOPIC", "events")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := ensureTopic(ctx, client, projectID, topicID); err != nil {
		log.Fatalf("ensure topic: %v", err)
	}
	log.Printf("Topic ready: %s", topicFullName(projectID, topicID))

	publisher := client.Publisher(topicID) // accepts ID or full name :contentReference[oaicite:3]{index=3}
	defer publisher.Stop()

	ticker := time.NewTicker(700 * time.Millisecond)
	defer ticker.Stop()

	i := 0
	for range ticker.C {
		key := []string{"user-1", "user-2", "user-3"}[i%3]
		e := common.NewEvent("UserUpdated", key, map[string]string{"i": time.Now().Format(time.RFC3339Nano)})
		b, err := common.Encode(e)
		if err != nil {
			log.Printf("encode event error: %v", err)
			continue
		}

		res := publisher.Publish(ctx, &pubsub.Message{
			Data: b,
			Attributes: map[string]string{
				"type": e.Type,
			},
			// OrderingKey: e.Key, // if you later enable ordering on topic/subscription
		})

		id, err := res.Get(ctx)
		if err != nil {
			log.Printf("publish error: %v", err)
			continue
		}

		log.Printf("published pubsub_id=%s key=%s event_id=%s", id, key, e.ID)
		i++
	}
}
