package main

import (
	"context"
	"event_brokers_lab/common"
	"log"
	"math/rand"
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
	return "projects/" + projectID + "/topics/" + topicID
}

func subFullName(projectID, subID string) string {
	// projects/{project}/subscriptions/{sub}
	return "projects/" + projectID + "/subscriptions/" + subID
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

func ensureSubscription(ctx context.Context, client *pubsub.Client, projectID, topicID, subID string) error {
	subName := subFullName(projectID, subID)

	_, err := client.SubscriptionAdminClient.GetSubscription(ctx, &pubsubpb.GetSubscriptionRequest{Subscription: subName})
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.NotFound {
		return err
	}

	// Minimal subscription config for emulator use.
	_, err = client.SubscriptionAdminClient.CreateSubscription(ctx, &pubsubpb.Subscription{
		Name:  subName,
		Topic: topicFullName(projectID, topicID),
		// AckDeadlineSeconds can be set here, but v2 client also manages extensions automatically. :contentReference[oaicite:4]{index=4}
		AckDeadlineSeconds: 20,
	})
	return err
}

func main() {
	ctx := context.Background()

	projectID := mustEnv("PUBSUB_PROJECT", "demo-project")
	topicID := mustEnv("PUBSUB_TOPIC", "events")
	subID := mustEnv("PUBSUB_SUB", "worker-sub")

	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	if err := ensureTopic(ctx, client, projectID, topicID); err != nil {
		log.Fatalf("ensure topic: %v", err)
	}
	if err := ensureSubscription(ctx, client, projectID, topicID, subID); err != nil {
		log.Fatalf("ensure subscription: %v", err)
	}
	log.Printf("Subscription ready: %s", subFullName(projectID, subID))

	sub := client.Subscriber(subID) // accepts ID or full name :contentReference[oaicite:5]{index=5}

	// v2 ReceiveSettings live on Subscriber.ReceiveSettings
	sub.ReceiveSettings.MaxOutstandingMessages = 50
	sub.ReceiveSettings.NumGoroutines = 8

	rand.Seed(time.Now().UnixNano())

	log.Printf("Pub/Sub v2 worker receiving on %s...", subID)
	err = sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		e, err := common.Decode(m.Data)
		if err != nil {
			log.Printf("bad message: %v", err)
			m.Ack()
			return
		}

		e.Attempts++
		fail := rand.Intn(10) < 3

		if fail && e.Attempts < 5 {
			log.Printf("FAIL id=%s key=%s attempt=%d -> NACK (redelivery)", e.ID, e.Key, e.Attempts)
			m.Nack()
			return
		}

		if fail && e.Attempts >= 5 {
			// In real GCP (not emulator): configure DeadLetterPolicy on subscription for automatic DLQ routing.
			log.Printf("DLQ threshold (emulator) id=%s key=%s attempt=%d -> ACK(drop)", e.ID, e.Key, e.Attempts)
			m.Ack()
			return
		}

		log.Printf("OK id=%s key=%s attempt=%d -> ACK", e.ID, e.Key, e.Attempts)
		m.Ack()
	})
	if err != nil && status.Code(err) != codes.Canceled {
		log.Fatal(err)
	}
}
