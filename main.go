package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	engineGrpc "kafka-engine-client/engineGrpc"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"google.golang.org/grpc"
)

func main() {

	// create the consumer and listen for new order messages
	consumer := createConsumer()

	// // create the producer of trade messages
	// producer := createProducer()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	// create a signal channel to know when we are done
	done := make(chan bool)

	conn, err := grpc.Dial("localhost:9093", grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// start processing orders
	go func() {
		for {
			fmt.Println("\nRunnning")
			select {
			case err := <-consumer.Errors():
				fmt.Println("consumer.Errors()", err)
			case ntf := <-consumer.Notifications():
				fmt.Printf("Rebalanced: %+v\n", ntf)
			case msg := <-consumer.Messages():
				// msgCount++
				fmt.Printf("Receiveing message => Key: %s, Value: %s\n", string(msg.Key), string(msg.Value))

				engineClient := engineGrpc.NewEngineClient(conn)
				fmt.Println(engineClient)
				orderString := string(msg.Value)
				order := &struct {
					Amount string
					Price  string
					ID     string
					Type   string
				}{}
				// decode the message
				// fmt.Println("Orderstring =: ", orderString)
				err := json.Unmarshal([]byte(orderString), order)
				if err != nil {
					fmt.Println("JSON Parse Error =: ", err)
					panic(err)
				}

				rr := &engineGrpc.Order{Type: engineGrpc.Side(engineGrpc.Side_value[order.Type]), Amount: order.Amount, Price: order.Price, ID: order.ID}
				resp, err := engineClient.Process(context.Background(), rr)
				if err != nil {
					fmt.Println("eerrr")
					panic(err)
				}
				fmt.Println("resp", resp)
				// // send trades to message queue
				// for _, trade := range trades {
				// 	rawTrade := trade.ToJSON()
				// 	fmt.Println("Raw Trade", string(rawTrade))
				// 	producer.Input() <- &sarama.ProducerMessage{
				// 		Topic: "trades",
				// 		Value: sarama.ByteEncoder(rawTrade),
				// 	}
				// }
				consumer.MarkOffset(msg, "")
			case <-signals:
				fmt.Println("Interrupt is detected")
				done <- true
			}
		}
	}()

	// wait until we are done
	<-done

	fmt.Println("Complete")
}

//
// Create the consumer
//

func createConsumer() *cluster.Consumer { //sarama.PartitionConsumer {
	// define our configuration to the cluster
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	config.Consumer.Offsets.CommitInterval = 1 * time.Second

	// create the consumer
	consumer, err := cluster.NewConsumer([]string{"localhost:9092"}, "myconsumer", []string{"test"}, config)
	if err != nil {
		log.Fatal("Unable to connect consumer to kafka cluster")
	}

	// go handleErrors(consumer)
	// go handleNotifications(consumer)
	return consumer
}

// func handleErrors(consumer *cluster.Consumer) {
// 	for err := range consumer.Errors() {
// 		log.Printf("Error: %s\n", err.Error())
// 	}
// }

// func handleNotifications(consumer *cluster.Consumer) {
// 	for ntf := range consumer.Notifications() {
// 		log.Printf("Rebalanced: %+v\n", ntf)
// 	}
// }

//
// Create the producer
//

func createProducer() sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		log.Fatal("Unable to connect producer to kafka server")
	}
	return producer
}
