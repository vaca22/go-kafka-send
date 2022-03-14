package main

import (
	"fmt"
	"github.com/Shopify/sarama"
)

func main() {
	fmt.Println("producer_test\n")
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Version = sarama.V0_11_0_2

	producer, err := sarama.NewAsyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Printf("producer_test create producer error :%s\n", err.Error())
		return
	}
	defer producer.AsyncClose()
	// send message
	msg := &sarama.ProducerMessage{
		Topic: "kafka_go_test",
		Key:   sarama.StringEncoder("go_test"),
	}
	fmt.Println("producer_test\n")
	value := "this is message"
	for {
		fmt.Println("producer_test\n")
		fmt.Scanln(&value)
		fmt.Println("producer_test\n")
		msg.Value = sarama.ByteEncoder(value)
		fmt.Printf("send [%s]\n", value)
		// send to chain
		producer.Input() <- msg
		select {
		case suc := <-producer.Successes():
			fmt.Printf("%s offset: %d \n", msg.Timestamp.String(), suc.Offset)
		case fail := <-producer.Errors():
			fmt.Println("err: %s\n", fail.Err.Error())
		}
	}
}
