package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const topic_name = "topic1"

func sub(client mqtt.Client, id string) {
	topic := topic_name
	//topic := fmt.Sprintf("t%s", id)
	//println("Subscribing to", topic)

	token := client.Subscribe(topic, 1, nil)
	token.Wait()
	// if token.Error() != nil {
	//  println("Retrying connection for client", id)
	//  time.Sleep(1 * time.Second)
	//  sub(client, id)
	// }

}

func NewTlsConfig() *tls.Config {
	certpool := x509.NewCertPool()
	ca, err := os.ReadFile("local/client.csr")
	if err != nil {
		log.Fatalln(err.Error())
	}

	certpool.AppendCertsFromPEM(ca)
	return &tls.Config{
		RootCAs:            certpool,
		InsecureSkipVerify: true,
	}

}

func publish(client mqtt.Client, id string) {
	num := 1000000
	for i := 0; i < num; i++ {
		text := fmt.Sprintf("Device: %s Message: %d", id, i)
		token := client.Publish(topic_name, 0, false, text)
		token.Wait()
		time.Sleep(500 * time.Millisecond)
	}

}

func create_client(id string) mqtt.Client {
	opts := mqtt.NewClientOptions()
	// tlsConfig := NewTlsConfig()
	// opts.SetTLSConfig(tlsConfig)
	// opts.AddBroker("tcp://localhost:8023")
	opts.AddBroker("tcp://localhost:1883")
	// opts.AddBroker("tls://mqtt.hono.dtpm.poc.systematic-synergy.io:8883")
	opts.SetClientID(id)
	opts.SetUsername("emqx")
	// opts.SetUsername(id + "@load_test")
	opts.SetPassword("public")
	opts.SetCleanSession(true)
	opts.SetKeepAlive(2)
	opts.SetPingTimeout(2)
	opts.SetAutoReconnect(true)
	opts.SetMaxReconnectInterval(2)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		fmt.Println("Client_id:", id, "Connected")

	})

	opts.SetConnectionLostHandler(func(client mqtt.Client, err error) {
		fmt.Println("Client_id:", id, "Connection lost:", err)
	})

	opts.SetReconnectingHandler(func(client mqtt.Client, opts *mqtt.ClientOptions) {
		println("Reconnecting")

	})

	client := mqtt.NewClient(opts)
	token := client.Connect()
	token.Wait()

	if token.Error() != nil {
		println(token.Error().Error())
	}
	return client

}

func device_runner(client mqtt.Client, wg *sync.WaitGroup, id string) {
	fmt.Println("Publishing for client", id)
	publish(client, id)
	client.Disconnect(250)
	fmt.Println("Disconnected client", id)
	defer wg.Done()

}

func establish_connection(connections *[]mqtt.Client, wg *sync.WaitGroup, id string) {
	client := create_client(id)
	sub(client, id)
	*connections = append(*connections, client)
	defer wg.Done()

}

func main() {
	mqtt.ERROR = log.New(os.Stdout, "[ERROR] ", 0)
	mqtt.CRITICAL = log.New(os.Stdout, "[CRIT] ", 0)
	const num_connections = 50
	var connections []mqtt.Client
	var ids []string
	var wg sync.WaitGroup

	// Establish connections
	for i := 1; i <= num_connections; i++ {
		wg.Add(1)
		client_id := "D" + fmt.Sprintf("%d", i)
		go establish_connection(&connections, &wg, client_id)
		ids = append(ids, client_id)
		time.Sleep(1 * time.Millisecond)
	}

	wg.Wait()
	println("All connections established")

	// Iterate over connections and publish

	for idx, client := range connections {
		wg.Add(1)
		go device_runner(client, &wg, ids[idx])
	}

	wg.Wait()

	println("Finished")
	time.Sleep(100 * time.Second)

}
