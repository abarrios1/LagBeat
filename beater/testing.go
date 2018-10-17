package beater

import (
	"strconv"
	"fmt"
	"time"
	"os"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/abarrios1/testing/config"
	"github.com/wvanbergen/kazoo-go"
	"github.com/Shopify/sarama"
)
var (
	sClient sarama.Client
	zClient *kazoo.Kazoo
)

// Testing configuration.
type Kafkabeat struct {
	config config.Config
	done   chan struct{}
	period time.Duration
	groups []string
	topics []string
	brokers []string
	zookeepers []string
	version sarama.KafkaVersion
	client beat.Client
}

type groupOffset struct {
	Partition int32  `json:"partition"`
	Offset    *int64 `json:"offset"`
	Lag       *int64 `json:"lag"`
}

type findGroupResult struct {
	done bool
	group string
}

// New creates an instance of Kafkabeat.
func  New(b *beat.Beat, cfg *common.Config) (beat.Beater, error)  {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Kafkabeat{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

func (bt *Kafkabeat) Config(b *beat.Beat) error {
	logp.Info("Configuring Kafkabeat...")

	// Initialize variable err
	var err error

	// Assign zookeepers to zookeepers in struct
	bt.zookeepers = bt.config.Zookeepers
	if bt.zookeepers == nil || len(bt.zookeepers) == 0 {
		fmt.Println("One zookeeper must be defined")
	}

	// Create a new sarama client to connect to brokers and zookeepers
	if sClient, err = sarama.NewClient([]string{"10.44.64.136:9092"}, bt.saramaConfig()); err != nil {
		fmt.Errorf("failed to create client err=%v", err)
	} else {
		logp.Info("Succesfully Connected to broker: %v")
	}

	// Connect to kazoo client
	zClient, err = kazoo.NewKazoo(bt.config.Brokers, nil)
	if err != nil {
		fmt.Errorf("Error identifying brokers from zookeeper: %v", err)
	}

	// Get a list of all brokers from kazoo
	bt.brokers, err = zClient.BrokerList()
	if (bt.brokers == nil || len(bt.brokers) == 0) {
		fmt.Println("Unable to identify active brokers")
	}
	logp.Info("Brokers: %v",bt.brokers)

	//This part of the program causes Run not to loop and publish
	
	// Get Topics
	bt.topics = bt.getTopics()
	
	// Get Consumer Groups
	bt.groups = bt.getConsumerGroups()

	return err
}

// Run starts Kafkabeat.
func (bt *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("Kafkabeat is running! Hit CTRL-C to stop it.")
	//Initialize error variable
	//var err error
	var err error
	// Pass beats to the config
	bt.Config(b)

	/*fmt.Println(group{})
	ctx := <-out
	for {
		if buf, err = marshal(ctx.output); err != nil {
			fmt.Errorf("Failed to marshal")
		}
		fmt.Println(string(buf))
		close(ctx.done)
	}

	fmt.Println ("\nPrinting buf\n")
	fmt.Println(string(buf))*/
	//Get partitions
	//partitions := bt.getTopicPartitions(topics)

	// Connect to beats client to publish events
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	// Initialize ticker with time set in config file
	ticker := time.NewTicker(bt.config.Period)
	//counter := 1
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
		
		for _, topic := range bt.topics {
				pids, err := bt.processTopic(topic)

				if err != nil {
					fmt.Errorf("Error with processing topic: %v", err)
					os.Exit(1)
				}

				events := bt.processGroups(bt.groups, topic, pids)
				fmt.Println("Publishing events")
				bt.client.PublishAll(events)

			}
	}
}

func (bt *Kafkabeat) processTopic(topic string) (map[int32]int64,error){
	pids, err := sClient.Partitions(topic)
	if err != nil {
		logp.Err("Unable to retrieve paritions for topic %v",topic)
		return nil,err
	}
	logp.Info("Partitions retrieved for topic %v",topic)
	return getPartitionSizes(topic, pids), nil
}

func getPartitionSizes(topic string, pids []int32) (map[int32]int64){
	pId_sizes := make(map[int32]int64)
	for _, pid := range pids {
		logp.Debug("kafkabeat","Processing partition %v", pid)
		pid_size,err:=sClient.GetOffset(topic, pid,sarama.OffsetNewest)
		if err != nil {
			logp.Err("Unable to identify size for partition %s and topic %s", pid,topic)
		} else {
			logp.Debug("kafkabeat","Current log size is %v for partition %v", strconv.FormatInt(pid_size,10), pid)
			pId_sizes[pid]=pid_size
		}

	}
	return pId_sizes
}

func (bt *Kafkabeat) processGroups(groups []string, topic string,pids map[int32]int64) ([]beat.Event){
	var events []beat.Event
	for _,group := range groups {
		pid_offsets,err := getConsumerOffsets(group, topic, pids)

		if err == nil {
			for pid,offset := range pid_offsets {

				size := pids[pid]

				event:=beat.Event{
					Timestamp: time.Now(),
					Fields: common.MapStr {
						"type": "consumer",
						"partition": pid,
						"topic":topic,
						"group": group,
						"lag": size-offset,
					},
				}

				events=append(events,event)
			}
		} else {
			logp.Debug("kafkabeat","No offsets for group %s on topic %s", group, topic)
		}
	}
	return events
}

func getConsumerOffsets(group string, topic string, pids map[int32]int64) (map[int32]int64,error) {
	broker,err := sClient.Coordinator(group)
	offsets := make(map[int32]int64)
	if err != nil {
		logp.Err("Unable to identify group coordinator for group %v",group)
	} else {
		request:=sarama.OffsetFetchRequest{ConsumerGroup:group,Version:1}
		for pid, size := range pids {
			if size > 0 {
				request.AddPartition(topic, pid)

			}
		}
		res,err := broker.FetchOffset(&request)
		if err != nil {
			logp.Err("Issue fetching offsets coordinator for topic %v",topic)
			logp.Err("%v",err)
		}
		var offset *sarama.OffsetFetchResponseBlock
		if res != nil {
			for pid := range pids {
				//offs := res.Blocks
				//fmt.Println(offs)
				offset = res.GetBlock(topic,pid)
				if offset != nil && offset.Offset > -1{
					offsets[pid]=offset.Offset
				}
			}
		}
	}
	return offsets,err
}

func (bt *Kafkabeat) getConsumerGroups() []string {
	//List all brokers...
	brokers := sClient.Brokers()

	// Initialize groups
	groups := []string{}

		// Find Groups must be passing a *sarama.Broker through, not string
	for _, g := range bt.findGroups(brokers) {
		groups = append(groups, g)
	}
	// Print the length of the groups
	fmt.Fprintf(os.Stderr, "found %v groups\n", len(groups))

	return groups
}

func (bt *Kafkabeat) getTopics() []string {
	//Initalize topics
	topics := []string{}

	// Calls function fetchTopics() for all topics
	for _, t := range bt.fetchTopics() {
		topics = append(topics, t)
	}
	// Print the length of the topics
	fmt.Fprintf(os.Stderr, "found %v topics\n", len(topics))

	return topics
}

func (bt *Kafkabeat) findGroups(brokers []*sarama.Broker) []string {
	var (
		doneCount int
		groups    = []string{}
		results   = make(chan findGroupResult)
		errs      = make(chan error)
	)

	for _, broker := range brokers {
		go bt.findGroupsOnBroker(broker, results, errs)
	}

awaitGroups:
	for {
		if doneCount == len(brokers) {
			return groups
		}

		select {
		case err := <-errs:
			fmt.Println("failed to find groups err=%v", err)
		case res := <-results:
			if res.done {
				doneCount++
				continue awaitGroups
			}

			groups = append(groups, res.group)
		}
	}
	return groups
}

func (bt *Kafkabeat) fetchTopics() []string {
	tps, err := sClient.Topics()
	if err != nil {
		fmt.Errorf("failed to read topics err=%v", err)
	}
	fmt.Println(tps[0])
	return tps
}

func (bt *Kafkabeat) findGroupsOnBroker(broker *sarama.Broker, results chan findGroupResult, errs chan error) {
	var (
		err  error
		resp *sarama.ListGroupsResponse
	)

	//fmt.Println(broker)
	if err = bt.connect(broker); err != nil {
		errs <- fmt.Errorf("failed to connect to broker %#v err=%s\n", broker.Addr(), err)
	}

	if resp, err = broker.ListGroups(&sarama.ListGroupsRequest{}); err != nil {
		errs <- fmt.Errorf("failed to list brokers on %#v err=%v", broker.Addr(), err)
	}
	if resp.Err != sarama.ErrNoError {
		errs <- fmt.Errorf("failed to list brokers on %#v err=%v", broker.Addr(), resp.Err)
	}

	for name := range resp.Groups {
		results <- findGroupResult{group: name}
	}
	results <- findGroupResult{done: true}
	
}

func (bt *Kafkabeat) connect(broker *sarama.Broker) error {
	if ok, _ := broker.Connected(); ok {
		return nil
	}

	if err := broker.Open(bt.saramaConfig()); err != nil {
		return err
	}

	connected, err := broker.Connected()
	if err != nil {
		return err
	}

	if !connected {
		return fmt.Errorf("failed to connect broker %#v", broker.Addr())
	}

	return nil
}

func (bt *Kafkabeat) saramaConfig() *sarama.Config {
	var cfg = sarama.NewConfig()
	cfg.Version = sarama.V0_10_0_0
	cfg.ClientID = "All-Groups" 

	return cfg
}

// Stop stops Kafkabeat.
func (bt *Kafkabeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
