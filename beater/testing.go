package beater

import (
	//"crypto/tls"
	"fmt"
	"sort"
	"sync"
	"time"
	"os"
	"os/user"
	"regexp"
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
	group  string
	topic  string
	brokers []string
	zookeepers []string
	partitions   []int32
	reset        int64
	verbose      bool
	pretty       bool
	filterGroups *regexp.Regexp
	filterTopics regexp.Regexp
	version sarama.KafkaVersion
	offsets      bool
	client beat.Client
}

type group struct {
	Name    string        `json:"name"`
	Topic   string        `json:"topic,omitempty"`
	Offsets []groupOffset `json:"offsets,omitempty"`
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

func (bt *Kafkabeat) Config(b *beat.Beat) {
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
		failf("failed to create client err=%v", err)
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

	out := make(chan printContext)
	go print(out, bt.pretty)

	// Get Consumer Groups
	groups := bt.getConsumerGroups()

	for i, grp := range groups {
		fmt.Println(grp)
		ctx := printContext{output: group{Name: grp}, done: make(chan struct{})}
		out <- ctx
		<-ctx.done

		if bt.verbose {
			fmt.Fprintf(os.Stderr, "%v/%v\n", i+1, len(groups))
		}
	}

	// Get Topics
	topics := bt.getTopics()

	//Get Topic Partitions
	topicPartitions := bt.getTopicPartitions(topics)

	wg := &sync.WaitGroup{}
	wg.Add(len(groups) * len(topics))
	for _, grp := range groups {
		for top, parts := range topicPartitions {
			go func(grp, topic string, partitions []int32) {
				bt.printGroupTopicOffset(out, grp, topic, partitions)
				wg.Done()
			}(grp, string(top), []int32(parts))
		}
	}

	wg.Wait()

	
}

// Run starts Kafkabeat.
func (bt *Kafkabeat) Run(b *beat.Beat) error {
	logp.Info("Kafkabeat is running! Hit CTRL-C to stop it.")
	//Initialize error variable
	var err error
	
	// Pass beats to the config
	bt.Config(b)

	// Get Topics
	topics := bt.getTopics()
	
	// Get Consumer Groups
	groups := bt.getConsumerGroups()

	//fmt.Println(group{})

	//Get partitions
	//partitions := bt.getTopicPartitions(topics)


	// Connect to beats client to publish events
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	// Initialize ticker with time set in config file
	ticker := time.NewTicker(bt.config.Period)
	counter := 1
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}

		event := beat.Event{
			Timestamp: time.Now(),
			Fields: common.MapStr{
				"type":    b.Info.Name,
				"counter": counter,
				"groups":   len(groups),
				"topics":   len(topics),
				"groupOffset": group{},

			},
		}
		bt.client.Publish(event)
		logp.Info("Event sent")
		counter++
	}
}

func (bt *Kafkabeat) getConsumerGroups() []string {
	//List all brokers...
	brokers := sClient.Brokers()

	// Initialize groups
	groups := []string{}

	// If group struct is empty then get all groups
	if bt.group == "" {
		// Find Groups must be passing a *sarama.Broker through, not string
		for _, g := range bt.findGroups(brokers) {
			groups = append(groups, g)
		}
	}
	// Print the length of the groups
	fmt.Fprintf(os.Stderr, "found %v groups\n", len(groups))

	return groups
}

func (bt *Kafkabeat) getTopics() []string {
	//Initalize topics
	topics := []string{}

	// If topic struct is empty then get all topics
	if bt.topic == "" {
		// Calls function fetchTopics() for all topics
		for _, t := range bt.fetchTopics() {
			topics = append(topics, t)
		}
	}
	// Print the length of the topics
	fmt.Fprintf(os.Stderr, "found %v topics\n", len(topics))

	return topics
}

func (bt *Kafkabeat) getTopicPartitions(topics []string) map[string][]int32 {

	topicPartitions := map[string][]int32{}
	parts := bt.partitions
	for _, topic := range topics {
		//fmt.Println(len(parts))
		if len(parts) == 0 {
			parts = bt.fetchPartitions(topic)
			//fmt.Fprintf(os.Stderr, "found partitions=%v for topic=%v\n", parts, topic)
		}
		topicPartitions[topic] = append(parts)
	}
	//fmt.Println(topicPartitions)

	return topicPartitions
}


func (bt *Kafkabeat) printGroupTopicOffset(out chan printContext, grp, top string, parts []int32) {
	target := group{Name: grp, Topic: top, Offsets: []groupOffset{}}
	results := make(chan groupOffset)
	done := make(chan struct{})

	wg := &sync.WaitGroup{}
	wg.Add(len(parts))
	for _, part := range parts {
		go bt.fetchGroupOffset(wg, grp, top, part, results)
	}
	go func() { wg.Wait(); close(done) }()

awaitGroupOffsets:
	for {
		select {
		case res := <-results:
			target.Offsets = append(target.Offsets, res)
		case <-done:
			break awaitGroupOffsets
		}
	}

	if len(target.Offsets) > 0 {
		sort.Slice(target.Offsets, func(i, j int) bool {
			return target.Offsets[j].Partition > target.Offsets[i].Partition
		})
		ctx := printContext{output: target, done: make(chan struct{})}
		out <- ctx
		<-ctx.done
	}
}

func (bt *Kafkabeat) resolveOffset(top string, part int32, off int64) int64 {
	resolvedOff, err := sClient.GetOffset(top, part, off)
	if err != nil {
		failf("failed to get offset to reset to for partition=%d err=%v", part, err)
	}

	if bt.verbose {
		fmt.Fprintf(os.Stderr, "resolved offset %v for topic=%s partition=%d to %v\n", off, top, part, resolvedOff)
	}

	return resolvedOff
}

func (bt *Kafkabeat) fetchGroupOffset(wg *sync.WaitGroup, grp, top string, part int32, results chan groupOffset) {
	var (
		err           error
		offsetManager sarama.OffsetManager
		shouldReset   = bt.reset >= 0 || bt.reset == sarama.OffsetNewest || bt.reset == sarama.OffsetOldest
	)

	if bt.verbose {
		fmt.Fprintf(os.Stderr, "fetching offset information for group=%v topic=%v partition=%v\n", grp, top, part)
	}

	defer wg.Done()

	if offsetManager, err = sarama.NewOffsetManagerFromClient(grp, sClient); err != nil {
		failf("failed to create client err=%v", err)
	}
	defer logClose("offset manager", offsetManager)

	pom, err := offsetManager.ManagePartition(top, part)
	if err != nil {
		failf("failed to manage partition group=%s topic=%s partition=%d err=%v", grp, top, part, err)
	}
	defer logClose("partition offset manager", pom)

	groupOff, _ := pom.NextOffset()
	if shouldReset {
		resolvedOff := bt.reset
		if resolvedOff == sarama.OffsetNewest || resolvedOff == sarama.OffsetOldest {
			resolvedOff = bt.resolveOffset(top, part, bt.reset)
		}
		groupOff = resolvedOff
		pom.MarkOffset(resolvedOff, "")
	}

	// we haven't reset it, and it wasn't set before - lag depends on client's config
	if groupOff == sarama.OffsetNewest || groupOff == sarama.OffsetOldest {
		results <- groupOffset{Partition: part}
		return
	}

	partOff := bt.resolveOffset(top, part, sarama.OffsetNewest)
	lag := partOff - groupOff
	results <- groupOffset{Partition: part, Offset: &groupOff, Lag: &lag}
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
		failf("failed to read topics err=%v", err)
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

func (bt *Kafkabeat) fetchPartitions(top string) []int32 {
	ps, err := sClient.Partitions(top)
	if err != nil {
		failf("failed to read partitions for topic=%s err=%v", top, err)
	}
	return ps
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
	var (
		err error
		usr *user.User
		cfg = sarama.NewConfig()
	)

	cfg.Version = sarama.V0_10_0_0
	fmt.Println(cfg.Version)
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "All-Groups" + sanitizeUsername(usr.Username)

	return cfg
}

// Stop stops Kafkabeat.
func (bt *Kafkabeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
