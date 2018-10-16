package beater

import (
	"strconv"
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
	out = make(chan printContext)
)

// Testing configuration.
type Kafkabeat struct {
	config config.Config
	done   chan struct{}
	period time.Duration
	groups []string
	group  string
	topic  string
	topics []string
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

	for _, topic := range topics {
		pids, err := bt.processTopic(topic)

		fmt.Println(pids)
		events := bt.processGroups(groups, topic, pids)

		if err != nil {
			fmt.Errorf("Error with processing topic: %v", err)
			os.Exit(1)
		}
		fmt.Println(events)


	}

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

	// Get Topics
	bt.topics = bt.getTopics()
	
	// Get Consumer Groups
	bt.groups = bt.getConsumerGroups()

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
				fmt.Println("Publishing events!")
				fmt.Println(events)
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

				size,ok := pids[pid]

				if ok {
					fmt.Println("Okay is all good")
				}
				fmt.Println("\nThis is the pid in process groups.")
				fmt.Println(pid)
				fmt.Println("\nThis is the topic in process groups")
				fmt.Println(topic)
				fmt.Println("\nThis is the group in process groups")
				fmt.Println(group)
				fmt.Println("\nThis is the lag in process groups")
				fmt.Println(size-offset)
				event:=beat.Event{
					Timestamp: time.Now(),
					Fields: common.MapStr {
						"type": "consumer",
						"partition": pid,
						"topic":topic,
						"group": group,
						"offset": size-offset,
					},
				}
				fmt.Println("Publishing Events!")
				fmt.Println(event)

				events=append(events,event)
			}
		} else {
			logp.Debug("kafkabeat","No offsets for group %s on topic %s", group, topic)
		}
	}
	return events
}

func getConsumerOffsets(group string, topic string, pids map[int32]int64) (map[int32]int64,error) {
	fmt.Println("Checking pids")
	fmt.Println(pids)
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
		fmt.Println("\nFetch request of offset")
		fmt.Println(request)
		res,err := broker.FetchOffset(&request)
		fmt.Println("\nGetting request result")
		fmt.Println(res)
		if err != nil {
			logp.Err("Issue fetching offsets coordinator for topic %v",topic)
			logp.Err("%v",err)
		}
		var offset *sarama.OffsetFetchResponseBlock
		if res != nil {
			for pid := range pids {
				fmt.Println("\nChecking pid")
				fmt.Println(pid)
				fmt.Println("\nChecking topic")
				fmt.Println(topic)
				offs := res.Blocks
				fmt.Println(offs["metricbeat"][0])
				offset = res.GetBlock(topic,pid)
				fmt.Println("\nChecking offset:")
				fmt.Println(offset)
				if offset != nil && offset.Offset > -1{
					offsets[pid]=offset.Offset
				}
			}
		}
		fmt.Println("\nChecking offsets:")
		fmt.Println(offsets)
		fmt.Println("\nChecking errors:")
		fmt.Println(err)
	}
	return offsets,err
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
	for _, topic := range topics {
		parts := bt.partitions
		if len(parts) == 0 {
			parts = bt.fetchPartitions(topic)
			fmt.Fprintf(os.Stderr, "found partitions=%v for topic=%v\n", parts, topic)
		}
		topicPartitions[topic] = parts
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
		fmt.Println("failed to get offset to reset to for partition=%d on topic=%v err=%v", part, top,err)
	}

	if bt.verbose {
		fmt.Fprintf(os.Stderr, "resolved offset %v for topic=%s partition=%d to %v\n", off, top, part, resolvedOff)
	}

	return resolvedOff
}

func (bt *Kafkabeat) fetchGroupOffset(wg *sync.WaitGroup, grp, top string, part int32, results chan groupOffset) chan groupOffset {
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
		//return
	}

	partOff := bt.resolveOffset(top, part, sarama.OffsetNewest)
	lag := partOff - groupOff
	results <- groupOffset{Partition: part, Offset: &groupOff, Lag: &lag}
	return results
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
