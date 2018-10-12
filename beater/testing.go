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
var zClient *kazoo.Kazoo

// Testing configuration.
type Testing struct {
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
	sClient sarama.Client
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

// New creates an instance of testing.
func  New(b *beat.Beat, cfg *common.Config) (beat.Beater, error)  {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Testing{
		done:   make(chan struct{}),
		config: c,
	}
	return bt, nil
}

// Run starts testing.
func (bt *Testing) Run(b *beat.Beat) error {
	logp.Info("testing is running! Hit CTRL-C to stop it.")

	var err error
	// Create a new sarama client to connect to brokers and zookeepers
	if bt.sClient, err = sarama.NewClient([]string{"10.44.64.136:9092"}, bt.saramaConfig()); err != nil {
		failf("failed to create client err=%v", err)
	} else {
		fmt.Println("Succesfully Connected to broker")
	}
	
	// Assign zookeepers to zookeepers in struct
	bt.zookeepers = bt.config.Zookeepers
	if bt.zookeepers == nil || len(bt.zookeepers) == 0 {
		fmt.Println("One zookeeper must be defined")
	}

	// Connect to kazoo client
	zClient, err = kazoo.NewKazoo(bt.config.Brokers, nil)
	if err != nil {
		fmt.Println("Error identifying brokers from zookeeper")
		return err
	}
	
	brokers := bt.sClient.Brokers()
	// Get a list of all brokers from kazoo
	bt.brokers, err = zClient.BrokerList()
	if (bt.brokers == nil || len(bt.brokers) == 0) {
		fmt.Println("Unable to identify active brokers")
	}
	logp.Info("Brokers: %v",bt.brokers)

	fmt.Println("\nChecking out the groups:\n")
	// Get consumer groups using kazoo
	groups := []string{bt.group}
	//fmt.Println(len(groups))
	if bt.group == "" {
		groups = []string{}
		for _, g := range bt.findGroups(brokers) {
			//if bt.filterGroups.MatchString(g) {
			groups = append(groups, g)
			//}
		}
	}
	fmt.Fprintf(os.Stderr, "found %v groups\n", len(groups))
	fmt.Println(groups)	
	topics := []string{bt.topic}
	if bt.topic == "" {
		topics = []string{}
		for _, t := range bt.fetchTopics() {
			//if bt.filterTopics.MatchString(t) {
			topics = append(topics, t)
			//	fmt.Println("In for loop topics")
			//}
			//fmt.Println(t)
		}
	}
	fmt.Fprintf(os.Stderr, "found %v topics\n", len(topics))

	out := make(chan printContext)
	go print(out, bt.pretty)

	fmt.Println(bt.offsets)

	fmt.Println(!bt.offsets)

		for i, grp := range groups {
			fmt.Println(grp)
			ctx := printContext{output: group{Name: grp}, done: make(chan struct{})}
			out <- ctx
			<-ctx.done

			if bt.verbose {
				fmt.Fprintf(os.Stderr, "%v/%v\n", i+1, len(groups))
			}
		}
	
	topicPartitions := map[string][]int32{}
	
	for _, topic := range topics {
		parts := bt.partitions
		fmt.Println(len(parts))
		if len(parts) == 0 {
			parts = bt.fetchPartitions(topic)
			fmt.Fprintf(os.Stderr, "found partitions=%v for topic=%v\n", parts, topic)
		}
		topicPartitions[topic] = parts
	}

	wg := &sync.WaitGroup{}
	wg.Add(len(groups) * len(topics))
	for _, grp := range groups {
		for top, parts := range topicPartitions {
			go func(grp, topic string, partitions []int32) {
				bt.printGroupTopicOffset(out, grp, topic, partitions)
				wg.Done()
			}(grp, top, parts)
		}
	}
	wg.Wait()

	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

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
				"group": bt.config.Group,
				"topic": bt.config.Topic,
				"brokers": bt.config.Zookeepers,
			},
		}
		bt.client.Publish(event)
		logp.Info("Event sent")
		counter++
	}

}

func (bt *Testing) printGroupTopicOffset(out chan printContext, grp, top string, parts []int32) {
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

func (bt *Testing) resolveOffset(top string, part int32, off int64) int64 {
	resolvedOff, err := bt.sClient.GetOffset(top, part, off)
	if err != nil {
		failf("failed to get offset to reset to for partition=%d err=%v", part, err)
	}

	if bt.verbose {
		fmt.Fprintf(os.Stderr, "resolved offset %v for topic=%s partition=%d to %v\n", off, top, part, resolvedOff)
	}

	return resolvedOff
}

func (bt *Testing) fetchGroupOffset(wg *sync.WaitGroup, grp, top string, part int32, results chan groupOffset) {
	var (
		err           error
		offsetManager sarama.OffsetManager
		shouldReset   = bt.reset >= 0 || bt.reset == sarama.OffsetNewest || bt.reset == sarama.OffsetOldest
	)

	if bt.verbose {
		fmt.Fprintf(os.Stderr, "fetching offset information for group=%v topic=%v partition=%v\n", grp, top, part)
	}

	defer wg.Done()

	if offsetManager, err = sarama.NewOffsetManagerFromClient(grp, bt.sClient); err != nil {
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
	
type findGroupResult struct {
	done bool
	group string
}

func (bt *Testing) findGroups(brokers []*sarama.Broker) []string {
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

func (bt *Testing) fetchTopics() []string {
	tps, err := bt.sClient.Topics()
	if err != nil {
		failf("failed to read topics err=%v", err)
	}
	fmt.Println(tps[0])
	return tps
}

func (bt *Testing) findGroupsOnBroker(broker *sarama.Broker, results chan findGroupResult, errs chan error) {
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

func (bt *Testing) fetchPartitions(top string) []int32 {
	ps, err := bt.sClient.Partitions(top)
	if err != nil {
		failf("failed to read partitions for topic=%s err=%v", top, err)
	}
	return ps
}

func (bt *Testing) connect(broker *sarama.Broker) error {
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

func (bt *Testing) saramaConfig() *sarama.Config {
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

// Stop stops testing.
func (bt *Testing) Stop() {
	bt.client.Close()
	close(bt.done)
}
