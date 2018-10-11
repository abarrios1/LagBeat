package beater

import (
	//"crypto/tls"
	"fmt"
	//"sort"
	//"sync"
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
	group  []string
	topic  []string
	brokers []string
	zookeepers []string
	partitions   []int32
	reset        int64
	verbose      bool
	pretty       bool
	filterGroups *regexp.Regexp
	filterTopics *regexp.Regexp
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
	if bt.sClient, err = sarama.NewClient(bt.config.Zookeepers, sarama.NewConfig()); err != nil {
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
	fmt.Println(bt.brokers)

	// Assign topics to topic from sarama
	bt.topic, err = bt.sClient.Topics()  
	if err != nil {
		return err
	}
	fmt.Printf("Monitoring topics: %v",bt.topic)
	//group := sarama.Broker{}	
	
	fmt.Println("\nChecking out the groups:\n")
	// Get consumer groups using kazoo
	groups := bt.config.Group
	if (groups == nil || len(groups) == 0) {
		groups = []string{}
		for _, g := range bt.findGroups(brokers) {
			if bt.filterGroups.MatchString(g) {
				groups = append(groups, g)
			}
		}
	}
	fmt.Fprintf(os.Stderr, "found %v groups\n", len(groups))
		
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
	
func getGroups() ([]string,error) {
	group_list,err :=zClient.Consumergroups()
	if err != nil {
		logp.Err("Unable to retrieve groups")
		return nil,err
	}
	groups:=make([]string,len(group_list))
	for i, group := range group_list {
		groups[i]=group.Name
	}
	return groups,nil
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
}

func (bt *Testing) fetchTopics() []string {
	tps, err := bt.sClient.Topics()
	if err != nil {
		failf("failed to read topics err=%v", err)
	}
	return tps
}

func (bt *Testing) findGroupsOnBroker(broker *sarama.Broker, results chan findGroupResult, errs chan error) {
	var (
		err  error
		resp *sarama.ListGroupsResponse
	)
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

	cfg.Version = bt.version
	if usr, err = user.Current(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read current user err=%v", err)
	}
	cfg.ClientID = "kt-group-" + sanitizeUsername(usr.Username)

	return cfg
}

// Stop stops testing.
func (bt *Testing) Stop() {
	bt.client.Close()
	close(bt.done)
}
