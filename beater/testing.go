package beater

import (
	//"crypto/tls"
	"fmt"
	"time"
	"os"
	"regexp"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/abarrios1/testing/config"
	"github.com/Shopify/sarama"
)

// Testing configuration.
type Testing struct {
	config config.Config
	done   chan struct{}
	period time.Duration
	group  string
	topic  string
	brokers []string
	zookeepers []string
	filterGroups *regexp.Regexp
	sClient sarama.Client
	client beat.Client
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
	
	//return &Testing {
	//	done: make(chan struct{}),
	//}
}

// Run starts testing.
func (bt *Testing) Run(b *beat.Beat) error {
	logp.Info("testing is running! Hit CTRL-C to stop it.")

	var err error
	if bt.sClient, err = sarama.NewClient(bt.config.Zookeepers, sarama.NewConfig()); err != nil {
		fmt.Println("failed to create client err=%v", err)
	} else {
		fmt.Println("Succesfully Connected to broker")
	}
	//topic := bt.sClient.Topics()
	brokers := bt.sClient.Brokers()
	fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))
	fmt.Println(bt.group)
	groups := []string{bt.group}
	if bt.group == "" {
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

	if err := broker.Open(sarama.NewConfig()); err != nil {
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

// Stop stops testing.
func (bt *Testing) Stop() {
	bt.client.Close()
	close(bt.done)
}
