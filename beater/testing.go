package beater

import (
	//"crypto/tls"
	"strings"
	"fmt"
	"time"
	"os"
	"os/user"
	"regexp"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/abarrios1/testing/config"
	"github.com/Shopify/sarama"
)

var (
	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
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
	version sarama.KafkaVersion
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
	brokers := bt.sClient.Brokers()
	fmt.Fprintf(os.Stderr, "found %v brokers\n", len(brokers))

	groups := []string{bt.config.Group}
	if bt.config.Group == "" {
		fmt.Println("No group was specified. Selecting all.")
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

func kafkaVersion(s string) sarama.KafkaVersion {
	dflt := sarama.V0_10_0_0
	switch s {
	case "v0.8.2.0":
		return sarama.V0_8_2_0
	case "v0.8.2.1":
		return sarama.V0_8_2_1
	case "v0.8.2.2":
		return sarama.V0_8_2_2
	case "v0.9.0.0":
		return sarama.V0_9_0_0
	case "v0.9.0.1":
		return sarama.V0_9_0_1
	case "v0.10.0.0":
		return sarama.V0_10_0_0
	case "v0.10.0.1":
		return sarama.V0_10_0_1
	case "v0.10.1.0":
		return sarama.V0_10_1_0
	case "v0.10.2.0":
		return sarama.V0_10_2_0
	case "":
		return dflt
	}

	fmt.Printf("unsupported kafka version %#v - supported: v0.8.2.0, v0.8.2.1, v0.8.2.2, v0.9.0.0, v0.9.0.1, v0.10.0.0, v0.10.0.1, v0.10.1.0, v0.10.2.0", s)
	return dflt
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}

// Stop stops testing.
func (bt *Testing) Stop() {
	bt.client.Close()
	close(bt.done)
}
