package beater

import (
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"regexp"
	"strings"
)

var (
	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

type printContext struct {
	output interface{}
	done   chan struct{}
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

	failf("unsupported kafka version %#v - supported: v0.8.2.0, v0.8.2.1, v0.8.2.2, v0.9.0.0, v0.9.0.1, v0.10.0.0, v0.10.0.1, v0.10.1.0, v0.10.2.0", s)
	return dflt
}

func failf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}
