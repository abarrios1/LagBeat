package beater

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"golang.org/x/crypto/ssh/terminal"
	"io"
	"os"
	"regexp"
	"strings"
	"syscall"
)

var (
	invalidClientIDCharactersRegExp = regexp.MustCompile(`[^a-zA-Z0-9_-]`)
)

func logClose(name string, c io.Closer) {
	if err := c.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to close %#v err=%v", name, err)
	}
}

type printContext struct {
	output interface{}
	done   chan struct{}
}

func print(in <-chan printContext, pretty bool) {
	var (
		buf     []byte
		err     error
		marshal = json.Marshal
	)

	if pretty && terminal.IsTerminal(int(syscall.Stdout)) {
		marshal = func(i interface{}) ([]byte, error) { return json.MarshalIndent(i, "", "  ") }
	}

	for {
		ctx := <-in
		if buf, err = marshal(ctx.output); err != nil {
			failf("failed to marshal output %#v, err=%v", ctx.output, err)
		}

		fmt.Println(string(buf))
		close(ctx.done)
	}
}

func failf(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func readStdinLines(max int, out chan string) {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, max), max)

	for scanner.Scan() {
		out <- scanner.Text()
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "scanning input failed err=%v\n", err)
	}
	close(out)
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

func sanitizeUsername(u string) string {
	// Windows user may have format "DOMAIN|MACHINE\username", remove domain/machine if present
	s := strings.Split(u, "\\")
	u = s[len(s)-1]
	// Windows account can contain spaces or other special characters not supported
	// in client ID. Keep the bare minimum and ditch the rest.
	return invalidClientIDCharactersRegExp.ReplaceAllString(u, "")
}
