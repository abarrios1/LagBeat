package main

import (
	"os"

	"github.com/abarrios1/testing/cmd"

	_ "github.com/abarrios1/testing/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
