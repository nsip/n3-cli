// Copyright © 2018 NAME HERE <EMAIL ADDRESS>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"fmt"

	liftbridge "github.com/liftbridge-io/go-liftbridge"
	"github.com/spf13/cobra"
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create",
	Short: "establishes a new context on the messaging servers",
	Long: `Creates a nats topic for the context, establishes a context.user publishing 
	channel for the user who creates it.
	Creates a stream to record entries published to the channel.
	Creates an aggregate stream for the context - context.* - which will collect
	messages from all users of the context.
	Creates a stream to record entries to the aggregate channel, this is used as 
	input to data storage or query services.
	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("create called")
		// fmt.Printf("args:\n%v\n", args)
		// fmt.Println("context name: ", contextName)
		createNewContext()
	},
}

var contextName string

func init() {
	contextCmd.AddCommand(createCmd)

	createCmd.Flags().StringVarP(&contextName, "name", "n", "SIF", "Name of the context to be created")
	createCmd.MarkFlagRequired("name")

	// TODO: possible flag to toggle stream for user channel, only required for audit so
	// maybe aloow users to choose - will be created by default for now
}

//
// does the work of creating the required nats channels and liftbridge streams
// for a context to be available on the broker
//
func createNewContext() {

	// connect to liftbridge server
	addr := "localhost:9292" // HACK: shold come from config
	lbClient, err := liftbridge.Connect([]string{addr})
	if err != nil {
		fmt.Println("Error connecting to liftbridge: ", err)
		return
	}
	// fmt.Println("liftbridge connection established")

	// create the streaminfo definition
	userName := "nsip01" // HACK: should come from config
	userSubject := fmt.Sprintf("%s.%s", contextName, userName)
	userStreamName := fmt.Sprintf("%s.%s-stream", contextName, userName)
	aggregateSubject := fmt.Sprintf("%s.*", contextName)
	aggregateStreamName := fmt.Sprintf("%s.stream", contextName)

	// create streaminfo for desired context/user combination
	// - context.user on NATS
	// - context.user-stream on LB
	userStream := liftbridge.StreamInfo{
		Subject:           userSubject,
		Name:              userStreamName,
		ReplicationFactor: 1,
	}
	// create stream info for aggregate stream
	// - context.* on NATS
	// - context.stream on LB
	aggregateStream := liftbridge.StreamInfo{
		Subject:           aggregateSubject,
		Name:              aggregateStreamName,
		ReplicationFactor: 1,
	}

	// attempt aggregate stream first, abort if exists - was already created by
	// someone else
	if err := lbClient.CreateStream(context.Background(), aggregateStream); err != nil {
		if err != liftbridge.ErrStreamExists {
			fmt.Println("Error creating context: ", err)
			return
		} else {
			fmt.Printf("\nCannot create context, %s, as it already exists...\n", contextName)
			fmt.Println("Use 'n3cli context join' to connect to an existing context\n")
			return
		}
	}
	fmt.Println("successfully created context aggregate - ", aggregateStream.Name)

	// create streams on broker, ignore errors if alsready created
	if err := lbClient.CreateStream(context.Background(), userStream); err != nil {
		if err != liftbridge.ErrStreamExists {
			fmt.Println("Error creating user context: ", err)
			return
		}
	}
	fmt.Println("successfully created context for user - ", userStream.Name)

}
