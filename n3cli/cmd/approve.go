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
	"log"
	"strings"

	nats "github.com/nats-io/go-nats"
	"github.com/nsip/n3-transport/messages"
	"github.com/nsip/n3-transport/messages/pb"
	"github.com/nsip/n3-transport/n3config"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// approveCmd represents the approve command
var approveCmd = &cobra.Command{
	Use:   "approve user(required) contextName(required)",
	Short: "Approve a user for access to a context",
	Long: `Approve a user to receive messages for a context.
	arguments are userid contextname:
	>approve userid context`,
	Args: cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("approve called")
		sendApproval(args)
	},
}

func init() {
	rootCmd.AddCommand(approveCmd)
}

func sendApproval(args []string) {

	// read the config
	err := n3config.ReadConfig()
	if err != nil {
		log.Fatalln("cannot proceed, no config found - run 'n3cli init' to create one")
	}

	// check the user
	user := strings.TrimSpace(args[0])
	contextName := strings.TrimSpace(args[1])
	if user == "" || contextName == "" {
		log.Println("cannot have empty arguments")
		return
	}

	// create approval tuple - predicate "deny" to revoke access
	// approvals are always only for contexts of this user
	approvalTuple := &pb.SPOTuple{Subject: user, Predicate: "grant", Object: contextName}
	tupleBytes, err := messages.EncodeTuple(approvalTuple)
	if err != nil {
		log.Println("unable to encode approval tuple: ", err)
		return
	}
	// log.Printf("%v\n", approvalTuple)

	// fetch required identity keys
	mypubKey := viper.GetString("pubkey")

	approvalMsg := &pb.N3Message{
		Payload:   tupleBytes,
		SndId:     mypubKey,
		NameSpace: mypubKey,
		CtxName:   "approvals",
	}

	approvalBytes, err := messages.EncodeN3Message(approvalMsg)
	if err != nil {
		log.Println("unable to encode approval message: ", err)
	}

	// send to approve
	natsAddr := viper.GetString("nats_addr")
	nc, err := nats.Connect(natsAddr)
	if err != nil {
		log.Println("cannot connect to nats server: ", err)
		return
	}
	defer nc.Close()

	err = nc.Publish("approvals", approvalBytes)
	if err != nil {
		log.Println("unable to publish approval message: ", err)
		return
	}

	log.Println("approval published.")

}
