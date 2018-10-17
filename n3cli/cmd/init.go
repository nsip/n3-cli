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

	"github.com/nsip/n3-transport/n3config"
	"github.com/spf13/cobra"
)

// initCmd represents the init command
var initCmd = &cobra.Command{
	Use:   "init [config-directory]",
	Short: "initialises an N3 node",
	Long: `Creates local user identity for service interactions and sets default server/port
	connection details for nats, liftbridge, grpc etc. servers.
	All details are stored in [current working directory]/config/n3config.toml
	`,
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("init called")
		createNodeIdentity(args)
	},
}

func init() {
	rootCmd.AddCommand(initCmd)
}

//
// generates a new config file with default connection params for
// n3 servers, and pub/priv keypair for this node
//
func createNodeIdentity(args []string) {

	err := n3config.CreateBaseConfig()
	if err != nil {
		log.Fatalln("init failed, cannot create base config:", err)
	}

}
