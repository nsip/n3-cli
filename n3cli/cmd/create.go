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
	"github.com/spf13/viper"
)

// createCmd represents the create command
var createCmd = &cobra.Command{
	Use:   "create context-name(required)",
	Short: "establishes a new data context on the messaging servers",
	Long: `Contexts are namespaces for managing user access and collaboration
	on a dataset. Create a context and approve users to read and write data
	within that context.
	Contexts also allow context-wide and user privacy restrictions.
	Creating a context means it is associated with you as the owner, and only approved
	users (by you) can write and read from that context.
	You will be automatically added as an approved user for any context you create.
	`,
	Args: cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// fmt.Println("create called")
		// fmt.Printf("args:\n%v\n", args)
		createNewContext(args[0])
	},
}

func init() {
	contextCmd.AddCommand(createCmd)
}

//
// contexts are actually virtual namsepaces for message delivery
// and are never directly created on the transport layer,
// the only real work here is to issue an aporoval for the creating user
// so that they can publish messages to this context, and also
// approve other users to access it.
//
func createNewContext(contextName string) {

	// read the config
	err := n3config.ReadConfig()
	if err != nil {
		log.Fatalln("cannot proceed, no config found - run 'n3cli init' to create one")
	}

	// id of this user
	user := viper.GetString("pubkey")

	// approve owner for access to the context
	approveCmd.Run(approveCmd, []string{user, contextName})

}
