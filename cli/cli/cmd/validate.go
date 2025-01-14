// Copyright DataStax, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

//validateCmd validates the hocon file against the specified stargate server
var validateCmd = &cobra.Command{
	Short:   "Validate schema",
	Long:    `Validate schema`,
	Use:     "validate path [host]",
	Example: "stargate validate ./todo.conf http://server.stargate.com:8080",
	Args:    cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		path := args[0]
		var url string
		if len(args) == 2 {
			url = args[1]
		}
		err := Apply(cmd, "validate", path, url)
		if err != nil {
			cmd.PrintErrln(err)
			os.Exit(1)
		}
		cmd.Println("No errors found! 🎉")
	},
}

func init() {
	rootCmd.AddCommand(validateCmd)
}
