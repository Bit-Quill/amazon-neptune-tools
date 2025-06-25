/*
Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune;

import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.help.Help;

@Cli(name = "neptune-bulk-loader.sh",
        description = "Bulk load Gremlin formatted csv files to Neptune",
        defaultCommand = Help.class,
        commands = {
                BulkLoadToNeptune.class,
                Help.class
        })
public class BulkLoadToNeptuneCli {
    public static void main(String[] args) {

        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(BulkLoadToNeptuneCli.class);

        try {
            Runnable cmd = cli.parse(args);
            cmd.run();
        } catch (Exception e) {

            System.err.println(e.getMessage());
            System.err.println();

            if (args.length > 0) {
                Runnable cmd = cli.parse("help", args[0]);
                cmd.run();
            } else {
                Runnable cmd = cli.parse("help");
                cmd.run();
            }

            System.exit(-1);
        }
    }
}
