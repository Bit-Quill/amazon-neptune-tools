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

import com.amazonaws.services.neptune.util.NeptuneBulkLoader;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import software.amazon.awssdk.regions.Region;

import java.io.File;

@Command(name = "neptune-bulk-load", description = "Upload CSV files to S3 and bulk load into Neptune")
public class NeptuneBulkLoad implements Runnable {

    @Option(name = {"--bucket-name"}, description = "S3 bucket name for CSV files output")
    @Required
    @Once
    private String bucketName;

    @Option(name = {"--region"}, description = "AWS region S3 bucket is located in (default: us-east-2)")
    @Once
    private String region = "us-east-2";

    @Option(name = {"-d", "--dir"}, description = "Root directory containing CSV files")
    @Required
    @Path(mustExist = true, kind = PathKind.DIRECTORY)
    @Once
    private File csvOutputDir;

    @Option(name = {"--s3-prefix"}, description = "S3 key prefix for uploaded files (default: neptune)")
    @Once
    private String s3Prefix = "neptune";

    @Option(name = {"--neptune-endpoint"}, description = "Neptune cluster endpoint")
    @Required
    @Once
    private String neptuneEndpoint;

    @Option(name = {"--iam-role-arn"}, description = "IAM role ARN for with Neptune permissions")
    @Required
    @Once
    private String iamRoleArn;

    @Option(name = {"--upload-only"}, description = "Only upload files to S3, skip Neptune bulk loading")
    @Once
    private boolean uploadOnly = false;

    @Option(name = {"--load-only"}, description = "Only perform Neptune bulk loading (assumes files already in S3)")
    @Once
    private boolean loadOnly = false;

    @Option(name = {"--monitor"}, description = "Monitor Neptune bulk load progress until completion")
    @Once
    private boolean monitor = true;

    @Override
    public void run() {
        try {
            // Validate conflicting options
            if (uploadOnly && loadOnly) {
                throw new IllegalArgumentException("Cannot specify both --upload-only and --load-only");
            }

            // Create NeptuneBulkLoader with custom configuration
            try (NeptuneBulkLoader neptuneBulkLoader = new NeptuneBulkLoader(
                    bucketName,
                    s3Prefix,
                    Region.of(region),
                    neptuneEndpoint,
                    iamRoleArn)) {

                if (!loadOnly) {
                    neptuneBulkLoader.uploadCsvFilesToS3(csvOutputDir.getAbsolutePath());
                }

                if (!uploadOnly) {
                    String trimmedBucketName = bucketName.replaceAll("/+$", "");
                    String trimmedS3Prefix = s3Prefix.replaceAll("/+$", "");
                    String s3SourceUri = "s3://" + trimmedBucketName + "/" + trimmedS3Prefix;
                    String loadId = neptuneBulkLoader.startNeptuneBulkLoad(s3SourceUri);

                    if (loadId != null) {
                        System.out.println("Neptune bulk load started successfully! Load ID: " + loadId);

                        if (monitor) {
                            neptuneBulkLoader.monitorLoadProgress(loadId);
                        }
                    } else {
                        System.err.println("Failed to start Neptune bulk load");
                        System.exit(1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
