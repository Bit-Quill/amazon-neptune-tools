/*
Copyright 2025 Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.metadata;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

/**
 * Configuration class for Neptune bulk load settings from YAML file.
 * <p>
 * Expected YAML format:
 * bucket-name: "my-s3-bucket"
 * s3-prefix: "neptune"
 * neptune-endpoint: "my-neptune-cluster.cluster-abc123.us-east-1.neptune.amazonaws.com"
 * iam-role-arn: "arn:aws:iam::123456789012:role/NeptuneLoadFromS3"
 * parallelism: "OVERSUBSCRIBE"
 * monitor: true
 */
@Data
@NoArgsConstructor
public class BulkLoadConfig {

    private String bucketName;
    private String s3Prefix = "neptune";
    private String neptuneEndpoint;
    private String iamRoleArn;
    private String parallelism = "OVERSUBSCRIBE";
    private boolean monitor = true;

    public static BulkLoadConfig fromFile(File configFile) throws IOException {
        BulkLoadConfig config = new BulkLoadConfig();

        if (configFile != null && configFile.exists()) {
            Yaml yaml = new Yaml();
            try (FileInputStream inputStream = new FileInputStream(configFile)) {
                Map<String, Object> yamlData = yaml.load(inputStream);
                config.loadFromYaml(yamlData);
            }
        }

        return config;
    }

    private void loadFromYaml(Map<String, Object> yamlData) {
        if (yamlData != null) {
            this.bucketName = getStringValue(yamlData, "bucket-name");
            this.s3Prefix = getStringValue(yamlData, "s3-prefix", this.s3Prefix);
            this.neptuneEndpoint = getStringValue(yamlData, "neptune-endpoint");
            this.iamRoleArn = getStringValue(yamlData, "iam-role-arn");
            this.parallelism = getStringValue(yamlData, "parallelism", this.parallelism);
            this.monitor = getBooleanValue(yamlData, "monitor", this.monitor);
        }
    }

    private String getStringValue(Map<String, Object> yamlData, String key) {
        return getStringValue(yamlData, key, null);
    }

    private String getStringValue(Map<String, Object> yamlData, String key, String defaultValue) {
        Object value = yamlData.get(key);
        return value != null ? value.toString() : defaultValue;
    }

    private boolean getBooleanValue(Map<String, Object> yamlData, String key, boolean defaultValue) {
        Object value = yamlData.get(key);
        if (value instanceof Boolean) {
            return (Boolean) value;
        } else if (value instanceof String) {
            return Boolean.parseBoolean((String) value);
        }
        return defaultValue;
    }

    public BulkLoadConfig withBucketName(String bucketName) {
        if (bucketName != null && !bucketName.trim().isEmpty()) {
            this.bucketName = bucketName;
        }
        return this;
    }

    public BulkLoadConfig withS3Prefix(String s3Prefix) {
        if (s3Prefix != null && !s3Prefix.trim().isEmpty()) {
            this.s3Prefix = s3Prefix;
        }
        return this;
    }

    public BulkLoadConfig withNeptuneEndpoint(String neptuneEndpoint) {
        if (neptuneEndpoint != null && !neptuneEndpoint.trim().isEmpty()) {
            this.neptuneEndpoint = neptuneEndpoint;
        }
        return this;
    }

    public BulkLoadConfig withIamRoleArn(String iamRoleArn) {
        if (iamRoleArn != null && !iamRoleArn.trim().isEmpty()) {
            this.iamRoleArn = iamRoleArn;
        }
        return this;
    }

    public BulkLoadConfig withParallelism(String parallelism) {
        if (parallelism != null && !parallelism.trim().isEmpty()) {
            this.parallelism = parallelism;
        }
        return this;
    }

    public BulkLoadConfig withMonitor(Boolean monitor) {
        if (monitor != null) {
            this.monitor = monitor;
        }
        return this;
    }

    public static void validateBulkLoadConfig(BulkLoadConfig config) throws IllegalArgumentException {
        if (!config.isValid()) {
            StringBuilder errorMsg = new StringBuilder("Error: Missing required bulk load parameters. Please ensure the following are provided either via CLI or config file:\n");
            if (config.getNeptuneEndpoint() == null || config.getNeptuneEndpoint().trim().isEmpty()) {
                errorMsg.append("  - Neptune endpoint (--neptune-endpoint)\n");
            }
            if (config.getBucketName() == null || config.getBucketName().trim().isEmpty()) {
                errorMsg.append("  - S3 bucket name (--bucket-name)\n");
            }
            if (config.getIamRoleArn() == null || config.getIamRoleArn().trim().isEmpty()) {
                errorMsg.append("  - IAM role ARN (--iam-role-arn)\n");
            }
            errorMsg.append("Conversion aborted due to invalid bulk load configuration.");

            throw new IllegalArgumentException(errorMsg.toString());
        }
    }

    // Validation methods
    private boolean isValid() {
        return neptuneEndpoint != null && !neptuneEndpoint.trim().isEmpty() &&
               bucketName != null && !bucketName.trim().isEmpty() &&
               iamRoleArn != null && !iamRoleArn.trim().isEmpty();
    }
}
