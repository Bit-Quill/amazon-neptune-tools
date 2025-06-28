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

package com.amazonaws.services.neptune.util;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.File;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;

/**
 * Utility class for uploading local CSV files to Amazon S3 and loading them into Neptune
 * Designed for Neptune data loading workflows with bulk loading capability
 */
public class NeptuneBulkLoader implements AutoCloseable {

    private static final Set<String> BULK_LOAD_STATUS_CODES_COMPLETED = Set.of(
        "LOAD_COMPLETED",
        "LOAD_COMMITTED_W_WRITE_CONFLICTS"
    );
    private static final Set<String> BULK_LOAD_STATUS_CODES_FAILURES = Set.of(
        "LOAD_CANCELLED_BY_USER",
        "LOAD_CANCELLED_DUE_TO_ERRORS",
        "LOAD_UNEXPECTED_ERROR",
        "LOAD_FAILED",
        "LOAD_S3_READ_ERROR",
        "LOAD_S3_ACCESS_DENIED_ERROR",
        "LOAD_DATA_DEADLOCK",
        "LOAD_DATA_FAILED_DUE_TO_FEED_MODIFIED_OR_DELETED",
        "LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED",
        "LOAD_FAILED_INVALID_REQUEST",
        "LOAD_CANCELLED"
    );

    private static final String NEPTUNE_PORT = "8182"; // Default Neptune port for HTTP API
    private final S3AsyncClient s3AsyncClient;
    private final String bucketName;
    private final String s3Prefix;
    private final Region region;
    private final String neptuneEndpoint;
    private final String iamRoleArn;
    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;

    public NeptuneBulkLoader(
            String bucketName,
            String s3Prefix,
            Region region,
            String neptuneEndpoint,
            String iamRoleArn) {

        this.bucketName = bucketName;
        this.s3Prefix = s3Prefix;
        this.region = region;
        this.neptuneEndpoint = neptuneEndpoint;
        this.iamRoleArn = iamRoleArn;
        this.objectMapper = new ObjectMapper();

        this.s3AsyncClient = S3AsyncClient.builder()
                .region(region)
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(120))
                .build();

        // Validate for not empty parameters
        if (bucketName.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 bucket name cannot be empty");
        }
        if (s3Prefix.trim().isEmpty()) {
            throw new IllegalArgumentException("S3 prefix cannot be empty");
        }
        if (region.toString().trim().isEmpty()) {
            throw new IllegalArgumentException("Region cannot be empty");
        }
        if (neptuneEndpoint.trim().isEmpty()) {
            throw new IllegalArgumentException("Neptune endpoint cannot be empty");
        }
        if (iamRoleArn.trim().isEmpty()) {
            throw new IllegalArgumentException("IAM Role Arn cannot be empty");
        }
        System.out.println("S3 Bucket: " + bucketName);
        System.out.println("S3 Prefix: " + s3Prefix);
        System.out.println("AWS Region: " + region);
        System.out.println("Neptune Endpoint: " + neptuneEndpoint);
        System.out.println("IAM Role ARN: " + iamRoleArn);
    }

    /**
     * Upload Neptune vertices and edges CSV files asynchronously
     */
    public void uploadCsvFilesToS3(String filePath) throws Exception {
        System.out.println("Uploading Gremlin CSV to S3...");

        // Grab the timestamp of ConvertCvs to use as S3 directory key prefix
        String convertCsvTimeStamp = filePath.substring(filePath.lastIndexOf('/') + 1);

        // Check if the S3 prefix is provided, and construct the full S3 key prefix using convertCsvTimeStamp
        String s3KeyPrefixTimeStamp = Optional.ofNullable(s3Prefix)
            .filter(prefix  -> !prefix.isEmpty())
            .map(prefix  -> prefix + "/")
            .orElse("") + convertCsvTimeStamp;

        // Start both uploads concurrently
        CompletableFuture<Boolean> verticesFuture = uploadFileAsync(
            filePath + File.separator + "vertices.csv",
            s3KeyPrefixTimeStamp + "/" + "vertices.csv"
        );
        CompletableFuture<Boolean> edgesFuture = uploadFileAsync(
            filePath + File.separator + "edges.csv",
            s3KeyPrefixTimeStamp + "/" + "edges.csv"
        );
        // Wait for both uploads to complete
        CompletableFuture<Void> allUploads = CompletableFuture.allOf(verticesFuture, edgesFuture);
        allUploads.get();

        // Check results
        boolean verticesSuccess = verticesFuture.get();
        boolean edgesSuccess = edgesFuture.get();
        if (!verticesSuccess || !edgesSuccess) {
            System.err.println("Upload failures - Vertices: " + verticesSuccess + ", Edges: " + edgesSuccess);
            throw new RuntimeException("One or more CSV uploads failed.");
        }

        System.out.println("CSV files uploaded successfully to S3. Files available at: " +
            "s3://" + bucketName + "/" + s3KeyPrefixTimeStamp+ "/");
    }

    /**
     * Upload a specific CSV file to S3 asynchronously
     */
    protected CompletableFuture<Boolean> uploadFileAsync(String localFilePath, String s3Key) throws Exception {
        // Create a File object to check existence
        File file = new File(localFilePath);

        if (!file.exists() || !file.isFile()) {
            throw new IllegalStateException("File does not exist: " + localFilePath);
        }

        System.out.println("Starting async upload of " + localFilePath + " to s3://" + bucketName + "/" + s3Key);

        PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .contentType("text/csv")
                .build();

        // Create async request body from file
        AsyncRequestBody requestBody = AsyncRequestBody.fromFile(file);

        // Start async upload
        CompletableFuture<PutObjectResponse> uploadFuture = s3AsyncClient.putObject(putObjectRequest, requestBody);

        // Return a future that resolves to boolean success
        return uploadFuture.handle((response, throwable) -> {
            if (throwable != null) {
                if (throwable instanceof S3Exception) {
                    System.err.println("S3 error uploading file " + localFilePath + ": " + throwable);
                } else {
                    System.err.println("Unexpected error uploading file " + localFilePath + ": " + throwable);
                }
                return false;
            } else {
                System.out.println("Successfully uploaded " + file.getName() + " - ETag: " + response.eTag());
                return true;
            }
        });
    }

    /**
     * Start Neptune bulk load job with automatic fallback
     */
    public String startNeptuneBulkLoad(String s3SourceUri) {
        System.out.println("Starting Neptune bulk load...");
        if (!testNeptuneConnectivity()) {
            throw new RuntimeException("Cannot connect to Neptune endpoint: " + neptuneEndpoint);
        }
        try {
            String loaderEndpoint = "https://" + neptuneEndpoint + ":" + NEPTUNE_PORT + "/loader";
            String requestBody = String.format(
                "{\n" +
                "  \"source\": \"%s\",\n" +
                "  \"format\": \"csv\",\n" +
                "  \"iamRoleArn\": \"%s\",\n" +
                "  \"region\": \"%s\",\n" +
                "  \"failOnError\": \"FALSE\",\n" +
                "  \"parallelism\": \"MEDIUM\",\n" +
                "  \"updateSingleCardinalityProperties\": \"FALSE\",\n" +
                "  \"queueRequest\": \"FALSE\"\n" +
                "}",
                s3SourceUri, iamRoleArn, region
            );

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(loaderEndpoint))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .timeout(Duration.ofSeconds(120))
                    .build();

            // Retry configuration
            int maxRetries = 3;
            long initialBackoffMillis = 1000; // 1 second
            HttpResponse<String> response = null;
            String loadId = null;

            // Retry loop with exponential backoff
            for (int attempt = 0; attempt <= maxRetries; attempt++) {
                try {

                    response = httpClient.send(request,
                            HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() != 200) {
                        throw new RuntimeException("Failed to start Neptune bulk load. Status: " +
                            response.statusCode() + " Response: " + response.body());
                    }

                    JsonNode responseJson = objectMapper.readTree(response.body());
                    loadId = responseJson.get("payload").get("loadId").asText();
                    System.out.println("Neptune bulk load started with ID: " + loadId);
                    break;
                } catch (Exception e) {
                    if (attempt == maxRetries) {
                        throw new RuntimeException("Failed to start Neptune bulk load. Status: " +
                            response.statusCode() + " Response: " + response.body());
                    }
                    System.err.println("Attempt " + (attempt + 1) + " failed: " + e.getMessage());
                    Thread.sleep(initialBackoffMillis * (1L << attempt)); // Exponential backoff
                }
            }
            return loadId;
        } catch (Exception e) {
            System.err.println("An error occurred while converting Neo4j CSV file:");
            e.printStackTrace();
            return null;
        }
    }

    /**
     * Test connectivity to Neptune endpoint
     */
    protected boolean testNeptuneConnectivity() {
        try {
            System.out.println("Testing connectivity to Neptune endpoint...");
            String testEndpoint = "https://" + neptuneEndpoint + ":" + NEPTUNE_PORT + "/status";

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(testEndpoint))
                    .header("Content-Type", "application/json")
                    .GET()
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                System.err.println("Failed to connect to Neptune status endpoint. Status: " + response.statusCode());
                return false;
            }

            JsonNode responseBody = objectMapper.readTree(response.body());
            if (!responseBody.has("status") ||
                    !responseBody.get("status").asText().equals("healthy")) {
                throw new RuntimeException("Status not found or instance is not healthy: " + responseBody);
            }

            System.out.println("Successful connected to Neptune. Status: " +
                response.statusCode() + " " + responseBody.get("status").asText());
            return true;
        } catch (Exception e) {
            System.err.println("Neptune connectivity test failed: " + e.getLocalizedMessage());
            return false;
        }
    }

    /**
     * Monitor Neptune bulk load progress
     */
    public void monitorLoadProgress(String loadId) {
        System.out.println("Monitoring load progress for job: " + loadId);
        try {
            int sleepTimeMs = 1000;
            int maxAttempts = 300; // Monitor for up to 5 minutes
            int attempt = 0;

            while (attempt < maxAttempts) {
                String statusResponse = checkNeptuneBulkLoadStatus(loadId);

                if (statusResponse != null) {
                    try {
                        JsonNode responseJson = objectMapper.readTree(statusResponse);
                        String status = "UNKNOWN";

                        if (responseJson.has("payload") &&
                                responseJson.get("payload").has("overallStatus")) {
                            status = responseJson.get("payload")
                                .get("overallStatus").get("status").asText();
                        } else if (responseJson.has("status")) {
                            status = responseJson.get("status").asText();
                        }

                        if (BULK_LOAD_STATUS_CODES_COMPLETED.contains(status)) {
                            System.out.println("Neptune bulk load completed with status: " + status);
                            break;
                        } else if (BULK_LOAD_STATUS_CODES_FAILURES.contains(status)) {
                            System.err.println("Neptune bulk load failed with status: " + status);
                            System.err.println("Full response: " + statusResponse);
                            break;
                        } else {
                            System.out.println("Neptune bulk load status: " + status);
                        }
                    } catch (Exception e) {
                        System.err.println("Could not parse status response: " + e.getMessage());
                    }
                }

                Thread.sleep(sleepTimeMs); // Wait 1 second
                attempt++;
            }

            if (attempt >= maxAttempts) {
                System.err.println(
                    "Monitoring timeouted at " + sleepTimeMs * maxAttempts + "ms. Check load status manually.");
            }

        } catch (Exception e) {
            System.err.println("Error monitoring load progress: " + e.getMessage());
        }
    }

    /**
     * Check the status of a Neptune bulk load job via HTTP
     */
    private String checkNeptuneBulkLoadStatus(String loadId) {
        try {
            String statusEndpoint = "https://" + neptuneEndpoint + ":" + NEPTUNE_PORT + "/loader/" + loadId;

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(statusEndpoint))
                    .header("Content-Type", "application/json")
                    .GET()
                    .timeout(Duration.ofSeconds(30))
                    .build();

            HttpResponse<String> response = httpClient.send(request,
                    HttpResponse.BodyHandlers.ofString());

            if (response.statusCode() == 200) {
                return response.body();
            } else {
                System.err.println("Failed to check load status via HTTP. Status: " + response.statusCode());
                return null;
            }

        } catch (Exception e) {
            System.err.println("Error checking Neptune bulk load status: " + e);
            return null;
        }
    }

    /**
     * Close the S3 async client and release resources (AutoCloseable implementation)
     */
    @Override
    public void close() {
        if (s3AsyncClient != null) {
            s3AsyncClient.close();
        }
    }
}
