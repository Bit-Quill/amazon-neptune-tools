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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import com.amazonaws.services.neptune.util.NeptuneBulkLoader;

import software.amazon.awssdk.regions.Region;

/**
 * Test data provider utility class for Neptune bulk loader tests
 * Contains helper methods for creating mock data and accessing private fields
 */
public class TestDataProvider {

    // Test constants
    public static final String BUCKET = "test-neptune-bucket";
    public static final String S3_PREFIX = "test-prefix";
    public static final String S3_KEY = "test-s3-key";
    public static final Region REGION_US_EAST_2 = Region.US_EAST_2;
    public static final String NEPTUNE_ENDPOINT = "test-neptune.cluster-abc123.us-east-2.neptune.amazonaws.com";
    public static final String IAM_ROLE_ARN = "arn:aws:iam::123456789012:role/TestNeptuneRole";
    public static final String TEMP_FOLDER_NAME = "TEST_TEMP_FOLDER";
    public static final String VERTICIES_CSV = "vertices.csv";
    public static final String EDGES_CSV = "edges.csv";
    public static final String S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES = S3_KEY + "/" + VERTICIES_CSV;
    public static final String S3_KEY_FOR_UPLOAD_FILE_ASYNC_EDGES = S3_KEY + "/" + EDGES_CSV;

    public static NeptuneBulkLoader createNeptuneBulkLoader(
            String bucket, String s3Prefix, Region region, String neptuneEndpoint, String iamRoleArn) {
        try (NeptuneBulkLoader loader = new NeptuneBulkLoader(
            bucket,
            s3Prefix,
            region,
            neptuneEndpoint,
            iamRoleArn
        )) {
            return loader;
        }
    }

    public static NeptuneBulkLoader createNeptuneBulkLoader() {
        try (NeptuneBulkLoader loader = new NeptuneBulkLoader(
            BUCKET,
            S3_PREFIX,
            REGION_US_EAST_2,
            NEPTUNE_ENDPOINT,
            IAM_ROLE_ARN
        )) {
            return loader;
        }
    }

    /**
     * Creates mock CSV files (both vertices and edges) in the specified directory
     * @param directory The directory where CSV files should be created
     * @param verticesFile The file location where verticies CSV data should be written
     * @param edgesFile The file location where edges CSV data should be written
     * @throws IOException If file creation fails
     */
    public static void createMockCsvFiles(File directory, File verticesFile, File edgesFile) throws IOException {
        createMockVerticesFile(directory, verticesFile);
        createMockEdgesFile(directory, edgesFile);
    }

    /**
     * Creates mock CSV files (both vertices and edges) in the specified directory
     * @param directory The directory where CSV files should be created
     * @throws IOException If file creation fails
     */
    public static void createMockCsvFiles(File directory) throws IOException {
        File testVerticiesFile = new File(directory, TestDataProvider.VERTICIES_CSV);
        File testEdgesFile = new File(directory, TestDataProvider.EDGES_CSV);
        createMockVerticesFile(directory, testVerticiesFile);
        createMockEdgesFile(directory, testEdgesFile);
    }

    /**
     * Creates a mock vertices.csv file with sample Neptune vertex data
     * @param directory The directory where the vertices.csv file should be created
     * @throws IOException If file creation fails
     */
    public static void createMockVerticesFile(File directory, File verticesFile) throws IOException {
        String verticesContent = "~id,~label,name,age\n" +
                                "v1,Person,John,30\n" +
                                "v2,Person,Jane,25\n" +
                                "v3,Company,ACME,null\n";
        Files.write(verticesFile.toPath(), verticesContent.getBytes());
    }

    /**
     * Creates a mock edges.csv file with sample Neptune edge data
     * @param directory The directory where the edges.csv file should be created
     * @throws IOException If file creation fails
     */
    public static void createMockEdgesFile(File directory, File edgesFile) throws IOException {
        String edgesContent = "~id,~from,~to,~label,weight\n" +
                             "e1,v1,v2,knows,0.8\n" +
                             "e2,v1,v3,works_for,1.0\n" +
                             "e3,v2,v3,works_for,1.0\n";
        Files.write(edgesFile.toPath(), edgesContent.getBytes());
    }
}
