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

package com.amazonaws.services.neptune;

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.Neo4jStreamWriter;
import com.amazonaws.services.neptune.io.OutputFile;
import com.amazonaws.services.neptune.metadata.*;
import com.amazonaws.services.neptune.util.CSVUtils;
import com.amazonaws.services.neptune.util.NeptuneBulkLoader;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;

@Command(name = "convert-csv", description = "Converts CSV file exported from Neo4j via 'apoc.export.csv.all' to Neptune Gremlin load data formatted CSV files, \n" + //
        "and optionally automates the bulk loading of the converted data into Amazon Neptune.")
public class ConvertCsv implements Runnable {

    // Neo4j CSV file conversion options
    @Option(name = {"-d", "--dir"}, description = "Root directory for output")
    @Required
    @Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File outputDirectory;

    @Option(name = {"-df", "--dotenv-file"}, description = "Path to the Dotenv file for Neo4j AuraDB connection.")
    @RequireOnlyOne(tag = "input")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File envFile;

    @Option(name = {"-u", "--uri"}, description = "URI of the Neo4j Database to stream data from")
    @RequireOnlyOne(tag = "input")
    @Once
    private String uri;

    @Option(name = {"-n", "--username"}, description = "Neo4j Database username")
    @Once
    private String username;

    @Option(name = {"-pw", "--password"}, description = "Neo4j Database password")
    @Once
    private String password;

    @Option(name = {"-i", "--input"}, description = "Path to Neo4j CSV file")
    @RequireOnlyOne(tag = "input")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File inputFile;

    @Option(name = {"--conversion-config"}, description = "Path to YAML file containing configuration for label mappings and record filtering")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File conversionConfigFile;

    @Option(name = {"--node-property-policy"}, description = "Conversion policy for multi-valued node properties (default, 'PutInSetIgnoringDuplicates')")
    @Once
    @AllowedValues(allowedValues = {"LeaveAsString", "Halt", "PutInSetIgnoringDuplicates", "PutInSetButHaltIfDuplicates"})
    private MultiValuedNodePropertyPolicy multiValuedNodePropertyPolicy = MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates;

    @Option(name = {"--relationship-property-policy"}, description = "Conversion policy for multi-valued relationship properties (default, 'LeaveAsString')")
    @Once
    @AllowedValues(allowedValues = {"LeaveAsString", "Halt"})
    private MultiValuedRelationshipPropertyPolicy multiValuedRelationshipPropertyPolicy = MultiValuedRelationshipPropertyPolicy.LeaveAsString;

    @Option(name = {"--semi-colon-replacement"}, description = "Replacement for semi-colon character in multi-value string properties (default, ' ')")
    @Once
    @Pattern(pattern = "^[^;]*$", description = "Replacement string cannot contain a semi-colon.")
    private String semiColonReplacement = " ";

    @Option(name = {"--infer-types"}, description = "Infer data types for CSV column headings")
    @Once
    private boolean inferTypes = false;

    // Neptune bulk load options
    @Option(name = {"--bulk-load-config"}, description = "Path to YAML file containing configuration for enabling bulk load to Neptune. " +
        "If provided, configuration values are loaded from this file first, then overridden by any CLI parameters specified.")
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File bulkLoadConfigFile;

    @Option(name = {"--bucket-name"}, description = "S3 bucket name for CSV files to be stored. " +
        "Overrides bucket-name from bulk-load-config file if both are provided.")
    @Once
    private String bucketName;

    @Option(name = {"--s3-prefix"}, description = "S3 prefix for uploaded file (default: neptune). " +
        "Overrides s3-prefix from bulk-load-config file if both are provided.")
    @Once
    private String s3Prefix = "neptune";

    @Option(name = {"--neptune-endpoint"}, description =
        "Neptune cluster endpoint. Example: my-neptune-cluster.cluster-abc123.<region>.neptune.amazonaws.com. " +
        "Overrides neptune-endpoint from bulk-load-config file if both are provided. " +
        "Either this parameter or --bulk-load-config must be provided to enable bulk loading.")
    @Once
    private String neptuneEndpoint;

    @Option(name = {"--iam-role-arn"}, description = "IAM role ARN for Neptune bulk loading. It will need S3 and Neptune access permissions. " +
        "Overrides iam-role-arn from bulk-load-config file if both are provided. \n" +
        "Refer to the following documentation for the specific policies/permissions required:\n" + //
        "https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM-CreateRole.html\n" + //
        "https://docs.aws.amazon.com/neptune/latest/userguide/bulk-load-tutorial-IAM-add-role-cluster.html")
    @Once
    private String iamRoleArn;

    @Option(name = {"--parallelism"}, description = "Parallelism level for Neptune bulk loading (default: OVERSUBSCRIBE). " +
        "Overrides parallelism from bulk-load-config file if both are provided.")
    @Once
    @AllowedValues(allowedValues = {"LOW", "MEDIUM", "HIGH", "OVERSUBSCRIBE"})
    private String parallelism = "OVERSUBSCRIBE";

    @Option(name = {"--monitor"}, description = "Monitor Neptune bulk load progress until completion (default: true). " +
        "Overrides monitor from bulk-load-config file if both are provided.")
    @Once
    private boolean monitor = true;

    @Override
    public void run() {
        try {
            // Early validation: Check if user wants to bulk load and validate parameters before conversion
            BulkLoadConfig bulkLoadConfig = readBulkLoadConfig();

            // Load label mapping configuration
            ConversionConfig conversionConfig = ConversionConfig.fromFile(conversionConfigFile);

            Directories directories = Directories.createFor(outputDirectory);

            File input = inputFile;
            File tempDataFile = null;

            // if no input file provided, it is via streaming
            if (input == null) {
                String uriInput, usernameInput, passwordInput;
                if (envFile != null) {
                    Dotenv dotenv = Dotenv.configure()
                            .directory(envFile.getParent())
                            .filename(envFile.getName())
                            .load();
                    uriInput = dotenv.get("NEO4J_URI");
                    usernameInput = dotenv.get("NEO4J_USERNAME");
                    passwordInput = dotenv.get("NEO4J_PASSWORD");
                } else {
                    uriInput = uri;
                    usernameInput = username;
                    passwordInput = password;
                }
                try (Neo4jStreamWriter writer = new Neo4jStreamWriter(uriInput, usernameInput, passwordInput, directories)) {
                    tempDataFile = writer.streamToFile();
                }

                input = tempDataFile;
            }

            try (Timer timer = new Timer();
                 OutputFile vertexFile = new OutputFile(directories, "vertices");
                 OutputFile edgeFile = new OutputFile(directories, "edges");
                 CSVParser parser = CSVUtils.newParser(input)) {

                Iterator<CSVRecord> iterator = parser.iterator();

                final AtomicLong vertexCount = new AtomicLong(0);
                final AtomicLong edgeCount = new AtomicLong(0);
                final AtomicLong skippedVertexCount = new AtomicLong(0);
                final AtomicLong skippedEdgeCount = new AtomicLong(0);

                if (iterator.hasNext()) {
                    CSVRecord headers = iterator.next();

                    VertexMetadata vertexMetadata = VertexMetadata.parse(
                            headers,
                            new PropertyValueParser(multiValuedNodePropertyPolicy, semiColonReplacement, inferTypes),
                            conversionConfig);
                    EdgeMetadata edgeMetadata = EdgeMetadata.parse(
                            headers,
                            new PropertyValueParser(multiValuedRelationshipPropertyPolicy, semiColonReplacement, inferTypes),
                            conversionConfig, vertexMetadata.getSkippedVertexIds());

                    while (iterator.hasNext()) {
                        CSVRecord record = iterator.next();
                        if (vertexMetadata.isVertex(record)) {
                            vertexMetadata.toIterable(record).ifPresentOrElse(it -> {
                                try {
                                    vertexFile.printRecord(it);
                                    vertexCount.getAndIncrement();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }, skippedVertexCount::getAndIncrement);
                        } else if (edgeMetadata.isEdge(record)) {
                            edgeMetadata.toIterable(record).ifPresentOrElse(it -> {
                                try {
                                    edgeFile.printRecord(it);
                                    edgeCount.getAndIncrement();
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            }, skippedEdgeCount::getAndIncrement);
                        } else {
                            throw new IllegalStateException("Unable to parse record: " + record.toString());
                        }
                    }

                    vertexFile.printHeaders(vertexMetadata.headers());
                    edgeFile.printHeaders(edgeMetadata.headers());

                    System.err.println("Vertices: " + vertexCount);
                    System.err.println("Edges   : " + edgeCount);

                    if (conversionConfig.hasSkipRules()) {
                        System.err.println("Skipped vertices: " + skippedVertexCount);
                        System.err.println("Skipped edges  : " + skippedEdgeCount);
                        System.err.println("Skip rules: " +
                                (!conversionConfig.getSkipVertices().getById().isEmpty() ?
                                        conversionConfig.getSkipVertices().getById().size() + " vertex IDs, " : "") +
                                (!conversionConfig.getSkipVertices().getByLabel().isEmpty() ?
                                        conversionConfig.getSkipVertices().getByLabel().size() + " vertex labels, " : "") +
                                (!conversionConfig.getSkipEdges().getByLabel().isEmpty()  ?
                                        conversionConfig.getSkipEdges().getByLabel().size() + " edge labels" : ""));
                    }

                    System.err.println("Output  : " + directories.outputDirectory());

                    if (!conversionConfig.getVertexLabels().isEmpty() || !conversionConfig.getEdgeLabels().isEmpty()) {
                        System.err.println("Label mappings applied from: " + conversionConfigFile.getAbsolutePath());
                    }

                    System.out.println(directories.outputDirectory());

                    // delete streamed data file after conversion
                    if (tempDataFile != null) Files.deleteIfExists(tempDataFile.toPath());
                }

            }

            // Bulk loading happens AFTER conversion (using pre-validated config)
            if (bulkLoadConfig != null) {
                try (NeptuneBulkLoader neptuneBulkLoader = new NeptuneBulkLoader(
                        bulkLoadConfig.getBucketName(),
                        bulkLoadConfig.getS3Prefix(),
                        bulkLoadConfig.getNeptuneEndpoint(),
                        bulkLoadConfig.getIamRoleArn(),
                        bulkLoadConfig.getParallelism())) {

                    String uri = directories.outputDirectory().toFile().getAbsolutePath();
                    String s3SourceUri = neptuneBulkLoader.uploadCsvFilesToS3(uri);
                    String loadId = neptuneBulkLoader.startNeptuneBulkLoad(s3SourceUri);

                    if (bulkLoadConfig.isMonitor()) {
                        neptuneBulkLoader.monitorLoadProgress(loadId);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred while running convert-csv:");
            e.printStackTrace();
        }
    }

    /**
     * Validates and prepares bulk load configuration if bulk loading is requested.
     * @return BulkLoadConfig if bulk loading is requested and valid, null if no bulk loading requested
     * @throws IllegalArgumentException if bulk loading is requested but configuration is invalid
     * @throws IOException if there's an error reading the bulk load config file
     */
    private BulkLoadConfig readBulkLoadConfig() throws Exception {
        if (bulkLoadConfigFile == null && neptuneEndpoint == null) {
            return null; // No bulk loading requested
        }

        BulkLoadConfig config = BulkLoadConfig.fromFile(bulkLoadConfigFile)
            .withBucketName(bucketName)
            .withS3Prefix(s3Prefix)
            .withNeptuneEndpoint(neptuneEndpoint)
            .withIamRoleArn(iamRoleArn)
            .withParallelism(parallelism)
            .withMonitor(monitor);

        BulkLoadConfig.validateBulkLoadConfig(config);

        return config;
    }
}
