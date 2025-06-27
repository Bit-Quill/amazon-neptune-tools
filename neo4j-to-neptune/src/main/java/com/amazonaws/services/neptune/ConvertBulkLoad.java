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

import com.amazonaws.services.neptune.io.Directories;
import com.amazonaws.services.neptune.io.OutputFile;
import com.amazonaws.services.neptune.metadata.*;
import com.amazonaws.services.neptune.util.CSVUtils;
import com.amazonaws.services.neptune.util.NeptuneBulkLoader;
import com.amazonaws.services.neptune.util.Timer;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.*;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import software.amazon.awssdk.regions.Region;

import java.io.File;
import java.util.Iterator;

@Command(name = "convert-bulk-load", description = "Convert Neo4j CSV to Neptune format and bulk load to Neptune")
public class ConvertBulkLoad implements Runnable {

    // ConvertCsv options
    @Option(name = {"-i", "--input"}, description = "Path to Neo4j CSV file")
    @Required
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File inputFile;

    @Option(name = {"-d", "--dir"}, description = "Root directory for output")
    @Required
    @Path(mustExist = false, kind = PathKind.DIRECTORY)
    @Once
    private File outputDirectory;

    @Option(name = {"--node-property-policy"}, description = "Conversion policy for multi-valued node properties (default: 'PutInSetIgnoringDuplicates')")
    @Once
    @AllowedValues(allowedValues = {"LeaveAsString", "Halt", "PutInSetIgnoringDuplicates", "PutInSetButHaltIfDuplicates"})
    private MultiValuedNodePropertyPolicy multiValuedNodePropertyPolicy = MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates;

    @Option(name = {"--relationship-property-policy"}, description = "Conversion policy for multi-valued relationship properties (default: 'LeaveAsString')")
    @Once
    @AllowedValues(allowedValues = {"LeaveAsString", "Halt"})
    private MultiValuedRelationshipPropertyPolicy multiValuedRelationshipPropertyPolicy = MultiValuedRelationshipPropertyPolicy.LeaveAsString;

    @Option(name = {"--semi-colon-replacement"}, description = "Replacement for semi-colon character in multi-value string properties (default: ' ')")
    @Once
    @Pattern(pattern = "^[^;]*$", description = "Replacement string cannot contain a semi-colon.")
    private String semiColonReplacement = " ";

    @Option(name = {"--infer-types"}, description = "Infer data types for CSV column headings")
    @Once
    private boolean inferTypes = false;

    // BulkLoadToNeptune options
    @Option(name = {"--bucket-name"}, description = "S3 bucket name for CSV files to be stored")
    @Required
    @Once
    private String bucketName;

    @Option(name = {"--s3-prefix"}, description = "S3 key prefix for uploaded file (default: neptune)")
    @Once
    private String s3Prefix = "neptune";

    @Option(name = {"--region"}, description = "AWS region (default: us-east-2)")
    @Once
    private String region = "us-east-2";

    @Option(name = {"--neptune-endpoint"}, description =
        "Neptune cluster endpoint. Example: my-neptune-cluster.cluster-abc123.<region>.neptune.amazonaws.com")
    @Required
    @Once
    private String neptuneEndpoint;

    @Option(name = {"--iam-role-arn"}, description = "IAM role ARN for Neptune bulk loading")
    @Required
    @Once
    private String iamRoleArn;

    @Option(name = {"--monitor"}, description = "Monitor Neptune bulk load progress until completion (default: true)")
    @Once
    private boolean monitor = true;

    @Override
    public void run() {
        try {
            Directories directories = Directories.createFor(outputDirectory);

            try (Timer timer = new Timer();
                 OutputFile vertexFile = new OutputFile(directories, "vertices");
                 OutputFile edgeFile = new OutputFile(directories, "edges");
                 CSVParser parser = CSVUtils.newParser(inputFile)) {

                Iterator<CSVRecord> iterator = parser.iterator();

                long vertexCount = 0;
                long edgeCount = 0;

                if (iterator.hasNext()) {
                    CSVRecord headers = iterator.next();

                    VertexMetadata vertexMetadata = VertexMetadata.parse(
                            headers,
                            new PropertyValueParser(multiValuedNodePropertyPolicy, semiColonReplacement, inferTypes));
                    EdgeMetadata edgeMetadata = EdgeMetadata.parse(
                            headers,
                            new PropertyValueParser(multiValuedRelationshipPropertyPolicy, semiColonReplacement, inferTypes));

                    while (iterator.hasNext()) {
                        CSVRecord record = iterator.next();
                        if (vertexMetadata.isVertex(record)) {
                            vertexFile.printRecord(vertexMetadata.toIterable(record));
                            vertexCount++;
                        } else if (edgeMetadata.isEdge(record)) {
                            edgeFile.printRecord(edgeMetadata.toIterable(record));
                            edgeCount++;
                        } else {
                            throw new IllegalStateException("Unable to parse record: " + record.toString());
                        }
                    }

                    vertexFile.printHeaders(vertexMetadata.headers());
                    edgeFile.printHeaders(edgeMetadata.headers());

                    System.err.println("Vertices: " + vertexCount);
                    System.err.println("Edges   : " + edgeCount);
                    System.err.println("Output  : " + directories.outputDirectory());
                    System.out.println(directories.outputDirectory());
                }
            }

            try (NeptuneBulkLoader neptuneBulkLoader = new NeptuneBulkLoader(
                    bucketName,
                    s3Prefix,
                    Region.of(region),
                    neptuneEndpoint,
                    iamRoleArn)) {

                String uri = directories.outputDirectory().toFile().getAbsolutePath();

                neptuneBulkLoader.uploadCsvFilesToS3(uri);
                String trimmedBucketName = bucketName.replaceAll("/+$", "");
                String trimmedS3Prefix = s3Prefix.replaceAll("/+$", "");
                String s3SourceUri = "s3://" + trimmedBucketName + "/" + trimmedS3Prefix;
                String loadId = neptuneBulkLoader.startNeptuneBulkLoad(s3SourceUri);


                if (loadId == null) {
                    System.err.println("Failed to start Neptune bulk load! Load ID: " + loadId);
                    System.exit(1);
                }

                System.out.println("Neptune bulk load started successfully! Load ID: " + loadId);
                if (monitor) {
                    neptuneBulkLoader.monitorLoadProgress(loadId);
                }
            }
        } catch (Exception e) {
            System.err.println("Error during convert and bulk load process: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}
