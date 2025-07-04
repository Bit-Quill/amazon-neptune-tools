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

import java.io.File;
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

    @Option(name = {"-i", "--input"}, description = "Path to Neo4j CSV file")
    @Required
    @Path(mustExist = true, kind = PathKind.FILE)
    @Once
    private File inputFile;

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
    @Option(name = {"--bulk-load"}, description = "Enable bulk load to Neptune. If true, the output will be uploaded to S3 and loaded into Neptune using the bulk loader (default: false)")
    @Once
    private boolean bulkLoad = false;

    @Option(name = {"--bucket-name"}, description = "S3 bucket name for CSV files to be stored")
    @RequiredOnlyIf(names = "--bulk-load")
    @Once
    private String bucketName;

    @Option(name = {"--s3-prefix"}, description = "S3 prefix for uploaded file (default: neptune)")
    @Once
    private String s3Prefix = "neptune";

    @Option(name = {"--neptune-endpoint"}, description =
        "Neptune cluster endpoint. Example: my-neptune-cluster.cluster-abc123.<region>.neptune.amazonaws.com")
    @RequiredOnlyIf(names = "--bulk-load")
    @Once
    private String neptuneEndpoint;

    @Option(name = {"--iam-role-arn"}, description = "IAM role ARN for Neptune bulk loading")
    @RequiredOnlyIf(names = "--bulk-load")
    @Once
    private String iamRoleArn;

    @Option(name = {"--parallelism"}, description = "Parallelism level for Neptune bulk loading (default: MEDIUM)")
    @Once
    @AllowedValues(allowedValues = {"LOW", "MEDIUM", "HIGH", "OVERSUBSCRIBE"})
    private String parallelism = "MEDIUM";

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

            // Only perform bulk load operations if --bulk-load is enabled
            if (bulkLoad) {
                try (NeptuneBulkLoader neptuneBulkLoader = new NeptuneBulkLoader(
                        bucketName,
                        s3Prefix,
                        neptuneEndpoint,
                        iamRoleArn,
                        parallelism)) {

                    String uri = directories.outputDirectory().toFile().getAbsolutePath();
                    String s3SourceUri = neptuneBulkLoader.uploadCsvFilesToS3(uri);
                    String loadId = neptuneBulkLoader.startNeptuneBulkLoad(s3SourceUri);

                    if (monitor) {
                        neptuneBulkLoader.monitorLoadProgress(loadId);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("An error occurred while running convert-csv:");
            e.printStackTrace();
        }
    }
}
