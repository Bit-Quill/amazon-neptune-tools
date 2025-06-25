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

package com.amazonaws.services.neptune.metadata;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringReader;

import static org.junit.Assert.*;

public class RecordFilterTest {

    @Test
    public void testVertexSkippingById() throws IOException {
        // Create a temporary YAML file with vertex ID skip rules
        File tempFile = File.createTempFile("test-skip", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
            writer.write("    - \"456\"\n");
        }
        
        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        RecordFilter filter = new RecordFilter(config);
        
        // Create mock vertex metadata
        String headerLine = "_id,_labels,name";
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        VertexMetadata vertexMetadata = VertexMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, " ", false));
        
        // Test vertex records
        String vertexData = "123,Person,John\n456,Company,Acme\n789,Person,Jane";
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(vertexData));
        
        int recordIndex = 0;
        for (CSVRecord record : records) {
            if (recordIndex == 0) {
                assertTrue("Vertex 123 should be skipped", filter.shouldSkipVertex(record));
            } else if (recordIndex == 1) {
                assertTrue("Vertex 456 should be skipped", filter.shouldSkipVertex(record));
            } else if (recordIndex == 2) {
                assertFalse("Vertex 789 should not be skipped", filter.shouldSkipVertex(record));
            }
            recordIndex++;
        }
        
        assertEquals(2, filter.getSkippedVertexIds().size());
        assertTrue(filter.getSkippedVertexIds().contains("123"));
        assertTrue(filter.getSkippedVertexIds().contains("456"));
    }
    
    @Test
    public void testVertexSkippingByLabel() throws IOException {
        // Create a temporary YAML file with vertex label skip rules
        File tempFile = File.createTempFile("test-skip-label", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
            writer.write("    - \"Deprecated\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        RecordFilter filter = new RecordFilter(config);
        
        // Create mock vertex metadata
        String headerLine = "_id,_labels,name";
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        VertexMetadata vertexMetadata = VertexMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, " ", false));
        
        // Test vertex records with different labels
        String vertexData = "1,Person,John\n2,TestData,Test\n3,Person:Deprecated,Old\n4,Company,Acme";
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(vertexData));
        
        int recordIndex = 0;
        for (CSVRecord record : records) {
            if (recordIndex == 0) {
                assertFalse("Person vertex should not be skipped", filter.shouldSkipVertex(record));
            } else if (recordIndex == 1) {
                assertTrue("TestData vertex should be skipped", filter.shouldSkipVertex(record));
            } else if (recordIndex == 2) {
                assertTrue("Deprecated vertex should be skipped", filter.shouldSkipVertex(record));
            } else if (recordIndex == 3) {
                assertFalse("Company vertex should not be skipped", filter.shouldSkipVertex(record));
            }
            recordIndex++;
        }
    }
    
    @Test
    public void testEdgeSkippingByConnectedVertices() throws IOException {
        // Create a temporary YAML file with vertex skip rules
        File tempFile = File.createTempFile("test-edge-skip", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"123\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        RecordFilter filter = new RecordFilter(config);
        
        // Create mock metadata
        String headerLine = "_id,_labels,name,_start,_end,_type";
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        VertexMetadata vertexMetadata = VertexMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, " ", false));
        EdgeMetadata edgeMetadata = EdgeMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, " ", false));
        
        // First, skip vertex 123
        String vertexData = "123,Person,John";
        CSVRecord vertexRecord = CSVFormat.DEFAULT.parse(new StringReader(vertexData)).iterator().next();
        filter.shouldSkipVertex(vertexRecord);
        
        // Test edge records
        String edgeData = ",,,456,789,KNOWS\n,,,123,789,WORKS_FOR\n,,,456,123,MANAGES";
        Iterable<CSVRecord> edgeRecords = CSVFormat.DEFAULT.parse(new StringReader(edgeData));
        
        int recordIndex = 0;
        for (CSVRecord record : edgeRecords) {
            if (recordIndex == 0) {
                assertFalse("Edge between 456-789 should not be skipped", filter.shouldSkipEdge(record, edgeMetadata));
            } else if (recordIndex == 1) {
                assertTrue("Edge from 123-789 should be skipped (123 is skipped vertex)", filter.shouldSkipEdge(record, edgeMetadata));
            } else if (recordIndex == 2) {
                assertTrue("Edge from 456-123 should be skipped (123 is skipped vertex)", filter.shouldSkipEdge(record, edgeMetadata));
            }
            recordIndex++;
        }
    }
    
    @Test
    public void testEdgeSkippingByLabel() throws IOException {
        // Create a temporary YAML file with edge label skip rules
        File tempFile = File.createTempFile("test-edge-label-skip", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipEdges:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TEMP_RELATIONSHIP\"\n");
            writer.write("    - \"DEBUG_LINK\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        RecordFilter filter = new RecordFilter(config);
        
        // Create mock edge metadata
        String headerLine = "_id,_labels,name,_start,_end,_type";
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        EdgeMetadata edgeMetadata = EdgeMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, " ", false));
        
        // Test edge records with different types
        String edgeData = ",,,1,2,KNOWS\n,,,2,3,TEMP_RELATIONSHIP\n,,,3,4,DEBUG_LINK\n,,,4,5,WORKS_FOR";
        Iterable<CSVRecord> records = CSVFormat.DEFAULT.parse(new StringReader(edgeData));
        
        int recordIndex = 0;
        for (CSVRecord record : records) {
            if (recordIndex == 0) {
                assertFalse("KNOWS edge should not be skipped", filter.shouldSkipEdge(record, edgeMetadata));
            } else if (recordIndex == 1) {
                assertTrue("TEMP_RELATIONSHIP edge should be skipped", filter.shouldSkipEdge(record, edgeMetadata));
            } else if (recordIndex == 2) {
                assertTrue("DEBUG_LINK edge should be skipped", filter.shouldSkipEdge(record, edgeMetadata));
            } else if (recordIndex == 3) {
                assertFalse("WORKS_FOR edge should not be skipped", filter.shouldSkipEdge(record, edgeMetadata));
            }
            recordIndex++;
        }
    }
    
    @Test
    public void testNoSkipRules() throws IOException {
        ConversionConfig config = ConversionConfig.fromFile(null);
        RecordFilter filter = new RecordFilter(config);
        
        assertFalse(filter.hasSkipRules());
        assertEquals("No skip rules configured", filter.getSkipStatistics());
        
        // Create mock metadata and records
        String headerLine = "_id,_labels,name,_start,_end,_type";
        CSVRecord headers = CSVFormat.DEFAULT.parse(new StringReader(headerLine)).iterator().next();
        VertexMetadata vertexMetadata = VertexMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedNodePropertyPolicy.PutInSetIgnoringDuplicates, " ", false));
        EdgeMetadata edgeMetadata = EdgeMetadata.parse(headers, 
            new PropertyValueParser(MultiValuedRelationshipPropertyPolicy.LeaveAsString, " ", false));
        
        // Test that nothing is skipped when no rules are configured
        String vertexData = "123,Person,John";
        CSVRecord vertexRecord = CSVFormat.DEFAULT.parse(new StringReader(vertexData)).iterator().next();
        assertFalse(filter.shouldSkipVertex(vertexRecord));
        
        String edgeData = ",,,,1,2,KNOWS";
        CSVRecord edgeRecord = CSVFormat.DEFAULT.parse(new StringReader(edgeData)).iterator().next();
        assertFalse(filter.shouldSkipEdge(edgeRecord, edgeMetadata));
    }
    
    @Test
    public void testSkipStatistics() throws IOException {
        // Create a comprehensive YAML file
        File tempFile = File.createTempFile("test-stats", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("skipVertices:\n");
            writer.write("  byId:\n");
            writer.write("    - \"1\"\n");
            writer.write("    - \"2\"\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TestData\"\n");
            writer.write("skipEdges:\n");
            writer.write("  byLabel:\n");
            writer.write("    - \"TEMP\"\n");
        }

        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        RecordFilter filter = new RecordFilter(config);
        
        assertTrue(filter.hasSkipRules());
        String stats = filter.getSkipStatistics();
        assertTrue(stats.contains("2 vertex IDs"));
        assertTrue(stats.contains("1 vertex labels"));
        assertTrue(stats.contains("1 edge labels"));
    }
}
