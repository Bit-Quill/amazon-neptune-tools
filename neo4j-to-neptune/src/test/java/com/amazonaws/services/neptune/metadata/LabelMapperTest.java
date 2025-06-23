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

import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import static org.junit.Assert.*;

public class LabelMapperTest {

    @Test
    public void testVertexLabelMapping() throws IOException {
        // Create a temporary YAML file
        File tempFile = File.createTempFile("test-mapping", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertex_labels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Company: Organization\n");
            writer.write("edge_labels:\n");
            writer.write("  WORKS_FOR: EMPLOYED_BY\n");
        }
        
        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        LabelMapper mapper = new LabelMapper(config);
        
        // Test vertex label mapping
        assertEquals("Individual", mapper.mapVertexLabels("Person"));
        assertEquals("Individual;Organization", mapper.mapVertexLabels("Person:Company"));
        assertEquals("Individual;Organization", mapper.mapVertexLabels(":Person:Company:"));
        assertEquals("UnmappedLabel", mapper.mapVertexLabels("UnmappedLabel"));
        assertEquals("Individual;UnmappedLabel", mapper.mapVertexLabels("Person:UnmappedLabel"));
        
        // Test edge label mapping
        assertEquals("EMPLOYED_BY", mapper.mapEdgeLabel("WORKS_FOR"));
        assertEquals("UNMAPPED_EDGE", mapper.mapEdgeLabel("UNMAPPED_EDGE"));
        
        // Test empty/null inputs
        assertEquals("", mapper.mapVertexLabels(""));
        assertNull(mapper.mapVertexLabels(null));
        assertEquals("", mapper.mapEdgeLabel(""));
        assertNull(mapper.mapEdgeLabel(null));
    }
    
    @Test
    public void testEmptyConfiguration() throws IOException {
        ConversionConfig config = ConversionConfig.fromFile(null);
        LabelMapper mapper = new LabelMapper(config);
        
        assertFalse(mapper.hasVertexMappings());
        assertFalse(mapper.hasEdgeMappings());
        
        // Should return original labels when no mappings exist
        assertEquals("Person;Company", mapper.mapVertexLabels("Person:Company"));
        assertEquals("WORKS_FOR", mapper.mapEdgeLabel("WORKS_FOR"));
    }
    
    @Test
    public void testConfigurationLoading() throws IOException {
        // Create a temporary YAML file
        File tempFile = File.createTempFile("test-config", ".yaml");
        tempFile.deleteOnExit();
        
        try (FileWriter writer = new FileWriter(tempFile)) {
            writer.write("vertex_labels:\n");
            writer.write("  Person: Individual\n");
            writer.write("  Company: Organization\n");
            writer.write("edge_labels:\n");
            writer.write("  WORKS_FOR: EMPLOYED_BY\n");
            writer.write("  KNOWS: CONNECTED_TO\n");
        }
        
        ConversionConfig config = ConversionConfig.fromFile(tempFile);
        
        assertTrue(config.hasVertexMappings());
        assertTrue(config.hasEdgeMappings());
        
        assertEquals(2, config.getVertexLabels().size());
        assertEquals(2, config.getEdgeLabels().size());
        
        assertEquals("Individual", config.getVertexLabels().get("Person"));
        assertEquals("Organization", config.getVertexLabels().get("Company"));
        assertEquals("EMPLOYED_BY", config.getEdgeLabels().get("WORKS_FOR"));
        assertEquals("CONNECTED_TO", config.getEdgeLabels().get("KNOWS"));
    }
}
