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

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class for mapping vertex and edge labels based on configuration.
 */
public class LabelMapper {
    
    private final Map<String, String> vertexLabelMappings;
    private final Map<String, String> edgeLabelMappings;
    
    public LabelMapper(ConversionConfig config) {
        this.vertexLabelMappings = config.getVertexLabels();
        this.edgeLabelMappings = config.getEdgeLabels();
    }
    
    /**
     * Maps vertex labels. Neo4j vertex labels are colon-separated and can have multiple labels.
     * Each individual label is mapped if a mapping exists, otherwise kept as-is.
     * 
     * @param originalLabels The original colon-separated vertex labels from Neo4j
     * @return The mapped labels, semicolon-separated for Neptune format
     */
    public String mapVertexLabels(String originalLabels) {
        if (originalLabels == null || originalLabels.trim().isEmpty()) {
            return originalLabels;
        }
        
        return Arrays.stream(originalLabels.split(":"))
                .filter(s -> !s.isEmpty())
                .map(label -> vertexLabelMappings.getOrDefault(label.trim(), label.trim()))
                .collect(Collectors.joining(";"));
    }
    
    /**
     * Maps edge labels. Neo4j edge types are single values.
     * 
     * @param originalLabel The original edge type from Neo4j
     * @return The mapped edge type, or original if no mapping exists
     */
    public String mapEdgeLabel(String originalLabel) {
        if (originalLabel == null || originalLabel.trim().isEmpty()) {
            return originalLabel;
        }
        
        return edgeLabelMappings.getOrDefault(originalLabel.trim(), originalLabel.trim());
    }
    
    /**
     * Check if any vertex label mappings are configured.
     */
    public boolean hasVertexMappings() {
        return !vertexLabelMappings.isEmpty();
    }
    
    /**
     * Check if any edge label mappings are configured.
     */
    public boolean hasEdgeMappings() {
        return !edgeLabelMappings.isEmpty();
    }
}
