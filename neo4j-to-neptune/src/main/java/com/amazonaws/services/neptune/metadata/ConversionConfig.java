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

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Configuration class for label mapping and filtering from YAML file.
 * <p>
 * Expected YAML format:
 * vertex_labels:
 *   OldVertexLabel: NewVertexLabel
 *   AnotherOldLabel: AnotherNewLabel
 * edge_labels:
 *   OLD_EDGE_TYPE: NEW_EDGE_TYPE
 *   ANOTHER_OLD_TYPE: ANOTHER_NEW_TYPE
 * skip_vertices:
 *   by_id:
 *     - "vertex_id_1"
 *     - "vertex_id_2"
 *   by_label:
 *     - "LabelToSkip"
 *     - "AnotherLabelToSkip"
 * skip_edges:
 *   by_label:
 *     - "RELATIONSHIP_TYPE_TO_SKIP"
 *     - "ANOTHER_TYPE_TO_SKIP"
 */
public class ConversionConfig {

    private Map<String, String> vertexLabels = new HashMap<>();
    private Map<String, String> edgeLabels = new HashMap<>();
    private Set<String> skipVertexIds = new HashSet<>();
    private Set<String> skipVertexLabels = new HashSet<>();
    private Set<String> skipEdgeLabels = new HashSet<>();

    public static ConversionConfig fromFile(File yamlFile) throws IOException {
        if (yamlFile == null || !yamlFile.exists()) {
            return new ConversionConfig(); // Return empty config if no file provided
        }

        Yaml yaml = new Yaml();
        try (FileInputStream inputStream = new FileInputStream(yamlFile)) {
            Map<String, Object> data = yaml.load(inputStream);

            ConversionConfig config = new ConversionConfig();

            if (data != null) {
                // Load vertex label mappings
                Object vertexLabelsObj = data.get("vertex_labels");
                if (vertexLabelsObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> vertexMappings = (Map<String, String>) vertexLabelsObj;
                    config.vertexLabels.putAll(vertexMappings);
                }

                // Load edge label mappings
                Object edgeLabelsObj = data.get("edge_labels");
                if (edgeLabelsObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> edgeMappings = (Map<String, String>) edgeLabelsObj;
                    config.edgeLabels.putAll(edgeMappings);
                }

                // Load skip vertex configuration
                Object skipVerticesObj = data.get("skip_vertices");
                if (skipVerticesObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> skipVertices = (Map<String, Object>) skipVerticesObj;

                    // Load vertex IDs to skip
                    Object skipVertexIdsObj = skipVertices.get("by_id");
                    if (skipVertexIdsObj instanceof List) {
                        // YAML file allows numerical IDs, cast and treat as strings
                        List<String> vertexIds = ((List<?>) skipVertexIdsObj).stream().map(Object::toString).collect(Collectors.toList());
                        config.skipVertexIds.addAll(vertexIds);
                    }

                    // Load vertex labels to skip
                    Object skipVertexLabelsObj = skipVertices.get("by_label");
                    if (skipVertexLabelsObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> vertexLabels = (List<String>) skipVertexLabelsObj;
                        config.skipVertexLabels.addAll(vertexLabels);
                    }
                }

                // Load skip edge configuration
                Object skipEdgesObj = data.get("skip_edges");
                if (skipEdgesObj instanceof Map) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> skipEdges = (Map<String, Object>) skipEdgesObj;

                    // Load edge labels to skip
                    Object skipEdgeLabelsObj = skipEdges.get("by_label");
                    if (skipEdgeLabelsObj instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<String> edgeLabels = (List<String>) skipEdgeLabelsObj;
                        config.skipEdgeLabels.addAll(edgeLabels);
                    }
                }
            }

            return config;
        }
    }

    public Map<String, String> getVertexLabels() {
        return vertexLabels;
    }

    public Map<String, String> getEdgeLabels() {
        return edgeLabels;
    }

    public Set<String> getSkipVertexIds() {
        return skipVertexIds;
    }

    public Set<String> getSkipVertexLabels() {
        return skipVertexLabels;
    }

    public Set<String> getSkipEdgeLabels() {
        return skipEdgeLabels;
    }

    public boolean hasVertexMappings() {
        return !vertexLabels.isEmpty();
    }

    public boolean hasEdgeMappings() {
        return !edgeLabels.isEmpty();
    }

    public boolean hasSkipRules() {
        return !skipVertexIds.isEmpty() || !skipVertexLabels.isEmpty() || !skipEdgeLabels.isEmpty();
    }
}
