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

import org.apache.commons.csv.CSVRecord;

import java.util.HashSet;
import java.util.Set;

/**
 * Utility class for filtering vertices and edges based on configuration rules.
 * Handles skipping of vertices and edges by ID or label, and automatically
 * skips edges connected to skipped vertices.
 */
public class RecordFilter {
    
    private final Set<String> skipVertexIds;
    private final Set<String> skipVertexLabels;
    private final Set<String> skipEdgeLabels;
    private final Set<String> skippedVertexIds;
    
    public RecordFilter(ConversionConfig config) {
        this.skipVertexIds = config.getSkipVertices().getById();
        this.skipVertexLabels = config.getSkipVertices().getByLabel();
        this.skipEdgeLabels = config.getSkipEdges().getByLabel();
        this.skippedVertexIds = new HashSet<>();
    }
    
    /**
     * Determines if a vertex should be skipped based on its ID or labels.
     * Also tracks skipped vertex IDs for edge filtering.
     * 
     * @param record The CSV record representing the vertex
     * @return true if the vertex should be skipped, false otherwise
     */
    public boolean shouldSkipVertex(CSVRecord record) {
        if (!hasVertexSkipRules()) {
            return false;
        }
        
        String vertexId = record.get(0); // _id is always the first column
        
        // Check if vertex ID should be skipped
        if (!skipVertexIds.isEmpty() && skipVertexIds.contains(vertexId)) {
            skippedVertexIds.add(vertexId);
            return true;
        }

        // Only retrieve and check labels if we have label-based skip rules
        if (!skipVertexLabels.isEmpty()) {
            String vertexLabels = getVertexLabels(record);

            if (vertexLabels != null && !vertexLabels.isEmpty()) {
                String[] labels = vertexLabels.split(":");
                for (String label : labels) {
                    String trimmedLabel = label.trim();
                    if (!trimmedLabel.isEmpty() && skipVertexLabels.contains(trimmedLabel)) {
                        skippedVertexIds.add(vertexId);
                        return true;
                    }
                }
            }
        }
        
        return false;
    }
    
    /**
     * Determines if an edge should be skipped based on its ID, label, or connected vertices.
     * 
     * @param record The CSV record representing the edge
     * @param edgeMetadata Metadata for parsing edge information
     * @return true if the edge should be skipped, false otherwise
     */
    public boolean shouldSkipEdge(CSVRecord record, EdgeMetadata edgeMetadata) {
        // An edge might still be skipped if its connected vertex is skipped
        if (!hasSkipRules()) {
            return false;
        }
        
        int firstColumnIndex = edgeMetadata.firstColumnIndex();
        
        // Make sure we have enough columns for edge data
        if (record.size() <= firstColumnIndex + 2) {
            return false; // Not enough data to be a valid edge
        }
        
        // The actual edge data starts at firstColumnIndex
        String startVertexId = record.get(firstColumnIndex);     // _start
        String endVertexId = record.get(firstColumnIndex + 1);   // _end
        String edgeType = record.get(firstColumnIndex + 2);      // _type

        // Skip edge if either connected vertex was skipped
        if (skippedVertexIds.contains(startVertexId) || skippedVertexIds.contains(endVertexId)) {
            return true;
        }
        
        // Check if edge type/label should be skipped
        if (edgeType != null && !edgeType.trim().isEmpty() && skipEdgeLabels.contains(edgeType.trim())) {
            return true;
        }
        
        // If needed, this could be extended to support edge property-based filtering.
        
        return false;
    }
    
    /**
     * Gets the vertex labels from a CSV record.
     */
    private String getVertexLabels(CSVRecord record) {
        // In Neo4j CSV exports, _labels is typically at index 1 (after _id at index 0)
        if (record.size() > 1) {
            return record.get(1);
        }
        return null;
    }
    
    /**
     * Returns the set of vertex IDs that were skipped during processing.
     * This is useful for reporting and debugging.
     */
    public Set<String> getSkippedVertexIds() {
        return new HashSet<>(skippedVertexIds);
    }

    /**
     * Checks if any skip rules are configured.
     */
    public boolean hasSkipRules() {
        return !skipVertexIds.isEmpty() || !skipVertexLabels.isEmpty() || !skipEdgeLabels.isEmpty();
    }

    /**
     * Checks if vertex-specific skip rules are configured.
     * Used for early optimization in vertex filtering.
     */
    private boolean hasVertexSkipRules() {
        return !skipVertexIds.isEmpty() || !skipVertexLabels.isEmpty();
    }
    
    /**
     * Returns statistics about skip rules.
     */
    public String getSkipStatistics() {
        if (!hasSkipRules()) {
            return "No skip rules configured";
        }
        
        StringBuilder stats = new StringBuilder();
        stats.append("Skip rules: ");
        
        if (!skipVertexIds.isEmpty()) {
            stats.append(skipVertexIds.size()).append(" vertex IDs, ");
        }
        if (!skipVertexLabels.isEmpty()) {
            stats.append(skipVertexLabels.size()).append(" vertex labels, ");
        }
        if (!skipEdgeLabels.isEmpty()) {
            stats.append(skipEdgeLabels.size()).append(" edge labels, ");
        }
        
        // Remove trailing comma and space
        if (stats.toString().endsWith(", ")) {
            stats.setLength(stats.length() - 2);
        }
        
        return stats.toString();
    }
}
