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

import com.amazonaws.services.neptune.TestDataProvider;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.core.async.AsyncRequestBody;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CompletableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.*;

public class NeptuneBulkLoaderTest {
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private ByteArrayOutputStream outputStream;
    private ByteArrayOutputStream errorStream;
    private PrintStream originalOut;
    private PrintStream originalErr;

    @Before
    public void setUp() throws Exception {
        // Capture System.out and System.err
        originalOut = System.out;
        originalErr = System.err;
        outputStream = new ByteArrayOutputStream();
        errorStream = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outputStream));
        System.setErr(new PrintStream(errorStream));
    }

    @Test
    public void testConstructorWithValidParameters() {
        // Test valid constructor parameters
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Verify constructor output messages
        String output = outputStream.toString();
        assertTrue("Should contain bucket name", output.contains("S3 Bucket: " + TestDataProvider.BUCKET));
        assertTrue("Should contain S3 prefix", output.contains("S3 Prefix: " + TestDataProvider.S3_PREFIX));
        assertTrue("Should contain region", output.contains("AWS Region: " + TestDataProvider.REGION_US_EAST_2));
        assertTrue("Should contain Neptune endpoint", output.contains("Neptune Endpoint: " + TestDataProvider.NEPTUNE_ENDPOINT));
        assertTrue("Should contain IAM role ARN", output.contains("IAM Role ARN: " + TestDataProvider.IAM_ROLE_ARN));
        assertNotNull("NeptuneBulkLoader should be created", neptuneBulkLoader);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithInvalidParameters() {
        Object[][] invalidParameters = {
            {"Empty bucket name", "", TestDataProvider.S3_PREFIX, TestDataProvider.REGION_US_EAST_2, TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.IAM_ROLE_ARN},
            {"Empty S3 prefix", TestDataProvider.BUCKET, "", TestDataProvider.REGION_US_EAST_2, TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.IAM_ROLE_ARN},
            {"Empty Neptune endpoint", TestDataProvider.BUCKET, TestDataProvider.S3_PREFIX, TestDataProvider.REGION_US_EAST_2, "", TestDataProvider.IAM_ROLE_ARN},
            {"Empty IAM role ARN", TestDataProvider.BUCKET, TestDataProvider.S3_PREFIX, TestDataProvider.REGION_US_EAST_2, TestDataProvider.NEPTUNE_ENDPOINT, ""},
            {"Whitespace bucket name", " ", TestDataProvider.S3_PREFIX, TestDataProvider.REGION_US_EAST_2, TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.IAM_ROLE_ARN},
            {"Whitespace S3 prefix", TestDataProvider.BUCKET, " ", TestDataProvider.REGION_US_EAST_2, TestDataProvider.NEPTUNE_ENDPOINT, TestDataProvider.IAM_ROLE_ARN}
        };

        for (Object[] params : invalidParameters) {
            NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader(
                (String) params[1], (String) params[2], (Region) params[3],
                (String) params[4], (String) params[5]);
            assertNull("NeptuneBulkLoader should fail creation with " + params[0], neptuneBulkLoader);
        }
    }

    @Test
    public void testUploadFileAsyncS3Success() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testVerticiesFile = new File(testDir, TestDataProvider.VERTICIES_CSV);
        TestDataProvider.createMockVerticesFile(testDir, testVerticiesFile);

        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock the S3AsyncClient
        S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);

        // Create a successful PutObjectResponse
        PutObjectResponse putObjectResponse = PutObjectResponse.builder()
            .eTag("mock-etag-12345")
            .build();

        // Create a CompletableFuture that completes successfully
        CompletableFuture<PutObjectResponse> successFuture = CompletableFuture.completedFuture(putObjectResponse);

        // Mock the putObject method to return the successful future
        when(mockS3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(successFuture);

        // Use reflection to replace the s3AsyncClient field with our mock
        Field s3AsyncClientField = NeptuneBulkLoader.class.getDeclaredField("s3AsyncClient");
        s3AsyncClientField.setAccessible(true);
        s3AsyncClientField.set(neptuneBulkLoader, mockS3AsyncClient);

        try {
            CompletableFuture<Boolean> result = neptuneBulkLoader.uploadFileAsync(
                testVerticiesFile.getAbsolutePath(),
                TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES
            );

            // The method should return a CompletableFuture
            assertNotNull("uploadFileAsync should return a CompletableFuture", result);

            // Wait for the result and verify it's true
            Boolean uploadResult = result.get();
            assertTrue("Upload should be successful", uploadResult);

            // Verify that the S3AsyncClient.putObject was called
            verify(mockS3AsyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

            // Verify the output contains success message
            String output = outputStream.toString();
            assertTrue("Should contain upload attempt message",
                output.contains("Starting async upload"));

        } catch (Exception e) {
            fail("Should not throw exception when S3AsyncClient is mocked successfully: " + e.getMessage());
        }
    }

    @Test
    public void testUploadFileAsyncS3Exception() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        File testVerticiesFile = new File(testDir, TestDataProvider.VERTICIES_CSV);
        TestDataProvider.createMockVerticesFile(testDir, testVerticiesFile);

        // Create a spy of NeptuneBulkLoader to mock the S3 exception
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        String testS3Key = TestDataProvider.S3_KEY_FOR_UPLOAD_FILE_ASYNC_VERTICES;

        // Mock the S3AsyncClient
        S3AsyncClient mockS3AsyncClient = mock(S3AsyncClient.class);

        // Mock S3Exception to be thrown
        S3Exception s3Exception = (S3Exception) S3Exception.builder()
            .message("Access Denied")
            .statusCode(403)
            .build();

        // Create a CompletableFuture that completes exceptionally with S3Exception
        CompletableFuture<PutObjectResponse> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(s3Exception);

        // Mock the putObject method to return the failed future
        when(mockS3AsyncClient.putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class)))
            .thenReturn(failedFuture);

        // Use reflection to replace the s3AsyncClient field with our mock
        Field s3AsyncClientField = NeptuneBulkLoader.class.getDeclaredField("s3AsyncClient");
        s3AsyncClientField.setAccessible(true);
        s3AsyncClientField.set(neptuneBulkLoader, mockS3AsyncClient);

        try {
            CompletableFuture<Boolean> result = neptuneBulkLoader.uploadFileAsync(
                testVerticiesFile.getAbsolutePath(),
                testS3Key
            );

            // Wait for the future to complete and expect it to return false due to exception
            Boolean uploadResult = result.get();
            assertFalse("Upload should fail and return false when S3Exception occurs", uploadResult);

            // Verify that the S3AsyncClient.putObject was called
            verify(mockS3AsyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));

            // Verify the output contains upload attempt message
            String output = outputStream.toString();
            assertTrue("Should contain upload attempt message",
                output.contains("Starting async upload"));

            // Verify error stream contains the exception details
            String error = errorStream.toString();
            assertTrue("Should contain error message about upload failure",
                error.contains("Error uploading file") || error.contains("Access Denied"));

        } catch (Exception e) {
            // If an exception is thrown instead of returning false, verify it's the S3Exception
            assertTrue("Should contain S3Exception in the cause chain",
                e.getCause() instanceof S3Exception || e instanceof S3Exception);

            // Verify the exception message
            String exceptionMessage = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
            assertTrue("Should contain Access Denied message",
                exceptionMessage.contains("Access Denied"));

            // Verify that the S3AsyncClient.putObject was called
            verify(mockS3AsyncClient, times(1)).putObject(any(PutObjectRequest.class), any(AsyncRequestBody.class));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadFileAsyncWithNonExistentDirectory() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadFileAsync("/non/existent/file.csv", TestDataProvider.S3_KEY);
    }

    @Test(expected = IllegalStateException.class)
    public void testUploadFileAsyncWithNonExistentFile() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);

        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        neptuneBulkLoader.uploadFileAsync(testDir.getAbsolutePath(), TestDataProvider.S3_KEY);
    }

    @Test
    public void testUploadCsvFilesToS3WithBothFilesSuccess() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock both uploadFileAsync calls to return successful futures
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);
        doReturn(successFuture).when(spyLoader).uploadFileAsync(anyString(), anyString());

        // Test upload
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());

        // Verify both files were uploaded (vertices.csv and edges.csv)
        verify(spyLoader, times(2)).uploadFileAsync(anyString(), anyString());

        // Verify specific file paths were called
        verify(spyLoader).uploadFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.VERTICIES_CSV),
            contains(TestDataProvider.VERTICIES_CSV)
        );
        verify(spyLoader).uploadFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.EDGES_CSV),
            contains(TestDataProvider.EDGES_CSV)
        );

        // Verify output messages
        String output = outputStream.toString();
        assertTrue("Should contain initial upload message",
            output.contains("Uploading Gremlin CSV to S3..."));
        assertTrue("Should contain success message",
            output.contains("CSV files uploaded successfully to S3. Files available at"));
        assertTrue("Should contain S3 bucket name",
            output.contains(TestDataProvider.BUCKET));
        assertTrue("Should contain timestamp",
            output.contains(testDir.getName()));
    }

    @Test(expected = RuntimeException.class)
    public void testUploadCsvFilesToS3WithVerticesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock vertices upload to fail, edges to succeed
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);

        doReturn(failureFuture).when(spyLoader).uploadFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.VERTICIES_CSV),
            anyString()
        );
        doReturn(successFuture).when(spyLoader).uploadFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.EDGES_CSV),
            anyString()
        );

        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain upload message",
            error.contains("Upload failures - Vertices: 1, Edges: 0"));
    }

    @Test(expected = RuntimeException.class)
    public void testUploadCsvFilesToS3WithEdgesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock vertices upload to succeed, edges to fail
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);

        doReturn(successFuture).when(spyLoader).uploadFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.VERTICIES_CSV),
            anyString()
        );
        doReturn(failureFuture).when(spyLoader).uploadFileAsync(
            eq(testDir.getAbsolutePath() + File.separator + TestDataProvider.EDGES_CSV),
            anyString()
        );

        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain upload message",
            error.contains("Upload failures - Vertices: 0, Edges: 1"));
    }

    @Test(expected = RuntimeException.class)
    public void testUploadCsvFilesToS3WithBothFilesFailure() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock both uploads to fail
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);
        doReturn(failureFuture).when(spyLoader).uploadFileAsync(anyString(), anyString());

        // Test upload - should throw RuntimeException
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
        String error = errorStream.toString();
        assertTrue("Should contain upload message",
            error.contains("Upload failures - Vertices: 1, Edges: 1"));
    }

    @Test
    public void testUploadCsvFilesToS3WithException() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock uploadFileAsync to throw an exception
        CompletableFuture<Boolean> exceptionFuture = new CompletableFuture<>();
        exceptionFuture.completeExceptionally(new RuntimeException("S3 connection failed"));
        doReturn(exceptionFuture).when(spyLoader).uploadFileAsync(anyString(), anyString());

        try {
            spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
            fail("Expected exception to be thrown");
        } catch (Exception e) {
            // Verify the exception is properly propagated
            assertTrue("Should contain connection error",
                e.getMessage().contains("S3 connection failed") ||
                e.getCause().getMessage().contains("S3 connection failed"));
        }
    }

    @Test
    public void testUploadCsvFilesToS3S3KeyConstruction() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);
        doReturn(successFuture).when(spyLoader).uploadFileAsync(anyString(), anyString());

        // Test upload
        spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());

        // Extract timestamp from directory name
        String expectedTimestamp = testDir.getName();
        String expectedS3KeyPrefix = TestDataProvider.S3_PREFIX + File.separator + expectedTimestamp;

        // Verify S3 keys are constructed correctly
        verify(spyLoader).uploadFileAsync(
            anyString(),
            eq(expectedS3KeyPrefix + "/" + TestDataProvider.VERTICIES_CSV)
        );
        verify(spyLoader).uploadFileAsync(
            anyString(),
            eq(expectedS3KeyPrefix + "/" + TestDataProvider.EDGES_CSV)
        );
    }

    @Test
    public void testUploadCsvFilesToS3ErrorMessages() throws Exception {
        File testDir = tempFolder.newFolder(TestDataProvider.TEMP_FOLDER_NAME);
        TestDataProvider.createMockCsvFiles(testDir);

        NeptuneBulkLoader spyLoader = spy(TestDataProvider.createNeptuneBulkLoader());

        // Mock vertices to fail, edges to succeed
        CompletableFuture<Boolean> failureFuture = CompletableFuture.completedFuture(false);
        CompletableFuture<Boolean> successFuture = CompletableFuture.completedFuture(true);

        doReturn(failureFuture).when(spyLoader).uploadFileAsync(
            contains("vertices.csv"), anyString()
        );
        doReturn(successFuture).when(spyLoader).uploadFileAsync(
            contains("edges.csv"), anyString()
        );

        try {
            spyLoader.uploadCsvFilesToS3(testDir.getAbsolutePath());
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Verify error messages
            String error = errorStream.toString();
            assertTrue("Should contain upload failure message",
                error.contains("Upload failures - Vertices: false, Edges: true"));

            assertTrue("Should contain runtime exception message",
                e.getMessage().contains("One or more CSV uploads failed"));
        }
    }

    @Test
    public void testNeptuneConnectivitySuccess() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock successful response
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"healthy\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Use reflection to replace the httpClient field
        Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(neptuneBulkLoader, mockHttpClient);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertTrue("Neptune connectivity should return true for healthy status", result);

        // Verify output messages
        String output = outputStream.toString();
        assertTrue("Should contain connectivity test message",
            output.contains("Testing connectivity to Neptune endpoint..."));
        assertTrue("Should contain success message",
            output.contains("Successful connected to Neptune. Status: 200 healthy"));
    }

    @Test
    public void testNeptuneConnectivityUnhealthyStatus() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock response with unhealthy status
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"unhealthy\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Use reflection to replace the httpClient field
        Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(neptuneBulkLoader, mockHttpClient);

        // Test connectivity - the RuntimeException is caught and method returns false
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false for unhealthy status", result);

        // Verify error message in stderr
        String error = errorStream.toString();
        assertTrue("Final error message for unhealthy status should be present",
            error.contains("Neptune connectivity test failed"));
    }

    @Test
    public void testNeptuneConnectivityMissingStatus() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock response without status field
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"message\":\"no status field\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Use reflection to replace the httpClient field
        Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(neptuneBulkLoader, mockHttpClient);

        // Test connectivity - the RuntimeException is caught and method returns false
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false for missing status", result);

        // Verify error message in stderr
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test failed message",
            error.contains("Neptune connectivity test failed"));
    }

    @Test
    public void testNeptuneConnectivityNon200StatusCode() throws Exception {
        Object[][] invalidStatusCodes = {
            {"400", 400}, {"401", 401}, {"403", 403}, {"404", 404}, {"405", 405}, {"406", 406},
            {"408", 408}, {"413", 413}, {"414", 414}, {"415", 415}, {"416", 416}, {"418", 418},
            {"429", 429}, {"500", 500}, {"502", 502}, {"503", 503}, {"504", 504}, {"509", 509}
        };
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        for (Object[] params : invalidStatusCodes) {
            // Mock invalid response
            when(mockResponse.statusCode()).thenReturn((Integer) params[1]);
            when(mockResponse.body()).thenReturn("Not Found");
            when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
                .thenReturn(mockResponse);

            // Use reflection to replace the httpClient field
            Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
            httpClientField.setAccessible(true);
            httpClientField.set(neptuneBulkLoader, mockHttpClient);

            // Test connectivity
            boolean result = neptuneBulkLoader.testNeptuneConnectivity();

            assertFalse("Neptune connectivity should return false for non-200 status", result);

            // Verify error message
            String error = errorStream.toString();
            assertTrue("Should contain failed connection message",
                error.contains("Failed to connect to Neptune status endpoint. Status: " + params[0]));
        }
    }

    @Test
    public void testNeptuneConnectivityHttpException() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient to throw exception
        HttpClient mockHttpClient = mock(HttpClient.class);
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenThrow(new RuntimeException("Connection timeout"));

        // Use reflection to replace the httpClient field
        Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(neptuneBulkLoader, mockHttpClient);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false when exception occurs", result);

        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test failed message",
            error.contains("Neptune connectivity test failed: Connection timeout"));
    }

    @Test
    public void testNeptuneConnectivityJsonParsingException() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        // Mock response with invalid JSON
        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("invalid json response");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Use reflection to replace the httpClient field
        Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(neptuneBulkLoader, mockHttpClient);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertFalse("Neptune connectivity should return false when JSON parsing fails", result);

        // Verify error message
        String error = errorStream.toString();
        assertTrue("Should contain connectivity test failed message",
            error.contains("Neptune connectivity test failed"));
    }

    @Test
    public void testNeptuneConnectivityEndpointConstruction() throws Exception {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Mock HttpClient and HttpResponse
        HttpClient mockHttpClient = mock(HttpClient.class);
        HttpResponse<String> mockResponse = mock(HttpResponse.class);

        when(mockResponse.statusCode()).thenReturn(200);
        when(mockResponse.body()).thenReturn("{\"status\":\"healthy\"}");
        when(mockHttpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
            .thenReturn(mockResponse);

        // Use reflection to replace the httpClient field
        Field httpClientField = NeptuneBulkLoader.class.getDeclaredField("httpClient");
        httpClientField.setAccessible(true);
        httpClientField.set(neptuneBulkLoader, mockHttpClient);

        // Test connectivity
        boolean result = neptuneBulkLoader.testNeptuneConnectivity();

        assertTrue("Neptune connectivity should succeed with custom endpoint", result);

        // Verify that the correct endpoint was used by checking the HttpRequest
        verify(mockHttpClient).send(argThat(request -> {
            String expectedUrl = "https://" + TestDataProvider.NEPTUNE_ENDPOINT + ":8182/status";
            return request.uri().toString().equals(expectedUrl);
        }), any(HttpResponse.BodyHandler.class));
    }

    @Test
    public void testCloseMethod() {
        NeptuneBulkLoader neptuneBulkLoader = TestDataProvider.createNeptuneBulkLoader();

        // Test close method - should not throw exception
        neptuneBulkLoader.close();

        // Test multiple close calls - should be safe
        neptuneBulkLoader.close();
        neptuneBulkLoader.close();
    }

    @After
    public void tearDown() {
        System.setOut(originalOut);
        System.setErr(originalErr);
    }

    //TODO: checkNeptuneBulkLoadStatus, monitorLoadProgress, startNeptuneBulkLoad
}
