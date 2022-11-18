/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
A copy of the License is located at
    http://www.apache.org/licenses/LICENSE-2.0
or in the "license" file accompanying this file. This file is distributed
on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
express or implied. See the License for the specific language governing
permissions and limitations under the License.
*/

package com.amazonaws.services.neptune.propertygraph.io;

import com.amazonaws.services.neptune.io.PrintOutputWriter;
import com.amazonaws.services.neptune.propertygraph.Label;
import com.amazonaws.services.neptune.propertygraph.schema.DataType;
import com.amazonaws.services.neptune.propertygraph.schema.LabelSchema;
import com.amazonaws.services.neptune.propertygraph.schema.PropertySchema;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.junit.Test;

import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class CsvPropertyGraphPrinterTest {

    @Test
    public void shouldUseSeparatorToSeparateMultipleValues() throws Exception {

        String separator = "|";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("X", "Y"));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().setMultiValueSeparator(separator).build()));

        printer.printProperties(props);

        assertEquals(
                "\"X|Y\"",
                stringWriter.toString());
    }

    @Test
    public void shouldEscapeSeparatorValuesInMultipleValues() throws Exception {

        String separator = "|";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("A|B", "Y"));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().setMultiValueSeparator(separator).build()));

        printer.printProperties(props);

        assertEquals(
                "\"A\\|B|Y\"",
                stringWriter.toString());
    }

    @Test
    public void shouldUseEmptySeparatorToSeparateMultipleValues() throws Exception {

        String separator = "";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, true);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Arrays.asList("X;B", "Y"));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().setMultiValueSeparator(separator).build()));

        printer.printProperties(props);

        assertEquals(
                "\"X;BY\"",
                stringWriter.toString());
    }

    @Test
    public void shouldEscapeDoubleQuotesProperlyAfterPrintPropertiesToCSVAndRewrite() throws Exception {

        String original2Quotes = "{\"hobby\" : \"watching \"Flash\"\"}";
        String original3Quotes = "{\"hobby\" : \"watching \"The \"Flash\"\"\"}";

        StringWriter stringWriter = new StringWriter();

        PropertySchema propertySchema1 = new PropertySchema("property1", false, DataType.String, false);
        PropertySchema propertySchema2 = new PropertySchema("property2", false, DataType.String, false);

        LabelSchema labelSchema = new LabelSchema(new Label("Entity"));
        labelSchema.put("property1", propertySchema1);
        labelSchema.put("property2", propertySchema2);

        HashMap<String, List<String>> props = new HashMap<String, List<String>>() {{
            put("property1", Collections.singletonList(original2Quotes));
            put("property2", Collections.singletonList(original3Quotes));
        }};

        CsvPropertyGraphPrinter printer = new CsvPropertyGraphPrinter(
                new PrintOutputWriter("outputId", stringWriter),
                labelSchema,
                new PrinterOptions(CsvPrinterOptions.builder().build()));

        printer.printProperties(props);

        // all double quotes should be escaped when printer prints
        assertEquals("\"{\"\"hobby\"\" : \"\"watching \"\"Flash\"\"\"\"}\"," +
                        "\"{\"\"hobby\"\" : \"\"watching \"\"The \"\"Flash\"\"\"\"\"\"}\"",
                stringWriter.toString());

        // using CSVFormat to read in printed items (same library used by RewriteCSV)
        String[] filePropertyHeaders = labelSchema.propertySchemas().stream()
                .map(p -> p.property().toString())
                .collect(Collectors.toList())
                .toArray(new String[]{});
        CSVFormat format = CSVFormat.RFC4180.withHeader(filePropertyHeaders);
        Reader in = new StringReader(stringWriter.toString());
        Iterable<CSVRecord> records = format.parse(in);

        for (CSVRecord record : records) {
            // what CSVFormat read in from printed CSV should be the original value
            assertEquals(original2Quotes, record.get("property1"));
            assertEquals(original3Quotes, record.get("property2"));

            // double quotes should all be properly escaped again when we format for rewrite
            assertEquals("\"{\"\"hobby\"\" : \"\"watching \"\"Flash\"\"\"\"}\"",
                    DataType.String.format(record.get("property1")));
            assertEquals("\"{\"\"hobby\"\" : \"\"watching \"\"The \"\"Flash\"\"\"\"\"\"}\"",
                    DataType.String.format(record.get("property2")));
        }
    }

}