/*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
/**
  * A starter example for writing Beam programs.
  *
  * <p>
  * The example takes two strings, converts them to their upper-case
  * representation and logs them.
  *
  * <p>
  * To run this starter example locally using DirectRunner, just execute it
  * without any additional parameters from your favorite development environment.
  *
  * <p>
  * To run this starter example using managed resource in Google Cloud Platform,
  * you should specify the following command-line options:
  * --project=<YOUR_PROJECT_ID>
  * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
  */
public class GCSToBigQuery {
     private static final Logger LOG = LoggerFactory.getLogger(GCSToBigQuery.class);
     private static String HEADERS = "ID,Code,Value,Date";
    public static class FormatForBigquery extends DoFn<String, TableRow> {
        private String[] columnNames = HEADERS.split(",");
        @ProcessElement
         public void processElement(ProcessContext c) {
             TableRow row = new TableRow();
             String[] parts = c.element().split(",");
            if (!c.element().contains(HEADERS)) {
                 for (int i = 0; i < parts.length; i++) {
                     // No typy conversion at the moment.
                     row.set(columnNames[i], parts[i]);
                 }
                 c.output(row);
             }
         }
        /** Defines the BigQuery schema used for the output. */
        static TableSchema getSchema() {
             List<TableFieldSchema> fields = new ArrayList<>();
             // Currently store all values as String
             fields.add(new TableFieldSchema().setName("ID").setType("STRING"));
             fields.add(new TableFieldSchema().setName("Code").setType("STRING"));
             fields.add(new TableFieldSchema().setName("Value").setType("STRING"));
             fields.add(new TableFieldSchema().setName("Date").setType("STRING"));
            return new TableSchema().setFields(fields);
         }
     }
    public static void main(String[] args) throws Throwable {
         // Currently hard-code the variables, this can be passed into as parameters
         String sourceFilePath = "gs://my-project-data-bucket/sample.csv";
         String tempLocationPath = "gs://my-project-data-bucket/temp/";
         boolean isStreaming = false;
         TableReference tableRef = new TableReference();
         // Replace this with your own GCP project id
         tableRef.setProjectId("responsive-hall-125714");
         tableRef.setDatasetId("sampledataflow");
         tableRef.setTableId("sample");
//        PipelineOptions options = PipelineOptionsFactory.create();
//        options.setRunner(Dataflo);
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
         // This is required for BigQuery
         options.setTempLocation(tempLocationPath);
         options.setJobName("csvtobiqquery");
         Pipeline p = Pipeline.create(options);
        p.apply("Read CSV File", TextIO.read().from(sourceFilePath))
                 .apply("Log messages", ParDo.of(new DoFn<String, String>() {
                     @ProcessElement
                     public void processElement(ProcessContext c) {
                         LOG.info("Processing row: " + c.element());
                         c.output(c.element());
                     }
                 })).apply("Convert to BigQuery TableRow", ParDo.of(new FormatForBigquery()))
                 .apply("Write into BigQuery",
                         BigQueryIO.writeTableRows().to(tableRef).withSchema(FormatForBigquery.getSchema())
                                 .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                                 .withWriteDisposition(isStreaming ? BigQueryIO.Write.WriteDisposition.WRITE_APPEND
                                         : BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        p.run().waitUntilFinish();
    }
}