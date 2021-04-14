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

import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;


public class MyPortableExample {
  public static void main(String[] args) {
    PortablePipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).as(PortablePipelineOptions.class);
    options.setJobEndpoint("kwu-mn1.linkedin.biz:8099");
//    options.setJobEndpoint("kwu-mn1.linkedin.biz:11440");
    options.setRunner(PortableRunner.class);
    options.setSdkWorkerParallelism(1);

    Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of(KV.of("1", 1), KV.of("1", 2), KV.of("2", 2), KV.of("2", 3), KV.of("3", 3)))
        .apply(Combine.perKey(Sum.ofIntegers()))
        .apply(ConsoleOutput.of(kv -> String.format("key = %s, value = %s", kv.getKey(), kv.getValue())));
    pipeline.run();
  }
}
