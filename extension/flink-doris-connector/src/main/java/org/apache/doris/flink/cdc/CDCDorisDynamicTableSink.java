// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package org.apache.doris.flink.cdc;

import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.types.RowKind;

/**
 * DorisDynamicTableSink
 **/
public class CDCDorisDynamicTableSink implements DynamicTableSink {

    private final CDCDorisOptions options;
    private final DorisReadOptions readOptions;
    private final DorisExecutionOptions executionOptions;
    private final ResolvedSchema schema;

    public CDCDorisDynamicTableSink(CDCDorisOptions options, DorisReadOptions readOptions, DorisExecutionOptions executionOptions,ResolvedSchema schema) {
        this.options = options;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.schema = schema;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode changelogMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        CDCDorisDynamicOutputFormat.Builder builder = CDCDorisDynamicOutputFormat.builder()
                .setFenodes(options.getFenodes())
                .setUsername(options.getUsername())
                .setPassword(options.getPassword())
                .setTableIdentifier(options.getTableIdentifier())
                .setPrimaryKey(options.getPrimaryKey())
                .setQueryURL(options.getQueryURL())
                .setReadOptions(readOptions)
                .setSchema(schema)
                .setExecutionOptions(executionOptions);

        return OutputFormatProvider.of(builder.build());
    }

    @Override
    public DynamicTableSink copy() {
        return new CDCDorisDynamicTableSink(options, readOptions, executionOptions,schema);
    }

    @Override
    public String asSummaryString() {
        return "Doris Table CDC Sink";
    }
}
