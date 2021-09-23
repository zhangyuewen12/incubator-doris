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
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.exception.DorisException;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.RestService;
import org.apache.doris.flink.table.DorisStreamLoad;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;


/**
 * DorisDynamicOutputFormat
 **/
public class CDCDorisDynamicOutputFormat extends RichOutputFormat<RowData> {

    private static final Logger LOG = LoggerFactory.getLogger(CDCDorisDynamicOutputFormat.class);
    private static final String FIELD_DELIMITER_KEY = "column_separator";
    private static final String FIELD_DELIMITER_DEFAULT = "\t";
    private static final String LINE_DELIMITER_KEY = "line_delimiter";
    private static final String LINE_DELIMITER_DEFAULT = "\n";
    private static final String NULL_VALUE = "\\N";
    private final String fieldDelimiter;
    private final String lineDelimiter;

    private CDCDorisOptions options;
    private DorisReadOptions readOptions;
    private DorisExecutionOptions executionOptions;
    private DorisStreamLoad dorisStreamLoad;


    private final List<String> batch = new ArrayList<>();
    private transient volatile boolean closed = false;

    private transient ScheduledExecutorService scheduler;
    private transient ScheduledFuture<?> scheduledFuture;
    private transient volatile Exception flushException;


    private  ResolvedSchema schema;

    public CDCDorisDynamicOutputFormat(CDCDorisOptions option, DorisReadOptions readOptions, DorisExecutionOptions executionOptions,ResolvedSchema schema) {
        this.options = option;
        this.readOptions = readOptions;
        this.executionOptions = executionOptions;
        this.fieldDelimiter = executionOptions.getStreamLoadProp().getProperty(FIELD_DELIMITER_KEY, FIELD_DELIMITER_DEFAULT);
        this.lineDelimiter = executionOptions.getStreamLoadProp().getProperty(LINE_DELIMITER_KEY, LINE_DELIMITER_DEFAULT);
        this.schema = schema;
    }

    @Override
    public void configure(Configuration configuration) {
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {
        dorisStreamLoad = new DorisStreamLoad(
                getBackend(),
                options.getTableIdentifier().split("\\.")[0],
                options.getTableIdentifier().split("\\.")[1],
                options.getUsername(),
                options.getPassword(),
                executionOptions.getStreamLoadProp());
        LOG.info("Streamload BE:{}", dorisStreamLoad.getLoadUrlStr());

        if (executionOptions.getBatchIntervalMs() != 0 && executionOptions.getBatchSize() != 1) {
            this.scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("doris-streamload-output-format"));
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(() -> {
                synchronized (CDCDorisDynamicOutputFormat.this) {
                    if (!closed) {
                        try {
                            flush();
                        } catch (Exception e) {
                            flushException = e;
                        }
                    }
                }
            }, executionOptions.getBatchIntervalMs(), executionOptions.getBatchIntervalMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new RuntimeException("Writing records to streamload failed.", flushException);
        }
    }

    @Override
    public synchronized void writeRecord(RowData row) throws IOException {
        checkFlushException();

        addBatch(row);
        if (executionOptions.getBatchSize() > 0 && batch.size() >= executionOptions.getBatchSize()) {
            flush();
        }
    }

    private void addBatch(RowData row)  {
        StringJoiner value = new StringJoiner(this.fieldDelimiter);
        GenericRowData rowData = (GenericRowData) row;
        RowKind rowKind = rowData.getRowKind();
        System.out.println(rowKind.shortString());
        if (rowKind == RowKind.DELETE){
            try {
                String table = options.getTableIdentifier();
                Class.forName("com.mysql.cj.jdbc.Driver");
                String primaryKey = options.getPrimaryKey();
                String primaryValue = "";
                for (int i = 0; i < row.getArity(); i++) {
                    String colName = schema.getColumn(i).get().getName();
                    if (colName.equals(primaryKey)){
                        Object field = rowData.getField(i);
                        if (field!=null){
                            primaryValue = field.toString();
                        }
                    }
                }
                String sql = "delete from " + table +" where "+ options.getPrimaryKey() +"="+primaryValue;
                System.out.println(sql);
                Connection connection = DriverManager.getConnection(options.getQueryURL(),options.getUsername(), options.getPassword());
                PreparedStatement ps = connection.prepareStatement(sql);
                ps.execute();
            }catch (Exception e){
                e.printStackTrace();;
                throw new RuntimeException("执行sql异常!");
            }
        }else {
            for (int i = 0; i < row.getArity(); ++i) {
                Object field = rowData.getField(i);
                if (field != null) {
                    value.add(field.toString());
                } else {
                    value.add(NULL_VALUE);
                }
            }
            //test
            System.out.println("test:"+value.toString());
            batch.add(value.toString());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            try {
                flush();
            } catch (Exception e) {
                LOG.warn("Writing records to doris failed.", e);
                throw new RuntimeException("Writing records to doris failed.", e);
            }
        }
        checkFlushException();
    }

    public synchronized void flush() throws IOException {
        checkFlushException();
        if (batch.isEmpty()) {
            return;
        }
        for (int i = 0; i <= executionOptions.getMaxRetries(); i++) {
            try {
                dorisStreamLoad.load(String.join(this.lineDelimiter, batch));
                batch.clear();
                break;
            } catch (StreamLoadException e) {
                LOG.error("doris sink error, retry times = {}", i, e);
                if (i >= executionOptions.getMaxRetries()) {
                    throw new IOException(e);
                }
                try {
                    dorisStreamLoad.setHostPort(getBackend());
                    LOG.warn("streamload error,switch be: {}", dorisStreamLoad.getLoadUrlStr(), e);
                    Thread.sleep(1000 * i);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IOException("unable to flush; interrupted while doing another attempt", e);
                }
            }
        }
    }


    private String getBackend() throws IOException {
        try {
            //get be url from fe
//            return RestService.randomBackend(options, readOptions, LOG);
            return RestService.randomBackend(options.getDorisOptions(), readOptions, LOG);
        } catch (IOException | DorisException e) {
            LOG.error("get backends info fail");
            throw new IOException(e);
        }
    }


    /**
     * A builder used to set parameters to the output format's configuration in a fluent way.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for {@link CDCDorisDynamicOutputFormat}.
     */
    public static class Builder {
        private CDCDorisOptions.Builder optionsBuilder;
        private DorisReadOptions readOptions;
        private DorisExecutionOptions executionOptions;
        private ResolvedSchema schema;

        public Builder() {
            this.optionsBuilder = CDCDorisOptions.builder();
        }

        public Builder setFenodes(String fenodes) {
            this.optionsBuilder.setFenodes(fenodes);
            return this;
        }

        public Builder setUsername(String username) {
            this.optionsBuilder.setUsername(username);
            return this;
        }

        public Builder setPassword(String password) {
            this.optionsBuilder.setPassword(password);
            return this;
        }

        public Builder setTableIdentifier(String tableIdentifier) {
            this.optionsBuilder.setTableIdentifier(tableIdentifier);
            return this;
        }

        public Builder setPrimaryKey(String primaryKey){
            this.optionsBuilder.setPrimaryKey(primaryKey);
            return this;
        }

        public Builder setQueryURL(String queryURL){
            this.optionsBuilder.setQueryURL(queryURL);
            return this;
        }
        public Builder setReadOptions(DorisReadOptions readOptions) {
            this.readOptions = readOptions;
            return this;
        }

        public Builder setExecutionOptions(DorisExecutionOptions executionOptions) {
            this.executionOptions = executionOptions;
            return this;
        }
        public Builder setSchema(ResolvedSchema schema){
            this.schema = schema;
            return this;
        }

        public CDCDorisDynamicOutputFormat build() {
            return new CDCDorisDynamicOutputFormat(
                    optionsBuilder.build(), readOptions, executionOptions,schema
            );
        }
    }
}
