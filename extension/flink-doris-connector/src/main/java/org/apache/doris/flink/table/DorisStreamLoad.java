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
package org.apache.doris.flink.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.doris.flink.exception.StreamLoadException;
import org.apache.doris.flink.rest.models.RespContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 * DorisStreamLoad
 **/
public class DorisStreamLoad implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(DorisStreamLoad.class);

    private final static List<String> DORIS_SUCCESS_STATUS = new ArrayList<>(Arrays.asList("Success", "Publish Timeout"));
    private static String loadUrlPattern = "http://%s/api/%s/%s/_stream_load?";
    private String user;
    private String passwd;
    private String loadUrlStr;
    private String hostPort;
    private String db;
    private String tbl;
    private String authEncoding;
    private Properties streamLoadProp;

    public DorisStreamLoad(String hostPort, String db, String tbl, String user, String passwd, Properties streamLoadProp) {
        this.hostPort = hostPort;
        this.db = db;
        this.tbl = tbl;
        this.user = user;
        this.passwd = passwd;
        this.loadUrlStr = String.format(loadUrlPattern, hostPort, db, tbl);
        this.authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        this.streamLoadProp = streamLoadProp;
    }

    public String getLoadUrlStr() {
        return loadUrlStr;
    }

    public String getHostPort() {
        return hostPort;
    }

    public void setHostPort(String hostPort) {
        this.hostPort = hostPort;
        this.loadUrlStr = String.format(loadUrlPattern, hostPort, this.db, this.tbl);
    }


    private HttpURLConnection getConnection(String urlStr, String label) throws IOException {
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setInstanceFollowRedirects(false);
        conn.setRequestMethod("PUT");
        String authEncoding = Base64.getEncoder().encodeToString(String.format("%s:%s", user, passwd).getBytes(StandardCharsets.UTF_8));
        conn.setRequestProperty("Authorization", "Basic " + authEncoding);
        conn.addRequestProperty("Expect", "100-continue");
        conn.addRequestProperty("Content-Type", "text/plain; charset=UTF-8");
        conn.addRequestProperty("label", label);
        for (Map.Entry<Object, Object> entry : streamLoadProp.entrySet()) {
            conn.addRequestProperty(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
        }
        conn.setDoOutput(true);
        conn.setDoInput(true);
        return conn;
    }

    public static class LoadResponse {
        public int status;
        public String respMsg;
        public String respContent;

        public LoadResponse(int status, String respMsg, String respContent) {
            this.status = status;
            this.respMsg = respMsg;
            this.respContent = respContent;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("status: ").append(status);
            sb.append(", resp msg: ").append(respMsg);
            sb.append(", resp content: ").append(respContent);
            return sb.toString();
        }
    }

    public void load(String value) throws StreamLoadException {
        LoadResponse loadResponse = loadBatch(value);
        LOG.info("Streamload Response:{}", loadResponse);
        if (loadResponse.status != 200) {
            throw new StreamLoadException("stream load error: " + loadResponse.respContent);
        } else {
            ObjectMapper obj = new ObjectMapper();
            try {
                RespContent respContent = obj.readValue(loadResponse.respContent, RespContent.class);
                if (!DORIS_SUCCESS_STATUS.contains(respContent.getStatus())) {
                    throw new StreamLoadException("stream load error: " + respContent.getMessage());
                }
            } catch (IOException e) {
                throw new StreamLoadException(e);
            }
        }
    }

    private LoadResponse loadBatch(String value) {
        String label = streamLoadProp.getProperty("label");
        if (StringUtils.isBlank(label)) {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
            String formatDate = sdf.format(new Date());
            label = String.format("flink_connector_%s_%s",formatDate,
                    UUID.randomUUID().toString().replaceAll("-", ""));
        }

        HttpURLConnection feConn = null;
        HttpURLConnection beConn = null;
        try {
            // build request and send to new be location
            beConn = getConnection(loadUrlStr, label);
            // send data to be
            BufferedOutputStream bos = new BufferedOutputStream(beConn.getOutputStream());
            System.out.println("value:"+value);
            bos.write(value.getBytes());
            bos.close();

            // get respond
            int status = beConn.getResponseCode();
            String respMsg = beConn.getResponseMessage();
            InputStream stream = (InputStream) beConn.getContent();
            BufferedReader br = new BufferedReader(new InputStreamReader(stream));
            StringBuilder response = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) {
                response.append(line);
            }
//            log.info("AuditLoader plugin load with label: {}, response code: {}, msg: {}, content: {}",label, status, respMsg, response.toString());
            return new LoadResponse(status, respMsg, response.toString());

        } catch (Exception e) {
            e.printStackTrace();
            String err = "failed to load audit via AuditLoader plugin with label: " + label;
            LOG.warn(err, e);
            return new LoadResponse(-1, e.getMessage(), err);
        } finally {
            if (feConn != null) {
                feConn.disconnect();
            }
            if (beConn != null) {
                beConn.disconnect();
            }
        }
    }
}
