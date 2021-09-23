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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Doris connection options.
 */
public class CDCDorisConnectionOptions implements Serializable {

    private static final long serialVersionUID = 1L;

    protected final String fenodes;
    protected final String username;
    protected final String password;
    protected final String primaryKey;
    protected final String queryURL;

    public CDCDorisConnectionOptions(String fenodes, String username, String password,String primaryKey,String queryURL) {
        this.fenodes = Preconditions.checkNotNull(fenodes, "fenodes  is empty");
        this.username = username;
        this.password = password;
        this.primaryKey = primaryKey;
        this.queryURL = queryURL;
    }

    public String getFenodes() {
        return fenodes;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getPrimaryKey(){
        return primaryKey;
    }

    public String getQueryURL() {
        return queryURL;
    }

    /**
     * Builder for {@link CDCDorisConnectionOptions}.
     */
    public static class CDCDorisConnectionOptionsBuilder {
        private String fenodes;
        private String username;
        private String password;
        private String primaryKey;
        private String queryURL;


        public CDCDorisConnectionOptionsBuilder withFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public CDCDorisConnectionOptionsBuilder withUsername(String username) {
            this.username = username;
            return this;
        }

        public CDCDorisConnectionOptionsBuilder withPassword(String password) {
            this.password = password;
            return this;
        }

        public CDCDorisConnectionOptionsBuilder withQueryURL(String queryURL) {
            this.queryURL = queryURL;
            return this;
        }
        public CDCDorisConnectionOptionsBuilder withPrimaryKey(String primaryKey) {
            this.primaryKey = primaryKey;
            return this;
        }


        public CDCDorisConnectionOptions build() {
            return new CDCDorisConnectionOptions(fenodes, username, password,primaryKey,queryURL);
        }
    }

}
