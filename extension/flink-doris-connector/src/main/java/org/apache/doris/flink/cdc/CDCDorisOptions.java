package org.apache.doris.flink.cdc;



import org.apache.doris.flink.cfg.DorisOptions;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CDCDorisOptions extends CDCDorisConnectionOptions {
    private static final long serialVersionUID = 1L;
    private String tableIdentifier;

    public CDCDorisOptions(String fenodes, String username, String password, String tableIdentifier,String primaryKey,String queryURL) {
        super(fenodes,username,password,primaryKey,queryURL);
        this.tableIdentifier = tableIdentifier;
    }

    public String getTableIdentifier() {
        return tableIdentifier;
    }

    public DorisOptions getDorisOptions(){
        DorisOptions.Builder builder = new DorisOptions.Builder();
        DorisOptions dorisOptions = builder.setFenodes(fenodes)
                .setPassword(password)
                .setTableIdentifier(tableIdentifier)
                .setUsername(username)
                .build();
        return dorisOptions;
    }

    public static Builder builder(){
        return new Builder();
    }
    /**
     * Builder of {@link CDCDorisOptions}.
     */
    public static class Builder {
        private String fenodes;
        private String username;
        private String password;
        private String tableIdentifier;
        private String primaryKey;
        private String queryURL;

        public Builder() {
        }

        /**
         * required, tableIdentifier
         */
        public Builder setTableIdentifier(String tableIdentifier) {
            this.tableIdentifier = tableIdentifier;
            return this;
        }

        /**
         * optional, user name.
         */
        public Builder setUsername(String username) {
            this.username = username;
            return this;
        }

        /**
         * optional, password.
         */
        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        /**
         * required, JDBC DB url.
         */
        public Builder setFenodes(String fenodes) {
            this.fenodes = fenodes;
            return this;
        }

        public Builder setPrimaryKey(String primaryKey){
            this.primaryKey = primaryKey;
            return this;
        }

        public Builder setQueryURL(String url){
            this.queryURL = url;
            return this;
        }

        public CDCDorisOptions build() {
            checkNotNull(fenodes, "No fenodes supplied.");
            checkNotNull(tableIdentifier, "No tableIdentifier supplied.");
            return new CDCDorisOptions(fenodes, username, password,tableIdentifier, primaryKey,queryURL);
        }
    }
}
