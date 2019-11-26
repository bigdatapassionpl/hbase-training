package com.bigdatapassion.hbase.api.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class ConnectionHandler {

    private static Connection connection = createConnection();

    public static Connection getConnection() {
        return connection;
    }

    private static Connection createConnection() {
        try {
            Configuration configuration = HbaseConfigurationFactory.getConfiguration();
            Connection connection = ConnectionFactory.createConnection(configuration);
            return connection;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
