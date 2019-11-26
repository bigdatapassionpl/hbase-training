package com.bigdatapassion.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.bigdatapassion.hbase.api.util.HBaseUtil;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static com.bigdatapassion.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class HbaseApiExternalTest {

    private static final TableName TEST_TABLE_NAME = HBaseUtil.getUserTableName("hbase_api_test_table");
    private static final String FAMILY_NAME_1 = "cf1";
    private static final String FAMILY_NAME_2 = "cf2";

    private Connection connection;
    private Admin admin;

    @Before
    public void createTestTable() throws Exception {
        Configuration configuration = getConfiguration();
        connection = ConnectionFactory.createConnection(configuration);
        admin = connection.getAdmin();

        if (admin.tableExists(TEST_TABLE_NAME)) {
            admin.disableTable(TEST_TABLE_NAME);
            admin.deleteTable(TEST_TABLE_NAME);
        }

        HTableDescriptor table = new HTableDescriptor(TEST_TABLE_NAME);

        HColumnDescriptor columnFamily1 = new HColumnDescriptor(FAMILY_NAME_1);
        columnFamily1.setMaxVersions(10);
        table.addFamily(columnFamily1);

        HColumnDescriptor columnFamily2 = new HColumnDescriptor(FAMILY_NAME_2);
        columnFamily2.setMaxVersions(10);
        table.addFamily(columnFamily2);

        HBaseUtil.createNamespaceIfNotExists(TEST_TABLE_NAME.getNamespaceAsString());
        admin.createTable(table);
    }

    @After
    public void deleteTable() throws Exception {
        if (admin != null) {
//            admin.disableTable(TEST_TABLE_NAME);
//            admin.deleteTable(TEST_TABLE_NAME);
            admin.close();
        }
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void shouldPutAndGetDataFromHbase() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier = "cell";
        String value = "nasza testowa wartość";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value));

        table.put(put);

        //when
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = table.get(get);

        //then
        assertThat(value).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier))));
    }

    @Test
    public void shouldPutAndGetDataFromHbaseWithVersions() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier = "cell";
        String value1 = "nasza testowa wartość";
        String value2 = "nasza testowa wartość 2";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value1));
        table.put(put);

        put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value2));
        table.put(put);

        //when
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = table.get(get);

        //then
        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier))));

        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier));
        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(CellUtil.cloneValue(columnCells.get(0))));
        assertThat(value1).isEqualToIgnoringCase(Bytes.toString(CellUtil.cloneValue(columnCells.get(1))));
    }

    @Test
    public void shouldDeleteDataFromHbase() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier1 = "cell1";
        String qualifier2 = "cell2";
        String value1 = "nasza testowa wartosc 1";
        String value2 = "nasza testowa wartosc 2";
        String value3 = "nasza testowa wartosc 3";

        long timestamp = 101;
        put(table, id, FAMILY_NAME_1, qualifier1, timestamp, value1);
        put(table, id, FAMILY_NAME_1, qualifier2, timestamp, value1);
        put(table, id, FAMILY_NAME_2, qualifier1, timestamp, value1);
        put(table, id, FAMILY_NAME_2, qualifier2, timestamp, value1);

        timestamp++;
        put(table, id, FAMILY_NAME_1, qualifier1, timestamp, value2);
        put(table, id, FAMILY_NAME_1, qualifier2, timestamp, value2);
        put(table, id, FAMILY_NAME_2, qualifier1, timestamp, value2);
        put(table, id, FAMILY_NAME_2, qualifier2, timestamp, value2);

        timestamp++;
        put(table, id, FAMILY_NAME_1, qualifier1, timestamp, value3);
        put(table, id, FAMILY_NAME_1, qualifier2, timestamp, value3);
        put(table, id, FAMILY_NAME_2, qualifier1, timestamp, value3);
        put(table, id, FAMILY_NAME_2, qualifier2, timestamp, value3);

        //when
        Delete delete = new Delete(Bytes.toBytes(id));
        delete.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier1));
        // delete.addColumn(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier1), 102);
        // delete.addColumns(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier1));
        // delete.addColumns(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier1), 102);
        // delete.addFamily(Bytes.toBytes(FAMILY_NAME_1));
        // delete.addFamily(Bytes.toBytes(FAMILY_NAME_1),102);
        // delete.addFamilyVersion(Bytes.toBytes(FAMILY_NAME_1),102);
        table.delete(delete);

        //then
        Get get = new Get(Bytes.toBytes(id));
        get.setMaxVersions(10);
        Result result = table.get(get);

        assertThat(value2).isEqualToIgnoringCase(Bytes.toString(result.getValue(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier1))));
    }

    private void put(Table table, String id, String family, String qualifier, long timestamp, String value) throws Exception {
        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(family),
                Bytes.toBytes(qualifier),
                timestamp,
                Bytes.toBytes(value));
        table.put(put);
    }

    @Test
    public void shouldScanTable() throws Exception {
        //given
        Table table = connection.getTable(TEST_TABLE_NAME);

        String id = "id";
        String qualifier = "cell";
        String value = "nasza testowa wartość";

        Put put = new Put(Bytes.toBytes(id));
        put.addColumn(Bytes.toBytes(FAMILY_NAME_1),
                Bytes.toBytes(qualifier),
                Bytes.toBytes(value));

        table.put(put);

        //when
        Scan scan = new Scan();
        scan.setMaxVersions(10);

        ResultScanner scanner = table.getScanner(scan);
        ArrayList<Result> results = new ArrayList<>();
        for (Result result : scanner) {
            results.add(result);
        }

        //then
        assertThat(results).isNotEmpty();
        assertThat(value).isEqualToIgnoringCase(Bytes.toString(results.get(0).getValue(Bytes.toBytes(FAMILY_NAME_1), Bytes.toBytes(qualifier))));
    }

}
