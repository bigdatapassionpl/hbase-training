package com.bigdatapassion.hbase.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Test;

import static com.bigdatapassion.hbase.api.util.HbaseConfigurationFactory.getConfiguration;

public class HbaseConfigurationFactoryExternalTest {

    @Test
    public void shouldCreateConfiguration() throws Exception {
        //given
        Configuration configuration = getConfiguration();

        //when
//        HBaseAdmin.checkHBaseAvailable(configuration);
         HBaseAdmin.available(configuration);

        //then
    }

}