package com.bigdatapassion.hbase.mapred.movies;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Before;
import org.junit.Test;
import com.bigdatapassion.hbase.api.dao.RatingDao;
import com.bigdatapassion.hbase.api.util.ConnectionHandler;
import com.bigdatapassion.hbase.api.util.HBaseTableUtil;
import com.bigdatapassion.hbase.api.util.HBaseUtil;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static com.bigdatapassion.hbase.mapred.movies.MovieAverageRatingsConstants.*;

/**
 * Wyliczenie średniej oceny dla filmu i zapisanie jej w nowej tabeli
 */
public class AverageRatingToTableExternalTest {

    @Before
    public void before() throws IOException {
        HBaseUtil.recreateTable(TABLE_NAME, FAMILY_NAME);
    }

    @Test
    public void shouldRunMapReduce() throws Exception {
        //given
        Configuration configuration = HBaseConfiguration.create();
        configuration.set(AverageRatingReducer.FAMILY, FAMILY_NAME);
        configuration.set(AverageRatingReducer.QUALIFIER, QUALIFIER_NAME);

        Job job = Job.getInstance(configuration, "Average Rating");
        job.setJarByClass(AverageRatingMapper.class);

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.addFamily(RatingDao.CF);

        TableMapReduceUtil.initTableMapperJob(
                RatingDao.TABLE,
                scan,
                AverageRatingMapper.class,
                IntWritable.class,
                DoubleWritable.class,
                job);
        TableMapReduceUtil.initTableReducerJob(
                TABLE_NAME.getNameAsString(),
                AverageRatingReducer.class,
                job);

        //when
        boolean succeeded = job.waitForCompletion(true);

        //then
        assertThat(succeeded).isTrue();
        assertThat(ConnectionHandler.getConnection().getAdmin().tableExists(TABLE_NAME)).isTrue();
        assertThat(HBaseTableUtil.countNumberOfRows(TABLE_NAME)).isGreaterThan(1000);
    }

}