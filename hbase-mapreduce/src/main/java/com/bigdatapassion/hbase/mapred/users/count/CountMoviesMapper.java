package com.bigdatapassion.hbase.mapred.users.count;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.bigdatapassion.hbase.api.dao.MovieDao;

public class CountMoviesMapper extends TableMapper<Text, LongWritable> {

    public enum Counters {
        MOVIE_COUNT
    }

    @Override
    protected void map(ImmutableBytesWritable rowkey, Result result, Mapper.Context context) {

        String genres = Bytes.toString(result.getValue(MovieDao.CF, MovieDao.GENRES));

        if (genres.toUpperCase().contains("Drama".toUpperCase())) {
            context.getCounter(Counters.MOVIE_COUNT).increment(1);
        }

    }

}
