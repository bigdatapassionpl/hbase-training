package com.bigdatapassion.hbase.spark

import java.util.Date

import com.bigdatapassion.hbase.api.dao.MovieDao
import com.bigdatapassion.hbase.api.util.{HBaseTableBuilder, HbaseConfigurationFactory}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.spark.{SparkConf, SparkContext}
import com.bigdatapassion.hbase.api.util.{HBaseTableBuilder, HbaseConfigurationFactory}
import org.apache.hadoop.mapreduce.Job

object SparkHbaseMoviesFilter {

  val inputTable = "radek:movies"
  val outputTable = "radek:movies_action"

  def main(args: Array[String]): Unit = {

    // prepare
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName + new Date())
    val sc = new SparkContext(conf)

    // run
    val hbaseConf = HbaseConfigurationFactory.getConfiguration
    hbaseConf.set(TableInputFormat.INPUT_TABLE, inputTable)

    val inputTableRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val moviesRdd = inputTableRdd.map(result => MovieDao.createMovie(result._2))
    val actionMoviesRdd = moviesRdd.filter(movie => movie.getGenres.contains("Action"))
    val outputTableRdd = actionMoviesRdd.map(movie => (movie.getMovieId, MovieDao.createPut(movie)))

    new HBaseTableBuilder().withTable(outputTable).withFamily(MovieDao.CF).rebuild()

    // new Hadoop API configuration// new Hadoop API configuration
    val newAPIJobConfiguration1 = Job.getInstance(hbaseConf)
    newAPIJobConfiguration1.getConfiguration.set(TableOutputFormat.OUTPUT_TABLE, outputTable)
    newAPIJobConfiguration1.setOutputFormatClass(classOf[TableOutputFormat[_]])
    outputTableRdd.saveAsNewAPIHadoopDataset(newAPIJobConfiguration1.getConfiguration)

    // end
    sc.stop()
  }

}
