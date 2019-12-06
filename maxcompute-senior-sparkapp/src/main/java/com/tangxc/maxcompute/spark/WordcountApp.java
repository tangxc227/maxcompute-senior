package com.tangxc.maxcompute.spark;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * 统计maxcompute table中的每个单词出现的次数
 *
 * @author Xicheng.Tang
 */
public class WordcountApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("spark_sql_ddl")
                .config("spark.sql.broadcastTimeout", 20 * 60)
                .config("spark.sql.crossJoin.enabled", true)
                .config("odps.exec.dynamic.partition.mode", "nonstrict")
                .getOrCreate();

        JavaRDD<Row> dataSourceRdd = spark.sql("select id, content from t_content").javaRDD();

        JavaPairRDD<String, Integer> resultRdd = dataSourceRdd
                .filter(new Function<Row, Boolean>() {
                    @Override
                    public Boolean call(Row row) throws Exception {
                        return StringUtils.isNotBlank(row.getString(1));
                    }
                })
                .flatMapToPair(new PairFlatMapFunction<Row, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(Row row) throws Exception {
                        List<Tuple2<String, Integer>> wordCountPair = new ArrayList<>();
                        String content = row.getString(1);
                        String[] tokens = content.split("\\s+");
                        for (String word : tokens) {
                            wordCountPair.add(new Tuple2<>(word, 1));
                        }
                        return wordCountPair.iterator();
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                });

        StructType schema = DataTypes.createStructType(
                Arrays.asList(
                        DataTypes.createStructField("word", DataTypes.StringType, true),
                        DataTypes.createStructField("frequency", DataTypes.IntegerType, true))
        );
        JavaRDD<Row> rowRDD = resultRdd.map(new Function<Tuple2<String, Integer>, Row>() {
            @Override
            public Row call(Tuple2<String, Integer> tuple) throws Exception {
                return RowFactory.create(tuple._1, tuple._2);
            }
        });

        spark
                .createDataFrame(rowRDD, schema)
                .createOrReplaceTempView("tmp_t_result");

        spark.sql("insert overwrite table t_result_spark select * from tmp_t_result");
        spark.close();
    }
}
