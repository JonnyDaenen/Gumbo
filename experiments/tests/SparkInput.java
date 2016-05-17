/**
 * Created on: 19 Feb 2015
 */
package tests;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author Jonny Daenen
 *
 */
public class SparkInput {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("Gumbo");
		// SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaPairRDD<LongWritable, Text> rdd = ctx.newAPIHadoopFile("input/dummyrelations1/part1.txt", TextInputFormat.class, LongWritable.class, Text.class, new Configuration());

		JavaPairRDD<Long, String> rdd2 = rdd.mapToPair(new PairFunction<Tuple2<LongWritable,Text>, Long, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<Long, String> call(Tuple2<LongWritable, Text> t) throws Exception {

				return new Tuple2<Long, String>(t._1.get(),new String(t._2.getBytes(),0,t._2.getLength()));
			}
		});
		
		
		for ( Tuple2<Long, String> v : rdd2.collect()){
			System.out.println(v._1 + " " + v._2);
		}
		
		ctx.close(); // TODO close context in engine!
	}

}
