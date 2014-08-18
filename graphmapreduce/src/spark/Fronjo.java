package spark;

/*
 * Fronjo 0.9
 * Computing the query R(x,y) & S(y)
 */

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class Fronjo {
	private static final Pattern COMMA = Pattern.compile(",");

	public static void main(String[] args) throws Exception {

		SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> R = ctx.textFile("./input/sparkrelations/R.txt", 1);
		JavaRDD<String> S = ctx.textFile("./input/sparkrelations/S.txt", 1);

		JavaPairRDD<String, String> Rmap = R.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				String[] K = COMMA.split(s);
				System.out.println(K[0] + ":" + K[1]);
				return new Tuple2<String, String>(K[1], K[0]);
			}
		});

		JavaPairRDD<String, String> Smap = S.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String s) {
				return new Tuple2<String, String>(s, "SSS");
			}
		});

		JavaPairRDD<String, String> p1 = Rmap.union(Smap);
		JavaPairRDD<String, Iterable<String>> p2 = p1.groupByKey();

		JavaPairRDD<String, Iterable<String>> p3 = p2.filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
			@Override
			public Boolean call(Tuple2<String, Iterable<String>> t) {
				String k = t._1();
				Iterable<String> L = t._2();
				for (String s : L) {
					if (s.equals("SSS")) {
						return true;
					}
				}
				return false;
			}
		});

		List<Tuple2<String, Iterable<String>>> output = p3.collect();
		for (Tuple2<String, Iterable<String>> tuple : output) {
			printing(tuple);
		}

		ctx.stop();
	}

	private static void printing(Tuple2<String, Iterable<String>> t) {
		Iterable<String> T = t._2();
		for (String ss : T) {
			if (!ss.equals("SSS"))
				System.out.println("[OUTPUT] " + t._1() + "," + ss);
		}
	}
}
