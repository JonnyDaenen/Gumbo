package spark;

/*
 * Fronjo 0.9
 * Computing the query R(x,y) & S(y)
 */

import scala.Tuple2;

import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function;

import guardedfragment.structure.gfexpressions.GFAtomicExpression;
import guardedfragment.structure.gfexpressions.GFExistentialExpression;
import guardedfragment.structure.gfexpressions.GFExpression;
import guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;
import mapreduce.data.Tuple;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.regex.Pattern;

public class Fronjo {
	private static final Pattern COMMA = Pattern.compile(",");
	private static GFExpression gfe;

	public static void main(String[] args) throws Exception {
		
		String inputfile = args[0];
		String query = args[1];
		
		GFPrefixSerializer parser = new GFPrefixSerializer();
		gfe = parser.deserialize(query);
		Set<GFExistentialExpression> formulaSet = new HashSet<GFExistentialExpression>();

		SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		JavaRDD<String> input = ctx.textFile(inputfile, 1);

		JavaPairRDD<String, String> X1 = input.flatMapToPair(new PairFlatMapFunction<String,String,String>() {
			@Override
			public Iterable<Tuple2<String, String>> call(String s) {
				Set<Tuple2<String,String>> K = new HashSet<Tuple2<String,String>>();
				Tuple t = new Tuple(s);
				
				
				for (GFExistentialExpression formula : formulaSet) {

					GFAtomicExpression guard = formula.getGuard();
					Set<GFAtomicExpression> guardedRelations = formula.getChild().getAtomic();

					if (guard.matches(t)) {

						K.add(new Tuple2(s,s));

						for (GFAtomicExpression guarded : guardedRelations) {

							GFAtomProjection gp = new GFAtomProjection(guard, guarded);
							Tuple tprime;
							try {
								// project to key
								tprime = gp.project(t);

								if (guarded.matches(tprime)) {

									K.add(new Tuple2(tprime.toString(),tprime.toString()));
								}
							} catch (NonMatchingTupleException e1) {
								// should not happen!
								e1.printStackTrace();
							}

						}

					}

					for (GFAtomicExpression guarded : guardedRelations) {

						// if so, output tuple with same key and value
						if (guarded.matches(t)) {
							// LOG.error(t + " matches " + guarded);
							K.add(new Tuple2(t.toString(),t.toString()));
							// break;
						}
					}
				}		
				return K;
			}
		});
		
		JavaPairRDD<String,Iterable<String>> map1 = X1.groupByKey(1);
		
		List<Tuple2<String, Iterable<String>>> output = map1.collect();
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
	
	private static void SimpleSemijoin(String inputR, String inputS) throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> R = ctx.textFile(inputR, 1);
		JavaRDD<String> S = ctx.textFile(inputS, 1);

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
}
