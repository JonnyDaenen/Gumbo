package spark;

/*
 * Fronjo 0.9
 * Computing the query R(x,y) & S(y)
 */

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import mapreduce.guardedfragment.planner.structures.data.Tuple;
import mapreduce.guardedfragment.structure.booleanexpressions.BEvaluationContext;
import mapreduce.guardedfragment.structure.booleanexpressions.BExpression;
import mapreduce.guardedfragment.structure.booleanexpressions.VariableNotFoundException;
import mapreduce.guardedfragment.structure.conversion.GFBooleanMapping;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConversionException;
import mapreduce.guardedfragment.structure.conversion.GFtoBooleanConvertor;
import mapreduce.guardedfragment.structure.gfexpressions.GFAtomicExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFPrefixSerializer;
import mapreduce.guardedfragment.structure.gfexpressions.operations.GFAtomProjection;
import mapreduce.guardedfragment.structure.gfexpressions.operations.NonMatchingTupleException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class FronjoSpark {
	private static final Pattern COMMA = Pattern.compile(",");
	private static GFExpression gfe;
	private static Set<GFExistentialExpression> formulaSet;

	public static void main(String[] args) throws Exception {

		// String inputfile = "input.txt";
		String query = new String("#O(x)&G(x,y,z)&S(x,z)!S(y,z)");

		formulaSet = new HashSet<GFExistentialExpression>();

		GFPrefixSerializer parser = new GFPrefixSerializer();
		gfe = parser.deserialize(query);
		for (GFExistentialExpression ff : gfe.getSubExistentialExpression(1)) {
			formulaSet.add(ff);
		}

//		SparkConf sparkConf = new SparkConf().setMaster("local[1]").setAppName("Fronjo");
		SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> input = ctx.textFile("input.txt", 1);

		JavaPairRDD<String, String> X1 = input.flatMapToPair(new PairFlatMapFunction<String, String, String>() {
					@Override
					public Iterable<Tuple2<String, String>> call(String s) {
						Set<Tuple2<String, String>> K = new HashSet<Tuple2<String, String>>();
						Tuple t = new Tuple(s);

						for (GFExistentialExpression formula : formulaSet) {

							GFAtomicExpression guard = formula.getGuard();
							Set<GFAtomicExpression> guardedRelations = formula.getChild().getAtomic();

							if (guard.matches(t)) {

								for (GFAtomicExpression guarded : guardedRelations) {

									GFAtomProjection gp = new GFAtomProjection(guard, guarded);
									Tuple tprime;
									try {
										// project to key
										tprime = gp.project(t);

										if (guarded.matches(tprime)) {
											K.add(new Tuple2<String, String>(tprime.toString(), t.toString()));
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
									K.add(new Tuple2<String, String>(t.toString(), t.toString()));
									// break;
								}
							}
						}
						return K;
					}
				});

		JavaPairRDD<String, Iterable<String>> map1 = X1.groupByKey(1);

		List<Tuple2<String, Iterable<String>>> output = map1.collect();
		for (Tuple2<String, Iterable<String>> tuple : output) {
			System.out.println(tuple);
		}

		JavaPairRDD<String, String> reduce1 = map1
				.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
					@Override
					public Iterable<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> kv) {
						Set<Tuple2<String, String>> K = new HashSet<Tuple2<String, String>>();
						Tuple tuple;
						String skey = kv._1();
						Boolean foundKey = false;

						for (GFExistentialExpression formula : formulaSet) {

							GFAtomicExpression guard = formula.getGuard();

							for (String s : kv._2()) {
								if (s.equals(skey)) {
									foundKey = true;
									break;
								}
							}

							// if the guarded tuple is actually in the database
							if (foundKey) {

								for (String s : kv._2()) {

									tuple = new Tuple(s);
									if (guard.matches(tuple)) {

										Tuple guardTuple = tuple;

										// get all atomics in the formula
										Set<GFAtomicExpression> guarded = formula.getChild().getAtomic();

										// for each atomic
										for (GFAtomicExpression guardedAtom : guarded) {

											// project the guard tuple onto
											// current atom
											GFAtomProjection p = new GFAtomProjection(guard, guardedAtom);
											Tuple projectedTuple;

											try {
												projectedTuple = p
														.project(guardTuple);
												if (projectedTuple.equals(new Tuple(skey))) {
													K.add(new Tuple2<String, String>(
															guardTuple.generateString(),
															guardedAtom.generateString()));
												}

											} catch (NonMatchingTupleException e) {
												// should not happen
												e.printStackTrace();
											}

										}
									}
								}
							} else {

								for (String s : kv._2()) {
									if (guard.matches(new Tuple(s))) {
										K.add(new Tuple2<String, String>(s,new String("")));
									}
								}
							}
						}
						return K;
					}
				});

		JavaPairRDD<String, Iterable<String>> map2 = reduce1.groupByKey();

		JavaPairRDD<String, Iterable<String>> reduce2 = map2
				.filter(new Function<Tuple2<String, Iterable<String>>, Boolean>() {
					@Override
					public Boolean call(Tuple2<String, Iterable<String>> t) {

						GFtoBooleanConvertor convertor = new GFtoBooleanConvertor();

						for (GFExistentialExpression formula : formulaSet) {

							GFAtomicExpression output = formula.getOutputRelation();
							GFAtomicExpression guard = formula.getGuard();
							GFExpression child = formula.getChild();
							Set<GFAtomicExpression> allAtoms = child.getAtomic();

							// calculate projection to output relation
							// OPTIMIZE this can be done in advance
							GFAtomProjection p = new GFAtomProjection(guard,output);

							// convert to boolean formula, while constructing
							// the mapping
							// automatically
							BExpression booleanChildExpression = null;
							// mapping will be created by convertor
							GFBooleanMapping mapGFtoB = null;
							try {
								booleanChildExpression = convertor.convert(child);
								mapGFtoB = convertor.getMapping();

							} catch (GFtoBooleanConversionException e1) {
								continue;
							}

							// if this tuple applies to the current formula
							if (guard.matches(new Tuple(t._1()))) {

								// Create a boolean context, and set all atoms
								// to false
								// initially
								BEvaluationContext booleanContext = new BEvaluationContext();
								for (GFAtomicExpression atom : allAtoms) {
									booleanContext.setValue(mapGFtoB.getVariable(atom), false);
								}

								// atoms that appear as values are set to true
								for (String vv : t._2()) {
									Tuple tuple = new Tuple(vv);
									GFAtomicExpression dummy = new GFAtomicExpression(
											tuple.getName(), tuple.getAllData());
									booleanContext.setValue(
											mapGFtoB.getVariable(dummy), true);
								}

								// evaluate boolean formula using the created
								// context
								try {
									if (booleanChildExpression.evaluate(booleanContext)) {
										return true;
									}
								} catch (VariableNotFoundException e) {
									// should not happen
									e.printStackTrace();
								}

							}
						}
						return false;
					}
				});
		
		JavaRDD<String> out = reduce2.keys(); 

		List<String> output2 = out.collect();
		for (String tuple : output2) {
			System.out.println("[MY OUTPUT] "+tuple);
		}

		ctx.stop();
	}


}
