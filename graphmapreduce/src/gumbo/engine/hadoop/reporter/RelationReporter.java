package gumbo.engine.hadoop.reporter;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.algorithms.Map1GuardAlgorithm;
import gumbo.engine.general.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.general.algorithms.MapAlgorithm;
import gumbo.engine.general.messagefactories.Map1GuardMessageFactoryInterface;
import gumbo.engine.general.messagefactories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations.GFOperationInitException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

public class RelationReporter {

	RelationFileMapping mapping;
	AbstractExecutorSettings settings;
	RelationSampleContainer rsc;


	public RelationReporter(RelationSampleContainer rsc, RelationFileMapping mapping, AbstractExecutorSettings settings) {
		this.mapping = mapping;
		this.settings = settings;
		this.rsc = rsc;
	}

	public Map<RelationSchema, RelationReport> generateReports(Collection<GFExistentialExpression> group) throws GFOperationInitException {

		HashMap<RelationSchema, RelationReport> reports = new HashMap<>();


		ExpressionSetOperations eso = new ExpressionSetOperations(group, mapping);

		// guard
		for (GFAtomicExpression guard : eso.getGuardsAll()){
			RelationSchema relation = guard.getRelationSchema();
			reports.put(relation, generateReport(relation, group, eso, true));
		}

		//guarded
		for (GFAtomicExpression guarded : eso.getGuardedsAll()){
			RelationSchema relation = guarded.getRelationSchema();
			reports.put(relation, generateReport(relation, group, eso, false));
		}

		return reports;
	}

	private RelationReport generateReport(RelationSchema relation,
			Collection<GFExistentialExpression> group, ExpressionSetOperations eso, boolean guard) {


		RelationReport rr = new RelationReport(relation);

		// count files
		long numFiles = mapping.getPaths(relation).size(); 
		rr.setNumFiles(numFiles);

		// count size
		long numInputBytes = mapping.getRelationSize(relation); 
		rr.setNumInputBytes(numInputBytes);

		// split sample in two unequal parts, to improve extrapolation
		byte [][] bytes = rsc.getSamples(relation);
		int split = (int) Math.floor(bytes.length / 10);
		RelationTupleSampleContainer rtsc = new RelationTupleSampleContainer(rsc, split, mapping);


		RelationReport rr1 = runSample(relation, group, eso, guard, rtsc.getSmallTuples(relation),rtsc.getSmallSize(relation));
		RelationReport rr2 = runSample(relation, group, eso, guard, rtsc.getBigTuples(relation),rtsc.getBigSize(relation));

		extrapolate(rr1,rr2,rr);
		
//		System.out.println(rr1);
//		System.out.println(rr2);
//		System.out.println(rr);
//		System.out.println("---");

		return rr;
	}



	private RelationReport runSample(RelationSchema rs, Collection<GFExistentialExpression> group,
			ExpressionSetOperations eso, boolean guard, 
			List<Tuple> tuples, long bytes) {

		RelationReport rr = new RelationReport(rs);

		try {
			// create new expressions


			// create algorithm
			MapAlgorithm algo;

			FakeMapper fm = new FakeMapper();

			if (guard) { // TODO make eso predicate
				Map1GuardMessageFactoryInterface fact = new Map1GuardMessageFactory(fm.context, settings, eso);
				fact.enableSampleCounting();
				algo = new Map1GuardAlgorithm(eso, fact, settings);
			} else {
				Map1GuardedMessageFactoryInterface fact = new Map1GuardedMessageFactory(fm.context, settings, eso);
				fact.enableSampleCounting();
				algo = new Map1GuardedAlgorithm(eso,fact,settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn));

			}



			long offset = 0;
			for (Tuple tuple : tuples) {
				// feed to algorithm
				algo.run(tuple, offset);
				offset += tuple.size(); // dummy offset

			}


			// get results from fake context
			// and extrapolate
			double ratio = 1; //rr.getNumInputBytes() / (double)totalBytesRead;

			long totalBytesRead = bytes;
			rr.setNumInputBytes(totalBytesRead);
			rr.setEstInputTuples((long)(ratio * fm.context.getInputTuples()));
			rr.setEstIntermTuples((long) (ratio * fm.context.getOutputTuples()));
			rr.setEstIntermBytes((long) (ratio * fm.context.getOutputBytes()));
			rr.setEstIntermKeyBytes((long) (ratio * fm.context.getOutputKeyBytes()));
			rr.setEstIntermValueBytes((long) (ratio * fm.context.getOutputValueBytes()));

			// average size of fields
			// TODO use tuples for this




		} catch (AlgorithmInterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return rr;


	}

	private void extrapolate(RelationReport rr1, RelationReport rr2,RelationReport goal) {
		try {
			LinearExtrapolator le = new LinearExtrapolator();
			for (Field f : RelationReport.class.getFields()) {

				if (f.getName().contains("est")) {
					le.loadValues(rr1.getNumInputBytes(), (long)f.get(rr1), rr2.getNumInputBytes(), (long)f.get(rr2));
					f.set(goal, (long)le.extrapolate(goal.getNumInputBytes()));
				}
			}
		} catch (IllegalArgumentException | IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
