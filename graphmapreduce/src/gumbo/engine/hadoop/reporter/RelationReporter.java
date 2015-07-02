package gumbo.engine.hadoop.reporter;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.algorithms.AlgorithmInterruptedException;
import gumbo.engine.general.algorithms.Map1GuardAlgorithm;
import gumbo.engine.general.algorithms.Map1GuardedAlgorithm;
import gumbo.engine.general.algorithms.MapAlgorithm;
import gumbo.engine.general.factories.Map1GuardMessageFactoryInterface;
import gumbo.engine.general.factories.Map1GuardedMessageFactoryInterface;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardMessageFactory;
import gumbo.engine.hadoop.mrcomponents.round1.algorithms.Map1GuardedMessageFactory;
import gumbo.engine.settings.AbstractExecutorSettings;
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
		byte [][] bytes1 = new byte[split][];
		byte [][] bytes2 = new byte[bytes.length - split][];
		for (int i = 0; i < bytes.length; i++) {
			if (i < split) {
				bytes1[i] = bytes[i];
			} else {
				bytes2[i-split] = bytes[i];
			}
		}
		
		RelationReport rr1 = runSample(relation, group, eso, guard, bytes1);
		RelationReport rr2 = runSample(relation, group, eso, guard, bytes2);

		// TODO make extrapolation optional.
		extrapolate(rr1,rr2,rr);

		return rr;
	}



	private RelationReport runSample(RelationSchema rs, Collection<GFExistentialExpression> group,
			ExpressionSetOperations eso, boolean guard, 
			byte [][] rawbytes) {

		RelationReport rr = new RelationReport(rs);

		try {
			// create new expressions


			// create algorithm
			MapAlgorithm algo;

			FakeMapper fm = new FakeMapper();
			Text t = new Text();

			if (guard) { // TODO make eso predicate
				Map1GuardMessageFactoryInterface fact = new Map1GuardMessageFactory(fm.context, settings, eso);
				fact.enableSampleCounting();
				algo = new Map1GuardAlgorithm(eso, fact, settings);
			} else {
				Map1GuardedMessageFactoryInterface fact = new Map1GuardedMessageFactory(fm.context, settings, eso);
				fact.enableSampleCounting();
				algo = new Map1GuardedAlgorithm(eso,fact,settings.getBooleanProperty(AbstractExecutorSettings.mapOutputGroupingOptimizationOn));

			}

			long totalBytesRead = 0;



			for (int i = 0; i < rawbytes.length; i++) {
				// read text lines
				try(LineReader l = new LineReader(new ByteArrayInputStream(rawbytes[i]))) {
					// skip first line, as it may be incomplete
					long bytesread = l.readLine(t); 

					long offset = 0;
					while (true) {
						// read next line
						bytesread = l.readLine(t); 
						if (bytesread <= 0)
							break;

						// if csv, wrap
						InputFormat format = mapping.getInputFormat(rs);

						String s = t.toString();
						if (format == InputFormat.CSV) {
							s = rs.getName() + "(" + s + ")";
						}

						// convert to tuple
						Tuple tuple = new Tuple(s);

						// feed to algorithm
						algo.run(tuple, offset);
						offset += bytesread;
					}

					// keep track of actual bytes read
					totalBytesRead += offset;
				}


			}


			// get results from fake context
			// and extrapolate
			double ratio = 1; //rr.getNumInputBytes() / (double)totalBytesRead;

			rr.setNumInputBytes(totalBytesRead);
			rr.setEstInputTuples((long)(ratio * fm.context.getInputTuples()));
			rr.setEstIntermTuples((long) (ratio * fm.context.getOutputTuples()));
			rr.setEstIntermBytes((long) (ratio * fm.context.getOutputBytes()));
			rr.setEstIntermKeyBytes((long) (ratio * fm.context.getOutputKeyBytes()));
			rr.setEstIntermValueBytes((long) (ratio * fm.context.getOutputValueBytes()));

			// average size of fields
			// TODO use tuples for this




		} catch (AlgorithmInterruptedException | IOException e) {
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
