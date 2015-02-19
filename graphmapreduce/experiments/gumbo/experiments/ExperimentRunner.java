package gumbo.experiments;

import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.input.GumboQuery;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

public class ExperimentRunner extends Configured implements Tool {


	private Experiment experiment;

	public ExperimentRunner(Class<? extends Experiment> theClass) {
		try {
			System.out.println(theClass);
			Constructor<? extends Experiment> ctor = theClass.getConstructor();
			this.experiment = ctor.newInstance();
		} catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {

		if (args.length < 1)
			throw new MissingArgumentException("Missing Experiment class.");
		
		// get query
		GumboQuery query = experiment.getQuery(args);
		
		// compile
		GFCompiler compiler = new GFCompiler();
		GumboPlan plan = compiler.createPlan(query);
		
		// execute and time execution
		long startTime = System.nanoTime();

		HadoopEngine engine = new HadoopEngine();
		engine.executePlan(plan, getConf());
		
		long endTime = System.nanoTime();
		long duration = (endTime - startTime) / 1000000;
		System.out.println(duration);
		
		return 0;
	}

}
