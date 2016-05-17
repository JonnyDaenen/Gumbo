package gumbo.engine.hadoop2;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

/**
 * Extracts and prints Hadoop job counters.
 * @author Jonny Daenen
 *
 */
public class HadoopReporter {
	
	public void printCounters(JobControl jc) {
		try {
			System.out.println(collectCounters(jc));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String collectCounters(JobControl jc) throws IOException {

		//		if (jc.getSuccessfulJobList().size() == 0)
		//			return;

		// initialize overall counters
		Counters overallCounters = new Counters();
		StringBuilder sb = new StringBuilder(20000);

		for (ControlledJob job : jc.getSuccessfulJobList()) {
			sb.append("Counters for job: " + job.getJobName());
			sb.append(System.lineSeparator());
			sb.append("--------------------------------------------------------------------------------");
			sb.append(System.lineSeparator());
			Counters counters = job.getJob().getCounters();


			for (String groupName : counters.getGroupNames()) {

				if (!groupName.contains("."))
					continue;

				CounterGroup group = counters.getGroup(groupName);

				sb.append(group.getDisplayName());
				sb.append(System.lineSeparator());
				sb.append(groupName);
				sb.append(System.lineSeparator());

				// aggregate counters
				CounterGroup overallGroup = overallCounters.getGroup(group.getName());
				//				CounterGroup overallGroup = overallCounters.addGroup(group.getName(), group.getDisplayName());


				for (Counter counter : group.getUnderlyingGroup()) {

					sb.append("\t" + counter.getDisplayName() + "=" + counter.getValue());
					sb.append(System.lineSeparator());

					// aggregate counters
					Counter overallCounter = overallGroup.findCounter(counter.getName(), true);
					//					Counter overallCounter = overallGroup.addCounter(counter.getName(), counter.getDisplayName(), 0);
					overallCounter.increment(counter.getValue());

				}

			}

		}


		Counters counters = overallCounters;


		sb.append(System.lineSeparator());
		sb.append("Overall Counters");
		sb.append(System.lineSeparator());
		sb.append("--------------------------------------------------------------------------------");
		sb.append(System.lineSeparator());
		for (String groupName : counters.getGroupNames()) {

			if (!groupName.contains("org.apache.hadoop.mapreduce"))
				continue;

			CounterGroup group = counters.getGroup(groupName);
			sb.append(group.getDisplayName());
			sb.append(System.lineSeparator());

			for (Counter counter : group.getUnderlyingGroup()) {
				sb.append("\t" + counter.getDisplayName() + "=" + counter.getValue() );
				sb.append(System.lineSeparator());
			}
		}

		return sb.toString();

	}
	
}
