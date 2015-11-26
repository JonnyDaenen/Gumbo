package gumbo.engine.hadoop.reporter;

import gumbo.structures.gfexpressions.io.Pair;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.Credentials;

public class FakeMapper extends Mapper<LongWritable, Text, BytesWritable, Text> {

	public class FakeCounter implements Counter {
		long c;
		String name;

		public FakeCounter(String name) {
			this.name = name;
			c = 0;
		}

		@Override
		public void write(DataOutput out) throws IOException {


		}

		@Override
		public void readFields(DataInput in) throws IOException {


		}

		@Override
		public void setDisplayName(String displayName) {
			name = displayName;
		}

		@Override
		public String getName() {
			return name;
		}

		@Override
		public String getDisplayName() {
			return name;
		}

		@Override
		public long getValue() {
			return c;
		}

		@Override
		public void setValue(long value) {
			c = value;

		}

		@Override
		public void increment(long incr) {
			c += incr;

		}

		@Override
		public Counter getUnderlyingCounter() {
			return null;
		}

	}

	public FakeContext context;

	public FakeMapper() {
		context = new FakeContext();
	}

	public class FakeContext extends Mapper<LongWritable, Text, BytesWritable, Text>.Context{

		HashMap<Enum<?>,Counter> counters;
		HashMap<Pair<String, String>,Counter> counters_p;
		
		

		public FakeContext() {
			counters = new HashMap<>();
		}

		@Override
		public InputSplit getInputSplit() {
			return null;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			return false;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
		InterruptedException {
			return null;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return null;
		}

		@Override
		public void write(BytesWritable key, Text value) throws IOException,
		InterruptedException {

		}

		@Override
		public OutputCommitter getOutputCommitter() {
			return null;
		}

		@Override
		public TaskAttemptID getTaskAttemptID() {
			return null;
		}

		@Override
		public void setStatus(String msg) {

		}

		@Override
		public String getStatus() {
			return null;
		}

		@Override
		public float getProgress() {
			return 0;
		}

		@Override
		public Counter getCounter(Enum<?> counterName) {
			if (counters.containsKey(counterName)) {
//				System.out.println("Counter requested for:" + counterName);
//				System.out.println("Counter returned: "+ counters.get(counterName) );
//				System.out.println("Current value: " + counters.get(counterName).getValue());
				return counters.get(counterName); 
			} else {
				Counter counter = new FakeCounter(counterName.toString());
				counters.put(counterName, counter);
				return counter;
			}
		}

		@Override
		public Counter getCounter(String groupName, String counterName) {
			Pair<String, String> p = new Pair<String,String>(groupName, counterName);
			if (counters_p.containsKey(counters_p)) {
				return counters_p.get(counters_p); 
			} else {
				Counter counter = new FakeCounter(groupName + "." + counterName);
				counters_p.put(p, counter);
				return counter;
			}
		}

		@Override
		public Configuration getConfiguration() {
			return null;
		}

		@Override
		public Credentials getCredentials() {
			return null;
		}

		@Override
		public JobID getJobID() {
			return null;
		}

		@Override
		public int getNumReduceTasks() {
			return 0;
		}

		@Override
		public Path getWorkingDirectory() throws IOException {
			return null;
		}

		@Override
		public Class<?> getOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getOutputValueClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputKeyClass() {
			return null;
		}

		@Override
		public Class<?> getMapOutputValueClass() {
			return null;
		}

		@Override
		public String getJobName() {
			return null;
		}

		@Override
		public Class<? extends InputFormat<?, ?>> getInputFormatClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends OutputFormat<?, ?>> getOutputFormatClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public Class<? extends Partitioner<?, ?>> getPartitionerClass()
				throws ClassNotFoundException {
			return null;
		}

		@Override
		public RawComparator<?> getSortComparator() {
			return null;
		}

		@Override
		public String getJar() {
			return null;
		}

		@Override
		public RawComparator<?> getCombinerKeyGroupingComparator() {
			return null;
		}

		@Override
		public RawComparator<?> getGroupingComparator() {
			return null;
		}

		@Override
		public boolean getJobSetupCleanupNeeded() {
			return false;
		}

		@Override
		public boolean getTaskCleanupNeeded() {
			return false;
		}

		@Override
		public boolean getProfileEnabled() {
			return false;
		}

		@Override
		public String getProfileParams() {
			return null;
		}

		@Override
		public IntegerRanges getProfileTaskRange(boolean isMap) {
			return null;
		}

		@Override
		public String getUser() {
			return null;
		}

		@Override
		public boolean getSymlink() {
			return false;
		}

		@Override
		public Path[] getArchiveClassPaths() {
			return null;
		}

		@Override
		public URI[] getCacheArchives() throws IOException {
			return null;
		}

		@Override
		public URI[] getCacheFiles() throws IOException {
			return null;
		}

		@Override
		public Path[] getLocalCacheArchives() throws IOException {
			return null;
		}

		@Override
		public Path[] getLocalCacheFiles() throws IOException {
			return null;
		}

		@Override
		public Path[] getFileClassPaths() {
			return null;
		}

		@Override
		public String[] getArchiveTimestamps() {
			return null;
		}

		@Override
		public String[] getFileTimestamps() {
			return null;
		}

		@Override
		public int getMaxMapAttempts() {
			return 0;
		}

		@Override
		public int getMaxReduceAttempts() {
			return 0;
		}

		@Override
		public void progress() {

		}

		public double getInputTuples() {
			return getCounter(CounterMeasures.IN_TUPLES).getValue();
		}

		public double getOutputTuples() {
			return getCounter(CounterMeasures.OUT_TUPLES).getValue();
		}

		public double getOutputBytes() {
			return getCounter(CounterMeasures.OUT_BYTES).getValue();
		}

		public double getOutputKeyBytes() {
			return getCounter(CounterMeasures.OUT_KEY_BYTES).getValue();
		}

		public double getOutputValueBytes() {
			return getCounter(CounterMeasures.OUT_VALUE_BYTES).getValue();
		}

	}
}
