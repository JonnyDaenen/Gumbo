package gumbo.engine.hadoop2.estimation;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
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

import gumbo.engine.hadoop2.datatypes.GumboMessageWritable;
import gumbo.engine.hadoop2.datatypes.VBytesWritable;
import gumbo.engine.hadoop2.mapreduce.multivalidate.ValidateMapper;
import gumbo.structures.data.Tuple;

public class DummyMapper extends ValidateMapper {

	public DummyMapper() {
		// TODO Auto-generated constructor stub
	}

	DummyContext getContext(Configuration conf, Iterable<Tuple> tuples, String relationname) {
		return new DummyContext(conf, tuples, relationname);
	}

	public class DummyContext extends Mapper<LongWritable, Text, VBytesWritable, GumboMessageWritable>.Context{

		String relationname;

		long offset;
		Iterable<Tuple> tuples;

		LongWritable key;
		Text value;
		private Iterator<Tuple> it;

		long asserts;
		long requests;

		private long keyBytes;
		private long valueBytes;

		private long assertBytes;
		private long requestBytes;

		private DataOutputBuffer db;
		private Configuration conf;

		public DummyContext(Configuration conf, Iterable<Tuple> tuples, String relationname) {
			this.tuples = tuples;
			it = tuples.iterator();
			this.conf = conf;
			this.relationname = relationname;

			db = new DataOutputBuffer(512);
			resetCounters();
			
			key = new LongWritable();
			value = new Text();
		}

		@Override
		public InputSplit getInputSplit() {
			return null;
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {

			if (!it.hasNext())
				return false;

			key.set(offset);
			value.set(it.next().toString());

			return true;
		}

		@Override
		public LongWritable getCurrentKey() throws IOException,
		InterruptedException {
			return key;
		}

		@Override
		public Text getCurrentValue() throws IOException, InterruptedException {
			return value;
		}

		@Override
		public void write(VBytesWritable key, GumboMessageWritable value) throws IOException,
		InterruptedException {

			// count bytes
			db.reset();
			key.write(db);
			keyBytes += db.getLength();

			db.reset();
			value.write(db);
			long messageBytes = db.getLength();
			valueBytes += messageBytes;

			// count types
			if (value.isAssert()) {
				asserts++;
				assertBytes += messageBytes;
			}
			else if (value.isRequest()) {
				requests++;
				requestBytes += messageBytes;
			}



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
			return null;
		}

		@Override
		public Counter getCounter(String groupName, String counterName) {
			return null;
		}

		@Override
		public Configuration getConfiguration() {
			return conf;
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

		public void resetCounters() {

			keyBytes = 0;
			valueBytes = 0;

			asserts = 0;
			assertBytes = 0;
			requests = 0;
			requestBytes = 0;

		}

		public long getOffset() {
			return offset;
		}

		public long getAsserts() {
			return asserts;
		}

		public long getRequests() {
			return requests;
		}

		public long getKeyBytes() {
			return keyBytes;
		}

		public long getValueBytes() {
			return valueBytes;
		}

		public long getAssertBytes() {
			return assertBytes;
		}

		public long getRequestBytes() {
			return requestBytes;
		}

		public long getFileID() {
			return 0;
		}

		public String getRelationName() {
			return relationname;
		}


	}
}

