package gumbo.engine.hadoop.reporter;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;


/**
 * Contains a list of tuples for a set of Relations.
 * The tuples are divided in two parts: a small set and a large set, determined by a percentage.
 * 
 * @author Jonny Daenen
 *
 */
public class RelationTupleSampleContainer {
	private static final Log LOG = LogFactory.getLog(RelationTupleSampleContainer.class);

	Map<RelationSchema, List<Tuple>> smallset;
	Map<RelationSchema, List<Tuple>> bigset;
	Map<RelationSchema, Long> smallBytes;
	Map<RelationSchema, Long> bigBytes;
	
	double pctSmall;
	private RelationFileMapping mapping;

	public RelationTupleSampleContainer(RelationSampleContainer rsc, double pctSmall) {
		smallset = new HashMap<>();
		bigset = new HashMap<>();
		smallBytes = new HashMap<>();
		bigBytes = new HashMap<>();
		
		this.pctSmall = pctSmall;
		this.mapping = rsc.getMapping();
		
		init(rsc);
	}


	private void init(RelationSampleContainer rsc) {
		
		for (RelationSchema rs: rsc.getRelationSchemas()) {
			smallset.put(rs,new LinkedList<Tuple>());
			bigset.put(rs,new LinkedList<Tuple>());
			
			initStrings(rs,rsc);
		}
		
	}
	
	public void update(RelationSampleContainer rsc) {
		for (RelationSchema rs: rsc.getRelationSchemas()) {
			
			// avoid re-sampling
			if (smallset.containsKey(rs) && bigset.containsKey(rs))
				continue;
			
			smallset.put(rs,new LinkedList<Tuple>());
			bigset.put(rs,new LinkedList<Tuple>());
			
			initStrings(rs,rsc);
		}
	}


	/**
	 * Creates a set of strings, where each string corresponds to one line of the
	 * byte sample. The first line is ignored, as this one may not be complete.
	 * The other lines are scanned until an end-of-line symbol is found.
	 * @param rsc 
	 * 
	 * @return the set of lines in the sample
	 */
	private void initStrings(RelationSchema rs, RelationSampleContainer rsc) {

		LOG.info("Parsing samples for relation " + rs);

		Text t = new Text();
		byte [][] rawbytes = rsc.getSamples(rs);
		
		long smallbytes = 0;
		long bigbytes = 0;
		long smalltuples = 0;
		long bigtuples = 0;
		
		int bound = (int)(pctSmall * rawbytes.length);
		LOG.info("Number of blocks: " + rawbytes.length);
		LOG.info("Number of blocks used for small sample: " + bound);
		
		
		for (int i = 0; i < rawbytes.length; i++) {
			// read text lines
			try(LineReader l = new LineReader(new ByteArrayInputStream(rawbytes[i]))) {
				// skip first line, as it may be incomplete
				l.readLine(t); 

				long bytesread = 0;
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
					} else {
						if (!s.contains(")"))
							continue;
					}
					
					Tuple tuple = new Tuple(s);
					
					if (i < bound) {
						smallbytes += bytesread;
						smallset.get(rs).add(tuple);
						smalltuples++;
					} else {
						bigbytes += bytesread;
						bigset.get(rs).add(tuple);
						bigtuples++;
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		

		LOG.info("Small tuples: " + smalltuples);
		LOG.info("Big tuples: " + bigtuples);
		
		smallBytes.put(rs, smallbytes);
		bigBytes.put(rs, bigbytes);
	}
	
	public List<Tuple> getSmallTuples(RelationSchema rs) {
		return smallset.get(rs);
	}
	
	public List<Tuple> getBigTuples(RelationSchema rs) {
		return bigset.get(rs);
	}
	
	public long getSmallSize(RelationSchema rs) {
		return smallBytes.get(rs);
	}
	
	public long getBigSize(RelationSchema rs) {
		return bigBytes.get(rs);
	}

}
