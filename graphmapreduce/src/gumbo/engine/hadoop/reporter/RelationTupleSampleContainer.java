package gumbo.engine.hadoop.reporter;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.sample.RelationSampleContainer;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.mortbay.log.Log;


/**
 * Contains a list of tuples for a set of Relations.
 * The tuples are divided in two parts: a small set and a large set, determined by a percentage.
 * 
 * @author Jonny Daenen
 *
 */
public class RelationTupleSampleContainer {

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


	/**
	 * Creates a set of strings, where each string corresponds to one line of the
	 * byte sample. The first line is ignored, as this one may not be complete.
	 * The other lines are scanned until an end-of-line symbol is found.
	 * @param rsc 
	 * 
	 * @return the set of lines in the sample
	 */
	private void initStrings(RelationSchema rs, RelationSampleContainer rsc) {

		Log.info("Parsing samples for relation " + rs);

		Text t = new Text();
		byte [][] rawbytes = rsc.getSamples(rs);
		
		long smallbytes = 0;
		long bigbytes = 0;
		
		int bound = (int)(pctSmall * rawbytes.length);
		
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
					}
					
					Tuple tuple = new Tuple(s);
					
					if (i < bound) {
						smallbytes += bytesread;
						smallset.get(rs).add(tuple);
					} else {
						bigbytes += bytesread;
						bigset.get(rs).add(tuple);
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
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
