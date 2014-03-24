package guardedfragment.mapreduce.mappers;

import guardedfragment.data.Projection;
import guardedfragment.data.RelationSchema;
import guardedfragment.structure.MyTuple;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ProjectionMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	Map<RelationSchema, Projection> projections;
	boolean passNonMatches;
	
	
	public ProjectionMapper(Map<RelationSchema, Projection> projections, boolean passNonMatches) {
		super();
		this.projections = projections;
		this.passNonMatches = passNonMatches;
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// convert value to tuple
		MyTuple t = new MyTuple(value.toString());
		
		RelationSchema s = t.extractSchema();
		
		// if there is a mapping present, we map it
		if (projections.containsKey(s)) {
			
			// get projection
			Projection projection = projections.get(s);
			
			// extract relation from tuple
			MyTuple newTuple = projection.project(t);
			
			// lookup projection associated with relation
			String newkey = newTuple.generateString();
			String newvalue = t.generateString();
			context.write(new Text(newkey),new Text(newvalue));
		}
		// if not we just pass the tuple, if enabled
		else if (passNonMatches){
			
			// we don't use original values because it may be in the wrong format...
			String newvalue = t.generateString();
			context.write(new Text(newvalue),new Text(newvalue));
			
		}
		
		
		
		
	}

}
