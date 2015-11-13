package gumbo.engine.general.grouper.sample;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.data.RelationSchema;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


/**
 * Contains a list of samples for a set of Relations.
 * 
 * @author Jonny Daenen
 *
 */
public class RelationSampleContainer {

	Map<RelationSchema, byte[][]> samples;
	private RelationFileMapping mapping;

	public RelationSampleContainer() {
		samples = new HashMap<>();
	}

	/**
	 * Couples the provided samples to the given relation.
	 * Any previously given bytes are de-coupled.
	 * 
	 * @param rs the relation schema
	 * @param samples the samples
	 */
	public void setSamples(RelationSchema rs, byte [][] samples) {
		this.samples.put(rs,samples);
	}

	/**
	 * Fetches the samples for the given relation schema.
	 * 
	 * @param rs the relation schema
	 * 
	 * @return samples the samples
	 */
	public byte [][] getSamples(RelationSchema rs) {

		byte [][] sample = samples.get(rs);
		if (sample != null)
			return sample;
		else
			return new byte [0][];
	}
	
	public boolean hasSamplesFor(RelationSchema rs) {
		return samples.containsKey(rs);
	}

	public Collection<RelationSchema> getRelationSchemas() {
		return samples.keySet();
	}

	public void setMapping(RelationFileMapping mapping) {
		this.mapping = mapping;
	}
	
	public RelationFileMapping getMapping() {
		return mapping;
	}

}
