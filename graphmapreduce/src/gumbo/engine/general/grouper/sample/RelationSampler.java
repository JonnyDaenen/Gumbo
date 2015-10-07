package gumbo.engine.general.grouper.sample;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.data.RelationSchema;
import gumbo.utils.estimation.Sampler;
import gumbo.utils.estimation.SamplingException;

import org.apache.hadoop.fs.Path;


/**
 * Extracts samples from a set of files of a given relation.
 * 
 * @author Jonny Daenen
 *
 */
public class RelationSampler {

	RelationFileMapping mapping;
	int blocksPerFile;
	int blockSize;
	
	
	public RelationSampler(RelationFileMapping mapping) {
		this(mapping, 10, 4096);
	}
	
	public RelationSampler(RelationFileMapping mapping, int blocksPerFile, int blockSize) {
		this.mapping = mapping;
		this.blocksPerFile = blocksPerFile;
		this.blockSize = blockSize;
	}
	
	public RelationSampleContainer sample() throws SamplingException {
		
		RelationSampleContainer rsc = new RelationSampleContainer();
		
		for (RelationSchema rs: mapping.getSchemas()) {
			
			byte [][] allSamples = new byte [mapping.getPaths(rs).size()*blocksPerFile][];
			int k = 0;
			
			for (Path p : mapping.getPaths(rs)) {
				
				byte [][] samples = Sampler.getRandomBlocks(p, blocksPerFile, blockSize);
				for (int i = 0; i < samples.length; i++) {
					allSamples[k++] = samples[i];
				}
				
			}
			
			rsc.setSamples(rs, allSamples);
		}
		
		rsc.setMapping(mapping);
		
		return rsc;
	}

}