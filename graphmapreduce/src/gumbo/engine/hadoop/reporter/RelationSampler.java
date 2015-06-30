package gumbo.engine.hadoop.reporter;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.data.RelationSchema;
import gumbo.utils.estimation.Sampler;
import gumbo.utils.estimation.SamplingException;


/**
 * Extracts samples from a set of files of a given relation.
 * 
 * @author Jonny Daenen
 *
 */
public class RelationSampler {

	RelationFileMapping mapping;
	
	
	public RelationSampler(RelationFileMapping mapping) {
		this.mapping = mapping;
	}
	
	public RelationSampleContainer sample(int blocksPerFile,int blockSize) throws SamplingException {
		
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
		
		return rsc;
	}

}
