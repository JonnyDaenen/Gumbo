package gumbo.engine.general.grouper.sample;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.grouper.policies.BestCostBasedGrouper;
import gumbo.structures.data.RelationSchema;
import gumbo.utils.estimation.Sampler;
import gumbo.utils.estimation.SamplingException;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;


/**
 * Extracts samples from a set of files of a given relation.
 * 
 * @author Jonny Daenen
 *
 */
public class RelationSampler {
	
	private static final Log LOG = LogFactory.getLog(RelationSampler.class);

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
			LOG.info("Fetching samples for relation " + rs);
			
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
