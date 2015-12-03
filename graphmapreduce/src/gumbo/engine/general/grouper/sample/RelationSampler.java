package gumbo.engine.general.grouper.sample;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
		sample(rsc);
		return rsc;
	}
	
	public void sample(RelationSampleContainer rsc) throws SamplingException {
		
		for (RelationSchema rs: mapping.getSchemas()) {
			
			if (rsc.hasSamplesFor(rs)) {
				LOG.info("Avoiding re-sample for " + rs);
				continue;
			}
			
			LOG.info("Fetching samples for relation " + rs);
			
			Set<Path> paths = selectPaths(mapping.getPaths(rs), 10);
			
			byte [][] allSamples = new byte [paths.size()*blocksPerFile][];
			int k = 0;
			
			for (Path p : paths) {
				
				byte [][] samples = Sampler.getRandomBlocks(p, blocksPerFile, blockSize);
				for (int i = 0; i < samples.length; i++) {
					allSamples[k++] = samples[i];
				}
				
			}
			
			// add samples to collection
			if (k > 0)
				rsc.setSamples(rs, allSamples);
		}
		
		rsc.setMapping(mapping);
		
	}

	/**
	 * Selects a subset of paths with an upper bound max.
	 * @param paths
	 * @param i
	 * @return
	 */
	private Set<Path> selectPaths(Set<Path> paths, int max) {
		
		if (paths.size() <= max)
			return paths;

		Set<Path> resultSet = new HashSet<Path>();
		Path[] patharray = paths.toArray(new Path[0]);
		for (int i = 0; i < max; i++) {
			Path p = patharray[(int) (Math.random()*patharray.length)];
			resultSet.add(p);
		}
		
		return resultSet;
	}

}
