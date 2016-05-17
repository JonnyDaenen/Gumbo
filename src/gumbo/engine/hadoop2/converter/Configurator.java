package gumbo.engine.hadoop2.converter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.FileManager;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.engine.general.utils.FileMappingExtractor;
import gumbo.engine.hadoop2.mapreduce.tools.PropertySerializer;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;
import gumbo.structures.gfexpressions.io.Pair;

public class Configurator {
	
	private GumboPlan plan;
	private FileMappingExtractor extractor;
	private FileManager fm;
	
	

	public Configurator(GumboPlan plan, FileManager fm) {
		super();
		this.plan = plan;
		this.fm = fm;
		
		extractor = new FileMappingExtractor();
	}


	public static void addQueries(Configuration conf, Collection<GFExistentialExpression> expressions) {
		GFPrefixSerializer serializer = new GFPrefixSerializer();
		conf.set("gumbo.queries", serializer.serializeSet(expressions));
	}

	public void configure(Job hadoopJob, Set<GFExistentialExpression> expressions) {
		configure(hadoopJob.getConfiguration(), expressions);
	}
	
	public void configure(Configuration conf, Set<GFExistentialExpression> expressions) {

		// control the map split size:
//		conf.set("mapreduce.input.fileinputformat.split.maxsize", ""+128*1024) ;
		
		// queries
		addQueries(conf, expressions);

		// output mapping
		HashMap<String, String> outmap = new HashMap<>();
		for (RelationSchema rs: fm.getOutFileMapping().getSchemas()) {
			for (Path path : fm.getOutFileMapping().getPaths(rs)) {
				outmap.put(rs.getName(), path.toString());
			}
		}
		String outmapString = PropertySerializer.objectToString(outmap);
		conf.set("gumbo.outmap", outmapString);

		// file to id mapping
		HashMap<String, Long> fileidmap = new HashMap<>();
		HashMap<Long, String> idrelmap = new HashMap<>();

		// resolve paths
		extractor.setIncludeOutputDirs(true);
		RelationFileMapping mapping = extractor.extractFileMapping(fm);

		List<Pair<RelationSchema, Path>> rspath = new ArrayList<>();
		// for each rel
		for (RelationSchema rs : mapping.getSchemas()) {
			// determine paths
			Set<Path> paths = mapping.getPaths(rs);
			for (Path p : paths) {
				rspath.add(new Pair<>(rs,p));
			}

		}

		Comparator<Pair<RelationSchema, Path>> comp = new HelpComparator();
		Collections.sort(rspath, comp );

		long i = 0;
		for (Pair<RelationSchema, Path> p : rspath) {
			fileidmap.put(p.snd.toString(), i);
			idrelmap.put(i,p.fst.getName());
			i++;
		}

		String fileidmapString = PropertySerializer.objectToString(fileidmap);
		conf.set("gumbo.fileidmap", fileidmapString);


		// file id to relation name mapping
		String idrelmapString = PropertySerializer.objectToString(idrelmap);
		conf.set("gumbo.filerelationmap", idrelmapString);

		// atom-id mapping
		HashMap<String,Integer> atomidmap = new HashMap<>();
		int maxatomid = 0;
		for (GFExistentialExpression exp : expressions) {
			for (GFAtomicExpression atom : exp.getAtomic()) {
				int id = plan.getAtomId(atom);
				atomidmap.put(atom.toString(), id);
				maxatomid = Math.max(id, maxatomid);
			}
		}

		String atomidmapString = PropertySerializer.objectToString(atomidmap);
		conf.set("gumbo.atomidmap", atomidmapString);


		// maximal atom id
		conf.setInt("gumbo.maxatomid", maxatomid);
	}

}
