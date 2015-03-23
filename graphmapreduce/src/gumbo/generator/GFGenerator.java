package gumbo.generator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFXorExpression;

/**
 * Generator class which can be used to easily generate GF queries
 * @author brentchesny
 *
 */
public class GFGenerator {
	
	private static final String DEFAULT_OUTPUT_DIR = "output/";
	private static final String DEFAULT_SCRATCH_DIR = "scratch/";

	private AtomicInteger _id;
	private RelationFileMapping _rfm;
	private Collection<GFExpression> _queries;
	private List<RelationSchema> _guards;
	private List<RelationSchema> _guardeds;
	
	/**
	 * Constructor method
	 */
	public GFGenerator() {
		_id = new AtomicInteger();
		_rfm = new RelationFileMapping();
		_queries = new HashSet<>();
		_guards = new ArrayList<>();
		_guardeds = new ArrayList<>();
	}
	
	/**
	 * Add an inputrelation to the generator as a guard
	 * @param relname The relation name
	 * @param arity The arity of the relation
	 * @param path The path of the relation
	 */
	public void addGuardRelation(String relname, int arity, String path) {
		this.addGuardRelation(relname, arity, path, InputFormat.REL);
	}
	
	/**
	 * Add an input relation to the generator as a guard
	 * @param relname The relation name
	 * @param arity The arity of the relation
	 * @param path The path of the relation
	 * @param format The format of the relation, either CSV of REL
	 */
	public void addGuardRelation(String relname, int arity, String path, InputFormat format) {
		RelationSchema rs = new RelationSchema(relname, arity);
		_guards.add(rs);
		_rfm.addPath(rs, new Path(path), format);
	}
	
	/**
	 * Add an inputrelation to the generator as a guarded relation
	 * @param relname The relation name
	 * @param arity The arity of the relation
	 * @param path The path of the relation
	 */
	public void addGuardedRelation(String relname, int arity, String path) {
		this.addGuardedRelation(relname, arity, path, InputFormat.REL);
	}
	
	/**
	 * Add an inputrelation to the generator as a guarded relation
	 * @param relname The relation name
	 * @param arity The arity of the relation
	 * @param path The path of the relation
	 * @param format The format of the relation, either CSV of REL
	 */
	public void addGuardedRelation(String relname, int arity, String path, InputFormat format) {
		RelationSchema rs = new RelationSchema(relname, arity);
		_guardeds.add(rs);
		_rfm.addPath(rs, new Path(path), format);
	}
	
	/**
	 * Add a query to the generator
	 * @param type The type of query to generate
	 * @param arity The number of atoms in the generated query
	 * @throws GFGeneratorException Throws an exception if the arity is not strictly positive, or if the querytype is unknown
	 */
	public void addQuery(QueryType type, int arity) throws GFGeneratorException {
		if (arity < 1)
			throw new GFGeneratorException("Please provide an arity of atleast 1.");
		
		RelationSchema guard = _guards.get(0); // get guard relation (can be changed to not only get the first one in the future)
		String[] guardFields = guard.getFields();
				
		GFAtomicExpression guardexpr = new GFAtomicExpression(guard.getName(), guardFields.clone());
		GFAtomicExpression outputexpr = createOutputExpression(guard);
		GFExpression childexpr = null;
		
		int currentGuardedId = 0;
		int currentGuardFieldId = 0;
		for (int i = 0; i < arity; i++) {
			RelationSchema guarded = _guardeds.get(currentGuardedId);
			currentGuardedId = (currentGuardedId + 1) % _guardeds.size();
			
			String[] fields = new String[guarded.getNumFields()];
			for (int j = 0; j < guarded.getNumFields(); j++) {
				fields[j] = guardFields[currentGuardFieldId];
				currentGuardFieldId = (currentGuardFieldId + 1) % guardFields.length;
			}
			
			GFExpression atom = new GFAtomicExpression(guarded.getName(), fields);
			
			childexpr = createSubExpressionOfType(type, childexpr, atom);
		}
		
		_queries.add(new GFExistentialExpression(guardexpr, childexpr, outputexpr));
	}
	
	private GFExpression createSubExpressionOfType(QueryType type, GFExpression childexpr, GFExpression atom) throws GFGeneratorException {
		GFExpression expr = null;
		
		if (type == QueryType.NEGATED_AND || type == QueryType.NEGATED_OR || type == QueryType.NEGATED_XOR)
			atom = new GFNotExpression(atom);
		
		if (childexpr == null)
			return atom;
		
		switch (type) {
		case NEGATED_AND:
		case AND:
			expr = new GFAndExpression(childexpr, atom);
			break;
		case NEGATED_OR:
		case OR:
			expr = new GFOrExpression(childexpr, atom);
			break;
		case NEGATED_XOR:
		case XOR:
			expr = new GFXorExpression(childexpr, atom);
			break;
		default:
			throw new GFGeneratorException("Unknown query type.");
		}
		
		return expr;
	}

	private GFAtomicExpression createOutputExpression(RelationSchema guard) {
		String[] fields = guard.getFields().clone();
		String outname = "Out" + _id.getAndIncrement();
		
		return new GFAtomicExpression(outname, fields);
	}
	
	/**
	 * Generates a GumboQuery based on the input relations and queries fed to the generator
	 * @param queryname The name of the generated GumboQuery
	 * @return  The generated GumboQuery
	 */
	public GumboQuery generate(String queryname) {
		return this.generate(queryname, DEFAULT_OUTPUT_DIR, DEFAULT_SCRATCH_DIR); 
	}
	
	public GumboQuery generate(String queryname, String outputdir, String scratchdir) {
		return new GumboQuery(queryname, _queries, _rfm, new Path(outputdir + queryname), new Path(scratchdir + queryname)); 
	}
	
	// for testing purposes only
	public static void main(String[] args) {
		try {
			GFGenerator generator = new GFGenerator();
			generator.addGuardRelation("R", 10, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator.addQuery(QueryType.AND, 10);
			generator.addQuery(QueryType.NEGATED_OR, 4);
			
			GumboQuery gq = generator.generate("GeneratorTest");
			System.out.println(gq);
		} catch (GFGeneratorException e) {
			e.printStackTrace();
		}
	}

}
