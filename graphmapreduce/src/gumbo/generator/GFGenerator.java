package gumbo.generator;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.generator.GFGeneratorInput.Relation;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFXorExpression;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;

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
	
	public void addInput(GFGeneratorInput input) {
		boolean guardAdded = false;
		for (Relation relation : input) {
			if (!guardAdded) {
				addGuardRelation(relation.name, relation.arity, relation.path, relation.format);
				guardAdded = true;
			}
			else 
				addGuardedRelation(relation.name, relation.arity, relation.path, relation.format);
		}
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
		if (_guardeds.size() < 1)
			throw new GFGeneratorException("Please provide atleast 1 guarded relation.");
		
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
	
	/**
	 * Method that creates queries that select all tuples for which exactly one of the variables appears in the given guarded relations
	 * e.g.: R(x1, x2) & ((S(x1) & !S(x2)) | ((!S(x1) & S(x2))
	 * 
	 * @throws GFGeneratorException
	 */
	public void addUniqueQuery() throws GFGeneratorException {
		if (!unaryGuardedCheck())
			throw new GFGeneratorException("Please provide atleast one unary guarded relation for this type of query.");
		
		RelationSchema guard = _guards.get(0); 
		String[] guardFields = guard.getFields();
				
		GFAtomicExpression guardexpr = new GFAtomicExpression(guard.getName(), guardFields.clone());
		GFAtomicExpression outputexpr = createOutputExpression(guard);
		GFExpression childexpr = null;
		int guardedId = 0;
		
		for (int i = 0; i < guard.getNumFields(); i++) {
			GFExpression andexpr = null; 
			for (int j = 0; j < guard.getNumFields(); j++) {
				RelationSchema guarded = null;
				while (guarded == null || guarded.getNumFields() != 1) {
					guarded = _guardeds.get(guardedId);
					guardedId = (guardedId + 1) % _guardeds.size();
				}
				
				String field = guardFields[j];
				GFExpression atom = new GFAtomicExpression(guarded.getName(), field);
				if (i != j)
					atom = new GFNotExpression(atom);
				
				andexpr = createSubExpressionOfType(QueryType.AND, andexpr, atom);
			}
			childexpr = createSubExpressionOfType(QueryType.OR, childexpr, andexpr);
		}
		
		_queries.add(new GFExistentialExpression(guardexpr, childexpr, outputexpr));
	}

	private boolean unaryGuardedCheck() {
		for (RelationSchema guarded : _guardeds) {
			if (guarded.getNumFields() == 1)
				return true;
		}
		
		return false;
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
			
			GFGenerator generator1 = new GFGenerator();
			generator1.addGuardRelation("R", 10, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator1.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator1.addQuery(QueryType.AND, 10);
			generator1.addQuery(QueryType.NEGATED_OR, 4);
			GumboQuery query1 = generator1.generate("GeneratorTest");
			System.out.println(query1);
			
			System.out.println("-----------------------");
			
			GFGenerator generator2 = new GFGenerator();
			generator2.addGuardRelation("R", 3, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator2.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator2.addGuardedRelation("T", 1, "input/experiments/EXP_008/T", InputFormat.CSV);
			generator2.addGuardedRelation("U", 1, "input/experiments/EXP_008/U", InputFormat.CSV);
			generator2.addUniqueQuery(); 
			GumboQuery query2 = generator2.generate("UniqueQuery");
			System.out.println(query2);
			
		} catch (GFGeneratorException e) {
			e.printStackTrace();
		}
	}

}
