package gumbo.convertors.pig;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.convertors.GFConversionException;
import gumbo.generator.GFGenerator;
import gumbo.generator.QueryType;
import gumbo.input.GumboQuery;
import gumbo.structures.conversion.DNFConverter;
import gumbo.structures.conversion.DNFConverter.DNFConversionException;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.io.Pair;

/**
 * Class that can be used to convert GF queries to Pig Latin scripts with a long execution plan
 * 
 * @author brentchesny
 *
 */
public class GFPigConverterLong extends GFPigConverter {
	
	
	/**
	 * Creates a pig script to evaluate a basic GF expression
	 * @param gfe The basic GF expression to evaluate
	 * @param rfm The RelationFileMapping relevant to this query
	 * @return A pig script to evaluate the query
	 * @throws GFConversionException
	 */
	public String convert(GFExistentialExpression gfe, RelationFileMapping rfm) throws GFConversionException {
		if (!gfe.isBasicGF())
			throw new GFConversionException("The given GFExpression was not a basic expression");
		
		// convert query to disjunctive normal form
		DNFConverter converter = new DNFConverter();
		GFExistentialExpression gfednf;
		try {
			gfednf = (GFExistentialExpression) converter.convert(gfe);
		} catch (DNFConversionException e) {
			throw new GFConversionException("Something went wrong during conversion to DNF!", e);
		}
		
		GFAtomicExpression guard = gfednf.getGuard();
		Set<GFExpression> conjunctions = extractConjunctions(gfednf.getChild());
		
		String query = "";
		
		int counter = 1;
		List<String> unions = new ArrayList<>();
		for (GFExpression conj : conjunctions) {
			Pair<String, String> result = processConjunction(gfe.getOutputRelation().getName(), counter, guard, conj, rfm);

			query += result.fst;
			unions.add(result.snd);
			
			counter++;
		}
		
		// create union of all conjunctions
		if (unions.size() > 1) {
			query += gfe.getOutputRelation().getName() + " = UNION ";
			for (int i = 0; i < unions.size(); i++) {
				query += unions.get(i);
				if (i < unions.size() - 1)
					query += ", ";
			}
			query += ";" + System.lineSeparator();
		} else {
			query += gfe.getOutputRelation().getName() + "_U"  + " = " + unions.get(0) + ";" + System.lineSeparator();
		}
		
		// project on output vars
		String projection = "";
		String[] outVars = gfe.getOutputRelation().getVars();
		String[] guardVars = gfe.getGuard().getVars();
		for (int i = 0; i < outVars.length; i++) {
			for (int j = 0; j < guardVars.length; j++) {
				if (outVars[i].equals(guardVars[j])) {
					projection += ", $" + j + " as x" + i;
				}
			}
		}
		projection = gfe.getOutputRelation().getName() + " = FOREACH " + gfe.getOutputRelation().getName() + "_U GENERATE " + projection.substring(2) + ";";
		
		query += projection + System.lineSeparator();
		query += System.lineSeparator();
		
		return query;
	}
	
	private Pair<String, String> processConjunction(String outname, int id, GFAtomicExpression guard, GFExpression conj, RelationFileMapping rfm) throws GFConversionException {
		String query = "";
		Set<GFExpression> literals = extractLiterals(conj);
		int counter = 1;
		String lastRelName = guard.getName();
		
		for (GFExpression literal : literals) {
			String negation = "not ";
			GFAtomicExpression ae;
			if (literal instanceof GFNotExpression) {
				negation = "";
				ae = (GFAtomicExpression) ((GFNotExpression) literal).getChild(); 
			} else {
				ae = (GFAtomicExpression) literal;
			}
			
			String literalGroupSchema = getLiteralGroupSchema(ae.getName(), rfm);
			String lastRelGroupSchema = getGuardGroupSchema(guard.getRelationSchema(), guard.getVars(), rfm, ae.getVars());
			
			String alias = null; 
			if (guard.getName().equals(ae.getName())) {
				alias = "Guarded";
				query += alias + " = FOREACH " + ae.getName() + " GENERATE *;" + System.lineSeparator();
			} else 
				alias = ae.getName();
			
			query += outname + "_" + id + "_X" + counter + " = COGROUP " + lastRelName + " BY " + lastRelGroupSchema + ", " + alias + " BY " + literalGroupSchema + ";" + System.lineSeparator();
			query += outname + "_" + id + "_Y" + counter + " = FILTER " + outname + "_" + id + "_X" + counter + " BY " + negation + "IsEmpty(" +  alias + ");" + System.lineSeparator();
			query += outname + "_" + id + "_Z" + counter + " = FOREACH " + outname + "_" + id + "_Y" + counter + " GENERATE flatten(" + lastRelName + ");" + System.lineSeparator();
			query += System.lineSeparator();
			
			lastRelName = outname + "_" + id + "_Z" + counter;
			counter++;
		}
		
		return new Pair<String, String>(query, lastRelName);
	}

	private Set<GFExpression> extractConjunctions(GFExpression expr) {
		Set<GFExpression> conjunctions = new HashSet<>();
		
		if (expr instanceof GFOrExpression) {
			GFOrExpression orexpr = (GFOrExpression) expr;
			conjunctions.addAll(extractConjunctions(orexpr.getChild1()));
			conjunctions.addAll(extractConjunctions(orexpr.getChild2()));
		} else {
			conjunctions.add(expr);
		}
		
		return conjunctions;		
	}
	
	private Set<GFExpression> extractLiterals(GFExpression expr) {
		Set<GFExpression> literals = new HashSet<>();
		
		if (expr instanceof GFAndExpression) {
			GFAndExpression andexpr = (GFAndExpression) expr;
			literals.addAll(extractLiterals(andexpr.getChild1()));
			literals.addAll(extractLiterals(andexpr.getChild2()));
		} else {
			literals.add(expr);
		}
		
		return literals;		
	}
	
	/**
	 * Returns the schema by which the guarded relation should be grouped in the pig script
	 * @param name The name of the guarded relation
	 * @param rfm The RelationFileMapping relevant to the query
	 * @return The schema by which a guarded relation should be grouped in the pig script
	 * @throws GFConversionException
	 */
	private String getLiteralGroupSchema(String name, RelationFileMapping rfm) throws GFConversionException {
		String schema = null;
		
		for (RelationSchema rs : rfm.getSchemas()) {
			if (rs.getName().equals(name)) {
				schema = rs.toString().substring(name.length());
				return schema;
			}
		}
		
		for (RelationSchema rs : _outSchemas) {
			if (rs.getName().equals(name)) {
				schema = rs.toString().substring(name.length());
				return schema;
			}
		}
		
		throw new GFConversionException("No schema found with name: " + name);
	}
	
	private String getGuardGroupSchema(RelationSchema guardSchema, String[] guardVars, RelationFileMapping rfm, String[] vars) {
		String schema = "";
		
		for (String var : vars) {
			for (int i = 0; i < guardVars.length; i++) {
				if (guardVars[i].equals(var)) {
					schema += "," + guardSchema.getFields()[i];
				}
			}
		}
		
		schema = "(" + schema.substring(1) + ")";
		
		return schema;
	}

	// for testing purposes only
	public static void main(String[] args) {
		try {
			GFGenerator generator = new GFGenerator();
			generator.addGuardRelation("R", 10, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator.addQuery(QueryType.NEGATED_AND, 10);
			GumboQuery gq = generator.generate("queryname");

			GFPigConverterLong converter = new GFPigConverterLong();
			String pig = converter.convert(gq);
			System.out.println(pig);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
