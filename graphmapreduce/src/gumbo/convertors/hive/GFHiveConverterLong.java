package gumbo.convertors.hive;

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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Class that converts a GF query to an equivalent hive script with a long query plan
 * 
 * @author brentchesny
 *
 */
public class GFHiveConverterLong extends GFHiveConverter {
	
	
	/**
	 * Creates a hive script to evaluate a basic GF expression
	 * @param gfe The basic GF expression to evaluate
	 * @param rfm The RelationFileMapping relevant to this query
	 * @return A hive script to evaluate the query
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
		
		String outname = gfednf.getOutputRelation().getName();
		GFAtomicExpression guard = gfednf.getGuard();
		Set<GFExpression> conjunctions = extractConjunctions(gfednf.getChild());
		
		String query = "";
		
		int counter = 1;
		List<String> unions = new ArrayList<>();
		List<String> views = new ArrayList<>();
		for (GFExpression conj : conjunctions) {
			Pair<String, List<String> > result = processConjunction(gfe.getOutputRelation().getName(), counter, guard, conj, rfm);

			query += result.fst;
			unions.add(result.snd.get(result.snd.size()-1));
			views.addAll(result.snd);
			
			counter++;
		}
		
		query += "INSERT OVERWRITE TABLE " + outname + System.lineSeparator();
		query += "SELECT " + generateViewSelect(gfednf.getOutputRelation(), gfednf.getGuard()) + System.lineSeparator();
		query += "FROM (" + System.lineSeparator();
		for (int i = 0; i < unions.size(); i++) {
			query += "\tSELECT " + generateSelectGuard(guard.getRelationSchema()) + System.lineSeparator();
			query += "\tFROM " + unions.get(i) + " " + guard.getName() + System.lineSeparator();
			if (i < unions.size() - 1) {
				query += "\tUNION ALL" + System.lineSeparator();
			}
		}
		query += ") " + guard.getName() + ";" + System.lineSeparator();
		query += System.lineSeparator();
	
		for (String view : views) {
			query += "DROP VIEW " + view + ";" + System.lineSeparator();
		}
		
		query += System.lineSeparator();
		
		return query;
	}
	
	private Pair<String, List<String> > processConjunction(String outname, int id, GFAtomicExpression guard, GFExpression conj, RelationFileMapping rfm) throws GFConversionException {
		String query = "";
		Set<GFExpression> literals = extractLiterals(conj);
		int counter = 1;
		String guardName = guard.getName();
		String lastRelName = guard.getName();
		List<String> views = new ArrayList<>();
		
		for (GFExpression literal : literals) {
			boolean negation = true;
			GFAtomicExpression ae;
			if (literal instanceof GFNotExpression) {
				negation = false;
				ae = (GFAtomicExpression) ((GFNotExpression) literal).getChild(); 
			} else {
				ae = (GFAtomicExpression) literal;
			}
			
			String alias = (guardName.equals(ae.getName())) ? "Guarded" : null;
					
			String selectGuard = generateSelectGuard(guard.getRelationSchema());
			String selectGuarded = generateSelectGuarded(ae.getRelationSchema(), negation, alias);
			String join = generateJoin(guard, ae, alias);
			
			query += "CREATE VIEW " + outname + "_" + id + "_CONJ" + counter + " AS" + System.lineSeparator();
			query += "SELECT " + selectGuard + System.lineSeparator();
			query += "FROM " + lastRelName + " " + guardName + " LEFT OUTER JOIN " + (alias == null ? ae.getName() : ae.getName() + " " + alias) + System.lineSeparator();
			query += "ON " + join + System.lineSeparator();
			query += "WHERE " + selectGuarded + ";" + System.lineSeparator();
			
			
			query += System.lineSeparator();
			
			lastRelName = outname + "_" + id + "_CONJ" + counter;
			views.add(lastRelName);
			counter++;
		}
		
		return new Pair<String, List<String> >(query, views);
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
	
	private String generateViewSelect(GFAtomicExpression out, GFAtomicExpression guard) {
		String projection = "";
		String[] outVars = out.getVars();
		String[] guardVars = guard.getVars();
		for (int i = 0; i < outVars.length; i++) {
			for (int j = 0; j < guardVars.length; j++) {
				if (outVars[i].equals(guardVars[j])) {
					projection += ", " + guard.getName() + ".x" + j;
				}
			}
		}
		return projection.substring(2);
	}
	
	private String generateSelectGuarded(RelationSchema schema, boolean negation, String alias) {
		String select = "";
		String name = (alias == null) ? schema.getName() : alias;
		
		String neg = negation ? " NOT" : "";
		
		for(String var : schema.getFields()) {
			select += " and " + name + "." + var + " IS" + neg + " null";
		}
		
		return select.substring(5);
	}
	
	private String generateSelectGuard(RelationSchema schema) {
		String select = "";
		String name = schema.getName();
		
		for(String var : schema.getFields()) {
			select += ", " + name + "." + var;
		}
		
		return select.substring(2);
	}
	
	private String generateJoin(GFAtomicExpression guard, GFAtomicExpression child, String alias) {
		String join = "";
		String[] guardVars = guard.getVars();
		String[] childVars = child.getVars();
		String childName = (alias == null) ? child.getName() : alias;
		
		for (int i = 0; i < childVars.length; i++) {
			for (int j = 0; j < guardVars.length; j++) {
				if (childVars[i].equals(guardVars[j])) {
					join += " and " + guard.getName() + "." + guard.getRelationSchema().getFields()[j] + " = " + childName + "." + child.getRelationSchema().getFields()[i];
				}
			}
		}
		
		return join.substring(5);
	}


	// for testing purposes only
	public static void main(String[] args) {
		try {
			GFGenerator generator = new GFGenerator();
			generator.addGuardRelation("R", 4, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator.addQuery(QueryType.OR, 4);
			GumboQuery gq = generator.generate("queryname");

			GFHiveConverterLong converter = new GFHiveConverterLong();
			String pig = converter.convert(gq);
			System.out.println(pig);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
