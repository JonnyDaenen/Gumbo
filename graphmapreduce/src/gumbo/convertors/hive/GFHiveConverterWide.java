package gumbo.convertors.hive;

import java.util.ArrayList;
import java.util.List;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.convertors.GFConversionException;
import gumbo.generator.GFGenerator;
import gumbo.generator.QueryType;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFAndExpression;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.GFNotExpression;
import gumbo.structures.gfexpressions.GFOrExpression;
import gumbo.structures.gfexpressions.GFVisitor;
import gumbo.structures.gfexpressions.GFVisitorException;



/**
 * Class that converts a GF query to an equivalent hive script with a wide query plan
 * 
 * @author brentchesny
 *
 */
public class GFHiveConverterWide extends GFHiveConverter implements GFVisitor<String> {

	@Override
	public String convert(GFExistentialExpression gfe, RelationFileMapping rfm) throws GFConversionException {
		if (!gfe.isBasicGF())
			throw new GFConversionException("The given GFExpression was not a basic expression");
		
		
		String outname = gfe.getOutputRelation().getName();
		List<String> childIds = new ArrayList<>();
		int counter = 1;
		
		String query = "";

		for (GFAtomicExpression child : gfe.getGuardedAtoms()) {
			String childId = generateAlias(child.toString());
			childIds.add(childId);
			
			String alias = (gfe.getGuard().getName().equals(child.getName())) ? "Guarded" : null;
			
			String selectGuard = generateSelectGuard(gfe.getGuard().getRelationSchema());
			String selectGuarded = generateSelectGuarded(child.getRelationSchema(), alias);
			String join = generateJoin(gfe.getGuard(), child, alias);
			
			query += "CREATE VIEW " + outname + "_CONJ" + counter + " AS" + System.lineSeparator();
			query += "SELECT " + selectGuard + "CASE WHEN " + selectGuarded + " THEN false ELSE true END as " + childId + System.lineSeparator();
			query += "FROM " + gfe.getGuard().getName() + " LEFT OUTER JOIN " + (alias == null ? child.getName() : child.getName() + " " + alias) + System.lineSeparator();
			query += "ON " + join + ";" + System.lineSeparator();
			query += System.lineSeparator();
						
			counter++;
		}
		
		query += "INSERT OVERWRITE TABLE " + outname + System.lineSeparator();
		query += "SELECT " + generateViewSelect(gfe.getOutputRelation(), gfe.getGuard()) + System.lineSeparator();
		query += "FROM " + outname + "_CONJ1" + System.lineSeparator();
		for (int i = 2; i < counter; i++) {
			query += "LEFT OUTER JOIN " + outname + "_CONJ" + i + System.lineSeparator();
			query += "ON " + generateViewJoin(outname, gfe.getGuard(), i) + System.lineSeparator();
		}
		
		String boolexpr;
		try {
			boolexpr = gfe.getChild().accept(this);
		} catch (GFVisitorException e) {
			throw new GFConversionException("Error while visiting GFExpression!", e);
		}
		
		query += "WHERE " + boolexpr + ";" + System.lineSeparator(); 
		query += System.lineSeparator();
		
		for (int i = 1; i < counter; i++) {
			query += "DROP VIEW " + outname + "_CONJ" + i + ";" + System.lineSeparator();
		}
		
		query += System.lineSeparator();
		
		return query;
	}
	
	private String generateViewSelect(GFAtomicExpression out, GFAtomicExpression guard) {
		String projection = "";
		String[] outVars = out.getVars();
		String[] guardVars = guard.getVars();
		for (int i = 0; i < outVars.length; i++) {
			for (int j = 0; j < guardVars.length; j++) {
				if (outVars[i].equals(guardVars[j])) {
					projection += ", " + out.getName() + "_CONJ1.x" + j;
				}
			}
		}
		return projection.substring(2);
	}

	private String generateViewJoin(String outname, GFAtomicExpression guard, int counter) {	
		String join = "";
		
		String[] fields = guard.getRelationSchema().getFields();
		for (String field : fields) {
			join += " and " + outname + "_CONJ1" + "." + field + " = " + outname + "_CONJ" + counter + "." + field;
		}
		
		return "(" + join.substring(5) + ")";
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

	private String generateSelectGuarded(RelationSchema schema, String alias) {
		String select = "";
		String name = (alias == null) ? schema.getName() : alias;
		
		for(String var : schema.getFields()) {
			select += " and " + name + "." + var + " IS null";
		}
		
		return select.substring(5);
	}

	private String generateSelectGuard(RelationSchema schema) {
		String select = "";
		String name = schema.getName();
		
		for(String var : schema.getFields()) {
			select += name + "." + var + ", ";
		}
		
		return select;
	}

	/**
	 * Creates an alias for a relation schema by removing the parentheses and commas
	 * @param string A relation schema
	 * @return An alias for the given relation schema
	 */
	private String generateAlias(String string) {
		return string.replace("(", "_").replace(")", "").replace(",", "");
	}	

	@Override
	public String visit(GFExpression e) throws GFVisitorException {
		throw new GFVisitorException("Unknown expression type!");
	}


	@Override
	public String visit(GFAtomicExpression e) throws GFVisitorException {
		return generateAlias(e.toString());
	}


	@Override
	public String visit(GFAndExpression e) throws GFVisitorException {
		return "(" + e.getChild1().accept(this) + " and " + e.getChild2().accept(this) + ")";
	}


	@Override
	public String visit(GFOrExpression e) throws GFVisitorException {
		return "(" + e.getChild1().accept(this) + " or " + e.getChild2().accept(this) + ")";
	}


	@Override
	public String visit(GFNotExpression e) throws GFVisitorException {
		return "(not " + e.getChild().accept(this) + ")";
	}


	@Override
	public String visit(GFExistentialExpression e) throws GFVisitorException {
		throw new GFVisitorException("GFExistentialExpression encountered in basic GF expression!");
	}

	// for testing-purposes only
	public static void main(String[] args) {
		try {
			GFGenerator generator = new GFGenerator();
			generator.addGuardRelation("R", 10, "/user/cloudera/input/query0/R", InputFormat.CSV);
			generator.addGuardedRelation("S", 1, "/user/cloudera/input/query0/S", InputFormat.CSV);
			generator.addQuery(QueryType.NEGATED_AND, 10);
			GumboQuery gq = generator.generate("convertertest", "/user/cloudera/output/", "/user/cloudera/scratch/");

			GFHiveConverterWide converter = new GFHiveConverterWide();
			String pig = converter.convert(gq);
			System.out.println(pig);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
