package gumbo.convertors.pig;

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
 * Class that can be used to convert GF queries to Pig Latin scripts with a wide execution plan
 * 
 * @author brentchesny
 *
 */
public class GFPigConverterWide extends GFPigConverter implements GFVisitor<String> {
	
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
		
		String outname = gfe.getOutputRelation().getName();
		String guardname = gfe.getGuard().getName();
		String guardschema = gfe.getGuard().getRelationSchema().toString().substring(guardname.length());
		List<String> childIds = new ArrayList<>();
		int counter = 1;
		
		String query = "";

		for (GFAtomicExpression child : gfe.getGuardedAtoms()) {
			String guardedGroupSchema = getGuardedGroupSchema(child.getName(), rfm);
			String guardGroupSchema = getGuardGroupSchema(gfe.getGuard().getRelationSchema(), gfe.getGuard().getVars(), rfm, child.getVars());
			String childId = generateAlias(child.toString());
			childIds.add(childId);
			
			String alias = null; 
			if (guardname.equals(child.getName())) {
				alias = "Guarded";
				query += alias + " = FOREACH " + child.getName() + " GENERATE *;" + System.lineSeparator();
			} else 
				alias = child.getName();

			query += outname + "_A" + counter + " = COGROUP " + guardname + " BY " + guardGroupSchema + ", " + alias + " BY " + guardedGroupSchema + ";" + System.lineSeparator();
			query += outname + "_B" + counter + " = FOREACH " + outname + "_A" + counter + " GENERATE flatten(" + guardname + "), (IsEmpty(" + alias + ")? false : true) as " + childId + ";" + System.lineSeparator();
			query += System.lineSeparator();
			
			counter++;
		}
		
		query += outname + "_X = COGROUP ";
		for (int i = 1; i < counter; i++) {
			query += outname + "_B" + i + " by " + guardschema;
			if (i != counter - 1)
				query += ", ";
		}
		query += ";" + System.lineSeparator();
		
		query += outname + "_Y = FOREACH " + outname + "_X GENERATE group";
		for (int i = 1; i < counter; i++) {
			query += ", flatten(" + outname + "_B" + i + "." + childIds.get(i-1) + ")";
		}
		query += ";" + System.lineSeparator();
		
		String boolexpr;
		try {
			boolexpr = gfe.getChild().accept(this);
		} catch (GFVisitorException e) {
			throw new GFConversionException("Error while visiting GFExpression!", e);
		}
		query += outname + "_Z = FILTER " + outname + "_Y BY " + boolexpr + ";" + System.lineSeparator();
		query += outname + "_U = FOREACH " + outname + "_Z GENERATE flatten(group);" + System.lineSeparator();
		
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


	/**
	 * Creates an alias for a relation schema by removing the parentheses and commas
	 * @param string A relation schema
	 * @return An alias for the given relation schema
	 */
	private String generateAlias(String string) {
		return string.replace("(", "_").replace(")", "").replace(",", "");
	}


	/**
	 * Returns the schema by which a guarded relation should be grouped in the pig script
	 * @param name The name of the guarded relation
	 * @param rfm The RelationFileMapping relevant to the query
	 * @return The schema by which a guarded relation should be grouped in the pig script
	 * @throws GFConversionException
	 */
	private String getGuardedGroupSchema(String name, RelationFileMapping rfm) throws GFConversionException {
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

	// for testing purposes only
	public static void main(String[] args) {
		try {
			GFGenerator generator = new GFGenerator();
			generator.addGuardRelation("R", 10, "input/experiments/EXP_008/R", InputFormat.CSV);
			generator.addGuardedRelation("S", 1, "input/experiments/EXP_008/S", InputFormat.CSV);
			generator.addQuery(QueryType.NEGATED_AND, 10);
			GumboQuery gq = generator.generate("queryname");
			
			GFPigConverterWide converter = new GFPigConverterWide();
			String pig = converter.convert(gq);
			System.out.println(pig);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
