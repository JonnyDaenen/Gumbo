package gumbo.input;

import java.util.Collection;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;

import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.gfexpressions.GFExpression;

/**
 * Collection of different query components.
 * 
 * @author Jonny Daenen
 *
 */
public class GumboQuery {

	String name;

	Path output;
	Path scratch;
	RelationFileMapping inputs;
	Collection<GFExpression> queries;	


	public GumboQuery(String name, GFExpression query, RelationFileMapping inputs, Path output, Path scratch) {
		this();

		Collection<GFExpression> queries = new HashSet<>();
		queries.add(query);


		this.name = name;
		this.queries = queries;
		this.output = output;
		this.scratch = scratch;
		this.inputs = inputs;

	}
	public GumboQuery(String name, Collection<GFExpression> queries, RelationFileMapping inputs, Path output, Path scratch) {
		this();
		this.name = name;
		this.queries = queries;
		this.output = output;
		this.scratch = scratch;
		this.inputs = inputs;
	}

	public GumboQuery() {
		name = "DefaultName";
	}
	public Path getOutput() {
		return output;
	}
	public Path getScratch() {
		return scratch;
	}
	public RelationFileMapping getInputs() {
		return inputs;
	}
	public Collection<GFExpression> getQueries() {
		return queries;
	}

	public void setOutput(Path output) {
		this.output = output;
	}

	public void setScratch(Path scratch) {
		this.scratch = scratch;
	}

	public void setInputs(RelationFileMapping inputs) {
		this.inputs = inputs;
	}

	public void setQueries(Collection<GFExpression> queries) {
		this.queries = queries;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {

		StringBuilder sb = new StringBuilder();

		sb.append("Input: " + System.lineSeparator() + inputs);
		sb.append(System.lineSeparator());
		sb.append("Output: " + output);
		sb.append(System.lineSeparator());
		sb.append("Scratch: " + scratch);
		sb.append(System.lineSeparator());

		sb.append("Queries: " + System.lineSeparator() + queries);
		sb.append(System.lineSeparator());

		return sb.toString();
	}

}
