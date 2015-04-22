/**
 * Created: 16 Feb 2015
 */
package gumbo.input;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.DeserializeException;
import gumbo.structures.gfexpressions.io.GFPrefixSerializer;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

/**
 * Simple class to load query from file.
 * 
 * @author Jonny Daenen
 *
 */
public class GumboFileParser {


	private static final Log LOG = LogFactory.getLog(GumboFileParser.class);
	GFPrefixSerializer parser;

	public class MissingQueryComponentException extends Exception {

		private static final long serialVersionUID = 1L;

		public MissingQueryComponentException(String msg) {
			super(msg);
		}

		public MissingQueryComponentException(String msg, Exception e) {
			super(msg,e);
		}

	}

	public GumboFileParser() {
		parser = new GFPrefixSerializer();
	}




	/**
	 * Parses the given file and returns a {@link GumboQuery} which contains all components. 
	 * 
	 * @param filename the file to parse
	 * @throws MissingQueryComponentException 
	 * @throws IOException 
	 */
	public GumboQuery parse(String filename) throws MissingQueryComponentException, IOException {

		// create object to collect different query components
		GumboQuery gq = new GumboQuery();

		// read file
		File file = new File(filename);
		List<String> content = Files.readAllLines(file.toPath(), StandardCharsets.US_ASCII);
		
		// set query name to file name
		gq.setName(file.getName());

		// collections of lines
		String output = null;
		String scratch = null;
		Set<String> inputs = new HashSet<String>();
		Set<String> queries = new HashSet<String>();

		// for each line 
		for (String line : content) {
			line = line.trim();
			// if it starts with //, -- or #, ignore 
			if (line.startsWith("//") || line.startsWith("--") || line.isEmpty())
				continue;
			// if contains Output ->, it is output path
			else if (line.startsWith("Output ->"))
				output = line;
			// if contains Scratch ->, it is scratch path
			else if (line.startsWith("Scratch ->"))
				scratch = line;
			// if contains <-, it is scratch path
			else if (line.contains("<-"))
				inputs.add(line);
			// else it must be a query
			else
				queries.add(line);
		}

		// process components
		setOutput(gq,output);
		setScratch(gq,scratch);
		setInput(gq,inputs);
		setQueries(gq,queries);

		return gq;
	}


	private void setQueries(GumboQuery gq, Set<String> queries) throws MissingQueryComponentException {

		try {

			Collection<GFExpression> querieset = new HashSet<>();

			// parse queries one by one
			// CLEAN is there an advantage in assembling a set first?
			for (String queryString : queries) {
//				System.out.println(queryString);
				GFExpression query = parser.deserialize(queryString);
				querieset.add(query);

			}
			// add to query structure
			gq.setQueries(querieset);

		} catch (DeserializeException e) {
			LOG.error("Error during query parsing: " + e.getMessage());
			e.printStackTrace();
			throw new MissingQueryComponentException("Error during query parsing.",e);
		}

	}


	private void setInput(GumboQuery gq, Set<String> inputs) throws MissingQueryComponentException {
		// create mapping
		RelationFileMapping rm = new RelationFileMapping();


		for (String input : inputs) {

			String[] splits = input.split("<-");

			// split into relationschema and file
			if (splits.length != 2)
				throw new MissingQueryComponentException("Wrongly formatted input line: " + input);
			else {
				// split schema into name and arity
				String[] schemaparts = splits[0].trim().split(",");
				if (schemaparts.length != 2)
					throw new MissingQueryComponentException("Wrongly formatted input line: " + input + ". schema needs arity, e.g. \"R,10\".");

				RelationSchema rs = new RelationSchema(schemaparts[0], Integer.parseInt(schemaparts[1]));

				// split path into location and type
				String [] pathparts = splits[1].trim().split(",");
				if (pathparts.length != 2)
					throw new MissingQueryComponentException("Wrongly formatted input line: " + input + ". Path needs location and type, e.g. \"a/b/c, csv\".");

				// input type
				InputFormat iF;
				if (pathparts[1].trim().toLowerCase().equals("csv")) {
					iF = InputFormat.CSV;
				} else
					iF = InputFormat.REL;

				// FUTURE separate multiple paths with ","
				Path p = new Path(pathparts[0]);

				rm.addPath(rs, p, iF);
			}
		}


		// add to query structure
		gq.setInputs(rm);

	}


	private void setScratch(GumboQuery gq, String scratch) throws MissingQueryComponentException {
		// if not present: exception
		if (scratch == null) {
			throw new MissingQueryComponentException("Missing scratch path.");
		}
		
		scratch = scratch.split("->")[1].trim();
		// otherwise, add to query structure
		gq.setScratch(new Path(scratch));

	}


	private void setOutput(GumboQuery gq, String output) throws MissingQueryComponentException {
		// if not present: exception
		if (output == null) {
			throw new MissingQueryComponentException("Missing output path.");
		}

		output = output.split("->")[1].trim();
		// otherwise, add to query structure
		gq.setOutput(new Path(output));
	}



}
