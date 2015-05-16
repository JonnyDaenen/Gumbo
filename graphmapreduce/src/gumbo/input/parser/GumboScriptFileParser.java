package gumbo.input.parser;

import gumbo.input.GumboQuery;

import java.io.FileInputStream;
import java.io.IOException;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Class to parse a Gumbo script into a GumboQuery
 * @author brentchesny
 *
 */
public class GumboScriptFileParser {

	private static final Log LOG = LogFactory.getLog(GumboScriptFileParser.class);
	
	public class GumboParseException extends Exception {

		private static final long serialVersionUID = 1L;

		public GumboParseException(String msg) {
			super(msg);
		}

		public GumboParseException(String msg, Exception e) {
			super(msg,e);
		}

	}
	
	/**
	 * Constructor method
	 */
	public GumboScriptFileParser() {
		
	}
	
	/**
	 * Parses the given file and returns a {@link GumboQuery} which contains all components. 
	 * 
	 * @param filename the file to parse
	 * @throws GumboParseException 
	 * @throws IOException 
	 */
	public GumboQuery parse(String filename) throws IOException, GumboParseException {
		
		LOG.info("Parsing query file: " + filename);
		
		// create inputstream from filename
		FileInputStream is = new FileInputStream(filename);
		ANTLRInputStream input = new ANTLRInputStream(is);
		
		// create lexer
		GumboLexer lexer = new GumboLexer(input);
		CommonTokenStream tokens = new CommonTokenStream(lexer);
		
		// create parser
		GumboParser parser = new GumboParser(tokens);
		
		// create parsetree, root is script rule
		ParseTree tree = parser.script();
		
		// use visitor to create the actual gumbo query
		GumboScriptVisitor visitor = new GumboScriptVisitor();
		GumboQuery gq = null;
		try {
			gq = visitor.visit(tree);
		} catch (Exception e) {
			throw new GumboParseException("Error parsing file: " + filename, e);
		}
		
		return gq;
	}
}
