package gumbo.generator;

import gumbo.compiler.filemapper.InputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Parses a file with input relations for a GFGenerator
 * @author brentchesny
 *
 */
public class GFGeneratorInputParser {
	
	private static final Log LOG = LogFactory.getLog(GFGeneratorInputParser.class);

	public GFGeneratorInputParser() {
	}
	
	public GFGeneratorInput parse(String filename) {
		String contents = "";
		try {
			contents = readFile(filename);
		} catch (IOException e) {
			LOG.error("Error parsing input file: " + e.getMessage());
			e.printStackTrace();
		}
		
		return parse(contents, false);
	}
	
	public GFGeneratorInput parse(String contents, boolean file) {
		GFGeneratorInput input = new GFGeneratorInput();
		
		String[] sin = contents.split(";");

		String[] dummy;
		for(int i = 0; i < sin.length; i++){
			if (!sin[i].contains("-"))
				continue;

			dummy = sin[i].split("-");
			if (dummy.length != 2) {
				LOG.error("Error while parsing inputrelations on line " + i + ": expecting <Relation>,<Arity> - <Path>,<Type>" );
				return input;
			}

			// schema 
			String[] rsParts = dummy[0].split(",");
			if (rsParts.length != 2){
				LOG.error("Error while parsing inputrelations on line " + i + ": expecting <Relation>,<Arity> - <Path>,<Type>" );
				return input;
			}
			
			// path
			String[] paths = dummy[1].split(",");
			if (paths.length != 2){
				LOG.error("Error while parsing inputrelations on line " + i + ": expecting <Relation>,<Arity> - <Path>,<Type>" );
				return input;
			}
			
			// type
			InputFormat type = InputFormat.REL;
			if (paths[1].trim().toLowerCase().equals("csv")) {
				type = InputFormat.CSV;
			}

			input.addInput(rsParts[0].trim(), Integer.parseInt(rsParts[1].trim()), paths[0].trim(), type);
		}
		
		return input;
	}
	
	private String readFile(String filename) throws IOException {
		BufferedReader bufferedReader = new BufferedReader(new FileReader(filename));

		StringBuffer stringBuffer = new StringBuffer();
		String line = null;

		while((line = bufferedReader.readLine()) != null){
			stringBuffer.append(line).append(System.lineSeparator());
		}
		
		bufferedReader.close();

		return stringBuffer.toString();
	}
	
}
