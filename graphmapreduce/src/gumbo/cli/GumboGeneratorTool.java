package gumbo.cli;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

import gumbo.generator.GFGenerator;
import gumbo.generator.GFGeneratorException;
import gumbo.generator.GFGeneratorInput;
import gumbo.generator.GFGeneratorInputParser;
import gumbo.generator.QueryType;
import gumbo.input.parser.GumboExporter;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GumboGeneratorTool implements GumboCommandLineTool {

private static final Log LOG = LogFactory.getLog(GumboGeneratorTool.class);
	
	private Options _options;

	public GumboGeneratorTool() {
		_options = new Options();
		_options.addOption("h", "help", false, "Show usage information");
		_options.addOption("n", "name", true, "An optional name for the generated query.");
		_options.addOption("i", "input", true, "File that contains the inputrelations to use.");
		_options.addOption("o", "output", true, "File to write the output to. If no output file is specified, the generated script will be printed to the console.");
		
		Option queriesOption = new Option("q", "queries", true, "Queries to generate. Eeach query requires a type followed by the number of atoms. The type can be any of the following: 'and', 'or', 'xor', 'negated_and', 'negated_or', 'negated_xor'. There is also a type called 'unique' that does not require the number of atoms to be specified.");
		queriesOption.setArgs(Option.UNLIMITED_VALUES);
		_options.addOption(queriesOption);
	}
	
	@Override
	public void run(String[] args) {
		// create the parser
	    CommandLineParser parser = new BasicParser();
	    CommandLine cmd = null;
	    
	    try {
	        // parse the command line arguments
	        cmd = parser.parse(_options, args);
	    } catch(ParseException e) {
	        LOG.error("Failed to parse commandline options: " + e.getMessage());
	        return;
	    }
	    
	    if (cmd.hasOption("h") || args.length <= 1) {
	    	help();
	    }
	    
	    if (!cmd.hasOption("i")) {
	    	LOG.error("No file with inputrelations specified!");
	    	return;
	    }
	    
	    String input = cmd.getOptionValue("i");

    	LOG.info("Parsing input relations");	    
	    GFGeneratorInputParser generatorparser = new GFGeneratorInputParser();
	    GFGeneratorInput relations = generatorparser.parse(input);	    
	    
	    
	    GFGenerator generator = new GFGenerator();
	    GumboExporter exporter = new GumboExporter();
	    
    	LOG.info("Adding input relations to generator");	    
	    generator.addInput(relations);
	
    	LOG.info("Parsing queries");	    
	    String[] queryArgs = cmd.getOptionValues("q");
	    
	    try {
	    	for (int i = 0; i < queryArgs.length; i++) {
	        	LOG.info("Adding query of type: " + queryArgs[i]);	    
				if (queryArgs[i].equalsIgnoreCase("unique")) {
					generator.addUniqueQuery();
				} else {
			    	QueryType type = stringToType(queryArgs[i]);
			    	if (type == QueryType.UNKNOWN) {
			    		LOG.error("Unknown querytype: " + queryArgs[i]);
				    	return;
			    	}
			    	i++;
			    	int arity = 0;
			    	try {
			    		arity = Integer.parseInt(queryArgs[i]);
			    	} catch(NumberFormatException e) {
			    		i--;
			    		LOG.error("Invalid arity for query of type: " + queryArgs[i]);
			    		continue;
			    	}
			    	generator.addQuery(type, arity);
				}
			}
	    } catch(GFGeneratorException e) {
	    	e.printStackTrace();
	    	return;
	    }
	    
	    LOG.info("Finished parsing queries!");
	    LOG.info("Exporting Gumbo script...");

	    String name = cmd.hasOption("n") ? cmd.getOptionValue("n") : generateName();
	    String script = exporter.export(generator.generate(name));
	    
	    if (cmd.hasOption("o")) {
	    	String out = cmd.getOptionValue("o");
	    	LOG.info("Writing to output file: " + out);
			BufferedWriter writer;
			try {
				writer = new BufferedWriter(new FileWriter(out));
				writer.write(script);
			    writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		    LOG.info("Done!");
	    } else {
	    	System.out.println();
	    	System.out.println(script);
	    }
	}
	
	private String generateName() {
		Format formatter = new SimpleDateFormat("yyyyMMd_HHmmss");
		String name = "query_" + formatter.format(new Date());
		
		return name;
	}
	
	private QueryType stringToType(String string) {
		if (string.equalsIgnoreCase("and"))
			return QueryType.AND;
		else if (string.equalsIgnoreCase("or"))
			return QueryType.OR;
		else if (string.equalsIgnoreCase("xor"))
			return QueryType.XOR;
		else if (string.equalsIgnoreCase("negated_and"))
			return QueryType.NEGATED_AND;
		else if (string.equalsIgnoreCase("negated_or"))
			return QueryType.NEGATED_OR;
		else if (string.equalsIgnoreCase("negated_xor"))
			return QueryType.NEGATED_XOR;
		else 
			return QueryType.UNKNOWN;
	}
	
	private void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("gumbocli generate [options]", _options);
		System.exit(0);
	}

}
