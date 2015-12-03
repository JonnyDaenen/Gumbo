package gumbo.cli;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.convertors.GFConversionException;
import gumbo.convertors.GFConverter;
import gumbo.convertors.hive.GFHiveConverterLong;
import gumbo.convertors.hive.GFHiveConverterWide;
import gumbo.convertors.pig.GFPigConverterLong;
import gumbo.convertors.pig.GFPigConverterWide;
import gumbo.input.GumboFileParser;
import gumbo.input.GumboFileParser.MissingQueryComponentException;
import gumbo.input.GumboQuery;
import gumbo.input.parser.GumboScriptFileParser;
import gumbo.input.parser.GumboScriptFileParser.GumboParseException;

public class GumboConverterTool implements GumboCommandLineTool {

	private static final Log LOG = LogFactory.getLog(GumboConverterTool.class);
	
	private Options _options;

	public GumboConverterTool() {
		_options = new Options();
		_options.addOption("h", "help", false, "Show usage information");
		_options.addOption("l", "lang", true, "Specify either 'pig' or 'hive' as the output language. Default: pig.");
		_options.addOption("m", "method", true, "Specify either 'wide' or 'long' as the conversion method. Default: wide.");
		_options.addOption("i", "input", true, "Specify the input gumboscript.");
		_options.addOption("o", "output", true, "Specify the output filename.");
		_options.addOption("p", "prefix", false, "Optional flag to undicate the file is in prefix format");
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
	    
	    if (cmd.hasOption("h") || cmd.getOptions().length == 0) {
	    	help();
	    }
	    
	    GFConverter converter = null;
	    String lang = cmd.getOptionValue("l", "pig");
	    String method = cmd.getOptionValue("m", "wide");
	    
	    
	    if (lang.equals("pig")) {
	    	if (method.equals("wide"))
	    		converter = new GFPigConverterWide(); 
	    	else if (method.equals("long"))
	    		converter = new GFPigConverterLong(); 
	    	else {
	    		LOG.error("Invalid conversion method specified: " + method);
		    	return;
	    	}
	    } else if (lang.equals("hive")) {
	    	if (method.equals("wide"))
	    		converter = new GFHiveConverterWide(); 
	    	else if (method.equals("long"))
	    		converter = new GFHiveConverterLong(); 
	    	else {
	    		LOG.error("Invalid conversion method specified: " + method);
		    	return;
	    	}
	    } else {
	    	LOG.error("Invalid output language specified: " + lang);
	    	return;
	    }
	    
	    if (!cmd.hasOption("i") || !cmd.hasOption("o")) {
	    	LOG.error("No input and/or output file specified!");
	    	return;
	    }
	    
	    String in = cmd.getOptionValue("i");
	    String out = cmd.getOptionValue("o");
	    
	    try {
			GumboQuery query = null;
			if (!cmd.hasOption("p")) {
			    GumboScriptFileParser gumboparser = new GumboScriptFileParser();
				query = gumboparser.parse(in);
			} else {
				GumboFileParser gumboparser = new GumboFileParser();
				query = gumboparser.parse(in,in);
			}
			
			LOG.info("Lang: " + lang);
			LOG.info("Method: " + method);
			LOG.info("Converting script...");
			String script = converter.convert(query);
			
			LOG.info("Finished converting script!");
			LOG.info("Writing to output file: " + out);
			BufferedWriter writer = new BufferedWriter(new FileWriter(out));
		    writer.write(script);
		    writer.close();
		    LOG.info("Done!");
		} catch (IOException | MissingQueryComponentException | GumboParseException | GFConversionException e) {
		    e.printStackTrace();
		} 
	    	
	}
	
	private void help() {
		HelpFormatter formatter = new HelpFormatter();
		formatter.printHelp("gumbocli convert", _options);
		System.exit(0);
	}

}
