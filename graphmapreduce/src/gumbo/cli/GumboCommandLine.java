package gumbo.cli;


public class GumboCommandLine {

	private static void usage() {
		System.err.println("Usage: gumbocli COMMAND");
		System.err.println("\t where COMMAND is one of:");
		System.err.println(String.format("  %-20s%-100s", "convert", "Convert a Gumbo script to pig or hive"));
		System.err.println(String.format("  %-20s%-100s", "generate", "Generate a Gumbo script"));
	}
	
	public static void main(String[] args) {
	    GumboCommandLineTool cli = null;
	    
	    if (args.length < 1) {
	    	usage();
	    	return;
	    }
	    
	    if (args[0].equals("convert")) 
	    	cli = new GumboConverterTool();
	    else if (args[0].equals("generate"))
	    	cli = new GumboGeneratorTool();
	    else {
	    	usage();
	    	return;
	    }
	    
	    cli.run(args);
	}
	
}
