package mapreduce.guardedfragment.gumbogui;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.PrintStream;
import java.security.Timestamp;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import org.apache.hadoop.fs.Path;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.executor.spark.SparkExecutor;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.GumboMainWindowMixIO;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.GumboMainWindowMixIOwithScroll;
import mapreduce.guardedfragment.planner.GFMRPlanner;
import mapreduce.guardedfragment.planner.GFMRPlannerException;
import mapreduce.guardedfragment.planner.partitioner.HeightPartitioner;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.RelationFileMappingException;
import mapreduce.guardedfragment.planner.structures.data.RelationSchemaException;
import mapreduce.guardedfragment.structure.gfexpressions.GFExistentialExpression;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFInfixSerializer;

public class GumboMixIO {
	
	
	private static JEditorPane editorIQ;
	private static JTextArea textConsole;
	
	private static TextField inPathText;
	private static TextField outPathText;
	
	private static JButton browseInPathButton;
	private static JButton browseOutPathButton;
	
	private static JButton buttonQC;
	private static JButton buttonSche;
	private static JButton buttonFH;
	private static JButton buttonFS;
	
	private static JCheckBox cbLevel;
	
	private static JTextAreaOutputStream outPipe;
		
	// Gumbo's variable
	
	private static Set<GFExistentialExpression> inputQuery;
	private static MRPlan plan;
	private static HadoopExecutor hadoopExec;
	private static SparkExecutor SparkExec;
	


	public static void main(String[] args) {
		
		editorIQ = new JEditorPane();
		editorIQ.setEditable(true);
		editorIQ.setFont(new Font("Courier New",1,13));
		
		textConsole = new JTextArea();
		textConsole.setEditable(false);
		textConsole.setFont(new Font("Courier New",1,13));
		
		

		JFileChooser outPathChooser = new JFileChooser();
		outPathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		
		inPathText = new TextField(87);
		outPathText = new TextField(87);
		
		inPathText.setEditable(false);
		outPathText.setEditable(false);

		browseInPathButton = new JButton("Browse");
		browseOutPathButton = new JButton("Browse");
		
		buttonQC = new JButton("Query Compiler");
		buttonSche = new JButton("Jobs constructor");
		buttonFH = new JButton("GUMBO-Hadoop");
		buttonFS = new JButton("GUMBO-Spark");
		cbLevel = new JCheckBox("with schedule");
		
		
		GumboMainWindowMixIOwithScroll mainwindow = new GumboMainWindowMixIOwithScroll(
				editorIQ, textConsole,buttonQC,
				buttonSche,buttonFH,buttonFS,cbLevel, inPathText, outPathText,
				browseInPathButton, browseOutPathButton);
			
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

        int height = screenSize.height;
        int width = screenSize.width;
        screenSize.setSize(width*(0.96), height*(0.94));
        int newheight = screenSize.height;
        int newwidth = screenSize.width;
		
		mainwindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainwindow.setSize(newwidth, newheight);
		mainwindow.setLocation((height-newheight)/2, (width-newwidth)/2);
		mainwindow.setVisible(true);
		
		outPipe = new JTextAreaOutputStream(textConsole);
		System.setOut (new PrintStream (outPipe));
			
	    buttonQC.addActionListener(new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	textConsole.setText("");
	        	textConsole.append("Compiling the input queries...\n");
	        	
	        	GFInfixSerializer parser = new GFInfixSerializer();
	        	
	        	try {
	        		inputQuery = parser.GetGFExpression(editorIQ.getText().trim());
	        	} catch (Exception exc) {
	        		
	        		/*StringWriter errors = new StringWriter();
	        		exc.printStackTrace(new PrintWriter(errors));
	        		textConsole.append(errors.toString());
	        		*/
	        		System.out.println(exc);
	    			exc.printStackTrace();
	    		} 

	        	textConsole.append("The following queries compiled:\n");
	        	
	        	for (GFExpression gf : inputQuery) {
	        		textConsole.append(gf.toString()+"\n");
	        		
	        	}
	        	


	        	
	       	}
	    });
	    
	    browseInPathButton.addActionListener(new ActionListener() {
	    	public void actionPerformed(ActionEvent e) {
	    		
		    	JFileChooser pathChooser = new JFileChooser();
				pathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				
				if (pathChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {
					inPathText.setText(pathChooser.getSelectedFile().getAbsolutePath());
		        }			
	    	}
	    });
		
	    browseOutPathButton.addActionListener(new ActionListener() {
	    	public void actionPerformed(ActionEvent e) {
	    		
		    	JFileChooser pathChooser = new JFileChooser();
				pathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
				
				if (pathChooser.showOpenDialog(null) == JFileChooser.APPROVE_OPTION) {					
					outPathText.setText(pathChooser.getSelectedFile().getAbsolutePath());
		        }    		
	    	}
	    });
	
		
	    
	    buttonFH.addActionListener(new ActionListener() {
	    	public void actionPerformed(ActionEvent e) {
	    		
	    		textConsole.setText("");
	        	textConsole.append("Running Gumbo-Hadoop...\n");
	    		
	        	GFMRPlanner planner = new GFMRPlanner(new HeightPartitioner());
	        	
	        	textConsole.append("Parsing the input and output files...\n");
	        	
	        	
	        	RelationFileMapping rfm = new RelationFileMapping();
				rfm.setDefaultPath(new Path(inPathText.getText()));
				MRPlan plan;
				String xtime = "" + System.currentTimeMillis() / 1000;
				try {
					plan = planner.createPlan(
							(Set<GFExistentialExpression>)inputQuery, rfm, 
							new Path(outPathText.getText()), 
							new Path("/Users/ntynvt/tempGumbo/"+xtime));
					
					HadoopExecutor executor = new HadoopExecutor();
					executor.execute(plan);
				
				} catch (GFMRPlannerException e1) {
					// TODO Auto-generated catch block
					System.out.println(e1);
					e1.printStackTrace();
				} catch (IllegalArgumentException e1) {
					// TODO Auto-generated catch block
					System.out.println(e1);
					e1.printStackTrace();
				}
				//plan.execute();
				
				
				
//				SparkExecutor executor = new SparkExecutor();
//				executor.execute(plan);
	        	
	        	// In the future should be:
	        	// plan = new MRPlan(inputQuery,rfmInPath, rfmOutPath);   		
	    	}
	    });
	    
	    
		
	}

}


