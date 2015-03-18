package gumbo.gui;

import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.filemapper.RelationFileMappingException;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.gui.gumbogui.*;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.RelationSchemaException;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.GFInfixSerializer;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class GumboMain extends Configured implements Tool {
	
	/* Eclipse tells me that this program does not have serialVersionUID
	 * So I ask Eclipse to generate one.
	 * I don't know what it is for though*/
	private static final long serialVersionUID = 5033027026087017263L;
	
	// GUI variables
	private static JEditorPane editorIQ;
	//private static JEditorPane editorIO;
	
	private static JEditorPane editorIn;
	
	private static TextField outPathText;
	private static JButton browseOutPathButton;
	
	private static TextField scratchPathText;
	private static JButton browseScratchPathButton;
	
	private static JFileChooser outPathChooser;
	
	private static JTextArea textConsole;
	
	private static JTextAreaOutputStream outPipe;
	
	private static JButton buttonQC;
	private static JButton buttonSche;
	private static JButton buttonFH;
	private static JButton buttonFS;
	
	private static JCheckBox cbLevel;
	
	// Gumbo's variable
	
	private static GumboQuery gumboQuery;
	
	

	HadoopEngine hadoopEngine;
	
	public static void main(String[] args) throws Exception {

		mainGumbo();
		
	}


	public static void mainGumbo() throws Exception {
		
		editorIQ = new JEditorPane();
		editorIQ.setEditable(true);
		editorIQ.setFont(new Font("Courier New",0,19));
		
		editorIQ.setText("Out0(x) : G(x,z); \n"
				+ "Out1(x) : E(x,y) & (!F(y) & Out0(x)); \n"
				+ "Out2(x) : E(x,y) & !Out1(y); \n"
				+ "Out3(x) : E(x,y) & (Out1(y) & !Out2(x)); \n"
				+ "Out4(x,y) : E(x,y) & (!Out1(x));");
		
		PanelA panelA = new PanelA(editorIQ);
		
		editorIn = new JEditorPane();
		editorIn.setEditable(true);
		editorIn.setFont(new Font("Courier New",0,19));	
		
		textConsole = new JTextArea();
		textConsole.setEditable(false);
		textConsole.setFont(new Font("Courier New",1,12));
		
		PanelB panelB = new PanelB(editorIn,textConsole);
		
		outPipe = new JTextAreaOutputStream(textConsole);
					
		outPathChooser = new JFileChooser();
		outPathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		outPathChooser.setDialogTitle("Select target directory");
		
		outPathText = new TextField(97);
		outPathText.setEditable(true);
		browseOutPathButton = new JButton("Browse");
		
		scratchPathText = new TextField(97);
		scratchPathText.setEditable(true);
		browseScratchPathButton = new JButton("Browse");
		
		PanelC panelC = new PanelC("Output directory: ", outPathText,browseOutPathButton);
			
		PanelC panelCs = new PanelC("Scratch directory: ", scratchPathText,browseScratchPathButton);
		
		
		buttonQC = new JButton("Query Compiler");		
		buttonSche = new JButton("Jobs constructor");
		buttonFH = new JButton("GUMBO-Hadoop");
		buttonFS = new JButton("GUMBO-Spark");
		cbLevel = new JCheckBox("with schedule");
		
		PanelD panelD = new PanelD(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel,null);
		
		PanelBA panelBA = new PanelBA(panelB, panelA);
		PanelDC panelDC = new PanelDC(panelD, panelC,panelCs);
		PanelDCBA panelDCBA = new PanelDCBA(panelDC, panelBA);
		
		GumboMainFrame mainwindow = new GumboMainFrame(panelDCBA);
		
			
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

        int height = screenSize.height;
        int width = screenSize.width;
        screenSize.setSize(width*(0.99), height*(0.99));
        int newheight = screenSize.height;
        int newwidth = screenSize.width;
		
		mainwindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainwindow.setSize(newwidth, newheight);
		mainwindow.setVisible(true);
		
		System.setOut (new PrintStream (outPipe));
		
		browseOutPathButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				File s;
				int returnVal = outPathChooser.showOpenDialog(null);
				if(returnVal == JFileChooser.APPROVE_OPTION) {
					s = outPathChooser.getSelectedFile();
					outPathText.setText(s.toString());
				}
				
			}
		});
		
		buttonFH.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				textConsole.setText("");
				textConsole.append("Evaluating the input query with Hadoop....\n");
				
				Configuration conf = null; //getConf(); 
				
				HadoopExecutorSettings settings = new HadoopExecutorSettings(conf);
				settings.loadDefaults();
				settings.turnOffOptimizations();
				
			}
		});

		
		buttonFS.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				textConsole.setText("");
				textConsole.append("Evaluating the input query with Spark....\n");
				
			}
		});

		
		
		browseScratchPathButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				
				File s;
				int returnVal = outPathChooser.showOpenDialog(null);
				if(returnVal == JFileChooser.APPROVE_OPTION) {
					s = outPathChooser.getSelectedFile();
					scratchPathText.setText(s.toString());
				}
				
			}
		});
		
	    buttonQC.addActionListener(new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	
	        	textConsole.setText("");      	
	        	
	        	Set<GFExpression> inputQuery = new HashSet();
	        	Path output;
	        	Path scratch;
	        	RelationFileMapping inputs = new RelationFileMapping();
	        	
	        	String sout = outPathText.getText();
	        	if (sout.length() == 0) {
	        		textConsole.setText("The output directory is still empty\n");
	        		return;
	        	} else {
	        		textConsole.append("The output directory is:" + sout);
	        		textConsole.append("\n");
	        		output = new Path(sout);
	        	}
	        	
	        	String sscratch = scratchPathText.getText();
	        	if (sscratch.length() == 0) {
	        		textConsole.setText("The scratch directory is still empty\n");
	        		return;
	        	} else {
	        		textConsole.append("The scratch directory is:" + sscratch);
	        		textConsole.append("\n");
	        		scratch = new Path(sscratch);
	        	}
	        	
	        	
	        	textConsole.append("Compiling the input queries...\n");
	        	
	        	//System.out.println("Testing, testing !!!!");
	        	
	        	GFInfixSerializer parser = new GFInfixSerializer();
	        	
	        	try {
	        		inputQuery = parser.GetGFExpression(editorIQ.getText().trim());
	        	} catch (Exception exc) {
	        		textConsole.append(exc.toString());
	        		//exc.printStackTrace();
	    			return;
	    		} 

	        	//textConsole.setText("");
	        	textConsole.append("The following queries compiled:\n");
	        	
	        	for (GFExpression gf : inputQuery) {
	        		textConsole.append(gf.toString()+"\n");
	        		
	        	}
	        	
	        	
	        	
	        	textConsole.append("Parsing the input directories...\n");
	        	
	        	try {
					String s = editorIn.getText().trim();
	        		String [] sin = s.split(";");
	        		
	        		RelationSchema r;
	        			        		
	        		String [] dummy;
	        		for(int i =0;i< sin.length;i++){
	        			
	        			dummy = sin[i].split("-");
	        			if (dummy.length == 2) {
	        				        					        				
	        				inputs.addPath(new RelationSchema(dummy[0].trim()), new Path(dummy[1].trim()));
	        			} else {
	        				textConsole.append("Error in line"+i+"in INPUT DIRECTORIES");
	        				textConsole.append("Expecting <Relation> - <Path> ");
	        				return;
	        			}       			   			
	        		}
	        		     			        		
				} catch (Exception e1) {
					
					textConsole.append("Get exception in reading the INPUT DIRECTORIES.\n");
					textConsole.append(e1.toString());
	        		return;
				}
				
	        	
	        	gumboQuery = new GumboQuery("Gumbo query",inputQuery, inputs, output,scratch);
	        	

	       	}
	    });
		
	}


	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}


}

