/**
 * Created on: 17 Mar 2015
 */
package gumbo.gui;

import gumbo.Gumbo;
import gumbo.compiler.GFCompiler;
import gumbo.compiler.GFCompilerException;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.plan.GraphVizPlanVisualizer;
import gumbo.engine.ExecutionException;
import gumbo.engine.hadoop.HadoopEngine;
import gumbo.gui.gumbogui.GumboMainFrame;
import gumbo.gui.gumbogui.PanelA;
import gumbo.gui.gumbogui.PanelB;
import gumbo.gui.gumbogui.PanelBA;
import gumbo.gui.gumbogui.PanelC;
import gumbo.gui.gumbogui.PanelD;
import gumbo.gui.gumbogui.PanelDC;
import gumbo.gui.gumbogui.PanelDCBA;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
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
import java.util.HashSet;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import org.apache.commons.configuration.DefaultConfigurationBuilder.ConfigurationDeclaration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import tests.GraphViz;

/**
 * 
 * @author Tony Tan
 * @author Jonny Daenen
 *
 */
public class GumboGUI extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(GumboGUI.class);

	// defaults
	private String defaultOutPath = "/users/jonny/output";
	private String defaultScratchPath = "/users/jonny/scratch";

	// GUI variables
	private JEditorPane inputEditor;

	private JEditorPane inputPathsText;

	private TextField outPathText;
	private JButton defaultOutPathButton;

	private TextField scratchPathText;
	private JButton defaultScratchPathButton;

	private JFileChooser outPathChooser;

	private JTextArea textConsole;

	private JTextAreaOutputStream outPipe;

	private JButton buttonQC;
	private JButton buttonSche;
	private JButton buttonFH;
	private JButton buttonFS;

	private static JCheckBox cbLevel;


	private GumboPlan plan;



	public GumboGUI() {
	}


	private void setup() {

		// query input
		inputEditor = new JEditorPane();
		inputEditor.setEditable(true);
		inputEditor.setFont(new Font("Courier New",0,14));


		// input paths
		inputPathsText = new JEditorPane();
		inputPathsText.setEditable(true);
		inputPathsText.setFont(new Font("Courier New",0,14));	

		// output path
		//		outPathChooser = new JFileChooser();
		//		outPathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		//		outPathChooser.setDialogTitle("Select target directory");

		outPathText = new TextField(97);
		outPathText.setEditable(true);
		defaultOutPathButton = new JButton("Load default");

		// scratch path
		scratchPathText = new TextField(97);
		scratchPathText.setEditable(true);
		defaultScratchPathButton = new JButton("Load default");

		// console
		textConsole = new JTextArea();
		textConsole.setEditable(false);
		textConsole.setFont(new Font("Courier New",1,12));

		// TODO check output redirection?
		outPipe = new JTextAreaOutputStream(textConsole);
		System.setOut (new PrintStream (outPipe));

		// buttons
		buttonQC = new JButton("Query Compiler");		
		buttonSche = new JButton("Job constructor");
		buttonFH = new JButton("GUMBO-Hadoop");
		buttonFS = new JButton("GUMBO-Spark");
		cbLevel = new JCheckBox("with schedule");


		// add demo data
		loadData();

		// assemble
		PanelA panelA = new PanelA(inputEditor);
		PanelB panelB = new PanelB(inputPathsText,textConsole);
		PanelC panelC = new PanelC("Output directory: ", outPathText,defaultOutPathButton);
		PanelC panelCs = new PanelC("Scratch directory: ", scratchPathText,defaultScratchPathButton);
		PanelD panelD = new PanelD(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		PanelBA panelBA = new PanelBA(panelB, panelA);
		PanelDC panelDC = new PanelDC(panelD, panelC,panelCs);
		PanelDCBA panelDCBA = new PanelDCBA(panelDC, panelBA);

		GumboMainFrame mainwindow = new GumboMainFrame(panelDCBA);


		// add actions
		addActions();

		// scale & show
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

		int height = screenSize.height;
		int width = screenSize.width;
		// TODO ??
		screenSize.setSize(width*(0.99), height*(0.99));
		int newheight = screenSize.height;
		int newwidth = screenSize.width;

		mainwindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainwindow.setSize(newwidth, newheight);
		mainwindow.setVisible(true);

		System.out.println("Ready for action!");
		LOG.info("ready for action");

	}


	/**
	 * 
	 */
	private void loadData() {
		inputEditor.setText("Out1(x) : E(x,y) & (!F(y) & G(x,z)); \n"
				+ "Out2(x) : E(x,y) & !Out1(y); \n"
				+ "Out3(x) : E(x,y) & (Out1(y) & !Out2(x)); \n"
				+ "Out4(x,y) : E(x,y) & (!Out1(x));");

		inputEditor.setText("Out(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) : "
				+ "R(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) & "
				+ "(!S(x1) & !S(x2) & !S(x3) & !S(x4) & !S(x5) & !S(x6) & !S(x7) & !S(x8) & !S(x9) & !S(x10))");
		inputPathsText.setText("R,10 - input/experiments/EXP_008/R, CSV;\n"
				+ "S,1 - input/experiments/EXP_008/S, CSV;");
		outPathText.setText(defaultOutPath);
		scratchPathText.setText(defaultScratchPath);


	}


	private void addActions() {

		/* output default */
		defaultOutPathButton.addActionListener(new ActionListener() {


			public void actionPerformed(ActionEvent e) {
				outPathText.setText(defaultOutPath);

				//				File s;
				//				int returnVal = outPathChooser.showOpenDialog(null);
				//				if(returnVal == JFileChooser.APPROVE_OPTION) {
				//					s = outPathChooser.getSelectedFile();
				//					outPathText.setText(s.toString());
				//				}

			}
		});


		/* scratch filechooser */
		defaultScratchPathButton.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				scratchPathText.setText(defaultScratchPath);
				//				File s;
				//				int returnVal = outPathChooser.showOpenDialog(null);
				//				if(returnVal == JFileChooser.APPROVE_OPTION) {
				//					s = outPathChooser.getSelectedFile();
				//					scratchPathText.setText(s.toString());
				//				}

			}
		});


		/* hadoop run */
		buttonFH.addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				textConsole.setText("");
				textConsole.append("Evaluating the input query with Hadoop....\n");
				// TODO run hadoop

				HadoopEngine engine = new HadoopEngine();
				try {
					// TODO add defaults to config
					// TODO recompile plan
					engine.executePlan(plan,getConf());
				} catch (ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} 

			}
		});


		/* spark run */
		buttonFS.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				textConsole.setText("");
				textConsole.append("Evaluating the input query with Spark....\n");
				// TODO run spark
			}
		});




		/* compiler button */
		buttonQC.addActionListener(new ActionListener() {
			private GumboQuery gumboQuery;

			public void actionPerformed(ActionEvent e) {

				try {
					// TODO why not write to stdout?
					textConsole.setText("");      	

					Set<GFExpression> inputQuery = new HashSet<>();
					Path output;
					Path scratch;
					RelationFileMapping inputs = new RelationFileMapping();

					String sout = outPathText.getText();
					if (sout.length() == 0) {
						throw new Exception("The output directory is empty");
					} else {
						textConsole.append("The output directory is:" + sout);
						textConsole.append("\n");
						output = new Path(sout);
					}

					String sscratch = scratchPathText.getText();
					if (sscratch.length() == 0) {
						throw new Exception("The scratch directory is empty");
					} else {
						textConsole.append("The scratch directory is:" + sscratch);
						textConsole.append("\n");
						scratch = new Path(sscratch);
					}


					textConsole.append("Compiling the input queries...\n");

					//System.out.println("Testing, testing !!!!");

					GFInfixSerializer parser = new GFInfixSerializer();


					inputQuery = parser.GetGFExpression(inputEditor.getText().trim());


					//textConsole.setText("");
					textConsole.append("The following queries compiled:\n");

					for (GFExpression gf : inputQuery) {
						textConsole.append(gf.toString()+"\n");

					}



					textConsole.append("Parsing the input directories...\n");


					String s = inputPathsText.getText().trim();
					String [] sin = s.split(";");

					String [] dummy;
					for(int i = 0; i< sin.length; i++){
						System.out.println("part:" + sin[i]);

						dummy = sin[i].split("-");
						if (dummy.length != 2)
							throw new Exception("Error in line "+i+" in INPUT DIRECTORIES\nExpecting <Relation>,<Arity> - <Path>,<Type>" );

						// schema 
						String[] rsParts = dummy[0].split(",");
						if (rsParts.length != 2)
							throw new Exception("Error in line "+i+" in INPUT DIRECTORIES\nExpecting <Relation>,<Arity> - <Path>,<Type>" );

						// path
						String[] paths = dummy[1].split(",");
						if (paths.length != 2)
							throw new Exception("Error in line "+i+" in INPUT DIRECTORIES\nExpecting <Relation>,<Arity> - <Path>,<Type>" );

						// type
						InputFormat type = InputFormat.REL;
						if (paths[1].trim().toLowerCase().equals("csv")) {
							type = InputFormat.CSV;
						}

						inputs.addPath(new RelationSchema(rsParts[0].trim(),Integer.parseInt(rsParts[1].trim())), new Path(paths[0].trim()), type);

					}




					gumboQuery = new GumboQuery("Gumbo query",inputQuery, inputs, output,scratch); // TODO add date to name




					// create plan
					GFCompiler compiler = new GFCompiler();
					plan = compiler.createPlan(gumboQuery);
					System.out.println(plan);

					// visualize plan
					GraphVizPlanVisualizer visualizer = new GraphVizPlanVisualizer();
					visualizer.savePlan(plan, "output/query.png");

					// update plan tab


				} catch (Exception e1) {
					System.out.println(e1.getMessage());
					e1.printStackTrace();
				}





			}
		});

	}


	public static void main(String[] args) throws Exception {

		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new GumboGUI(), args);

		//		System.exit(res);

	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		setup();
		return 0;
	}

}
