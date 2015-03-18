/**
 * Created on: 17 Mar 2015
 */
package gumbo.gui;

import gumbo.compiler.GFCompiler;
import gumbo.compiler.GumboPlan;
import gumbo.compiler.filemapper.InputFormat;
import gumbo.compiler.filemapper.RelationFileMapping;
import gumbo.compiler.partitioner.CalculationPartitioner;
import gumbo.compiler.partitioner.DepthPartitioner;
import gumbo.compiler.partitioner.HeightPartitioner;
import gumbo.compiler.partitioner.OptimalPartitioner;
import gumbo.compiler.partitioner.UnitPartitioner;
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
import gumbo.gui.gumbogui.PlanViewer;
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
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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


	String[] partitioners = { "Unit", "Optimal", "Height", "Depth" };

	// GUI variables
	JComboBox<String> partitionerList;
	JComboBox<String> demoList;

	private JEditorPane inputEditor;

	private JEditorPane inputPathsText;
	private JEditorPane metricsText;

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

	JTabbedPane tabbedPane;
	private GumboPlan plan;

	private PlanViewer planView;



	public GumboGUI() {
	}


	private void createAndShowGUI() {

		// create demo list
		createDemoList();

		// query input
		inputEditor = new JEditorPane();
		inputEditor.setEditable(true);
		//		inputEditor.setFont(new Font("Courier New",0,14));
		inputEditor.setFont(new Font("monospaced", Font.PLAIN, 14));



		// input paths
		inputPathsText = new JEditorPane();
		inputPathsText.setEditable(true);
		//		inputPathsText.setFont(new Font("Courier New",0,14));	
		inputPathsText.setFont(new Font("monospaced", Font.PLAIN, 14));

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

		disableExecuteButtons();

		// compiler options
		partitionerList = new JComboBox(partitioners);
		partitionerList.setSelectedIndex(0);

		// TODO add plan details option

		// execution options
		// FUTURE add
		


		// add demo data
		loadData();

		// assemble
		PanelA panelA = new PanelA(inputEditor);
		PanelB panelB = new PanelB(inputPathsText,textConsole);
		PanelC panelC = new PanelC("Output directory: ", outPathText,defaultOutPathButton);
		PanelC panelCs = new PanelC("Scratch directory: ", scratchPathText,defaultScratchPathButton);
		PanelD panelD = new PanelD(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel, partitionerList, demoList);
		PanelBA panelBA = new PanelBA(panelB, panelA);
		PanelDC panelDC = new PanelDC(panelD, panelC,panelCs);
		PanelDCBA panelDCBA = new PanelDCBA(panelDC, panelBA);

		
		// plan
		planView = new PlanViewer();

		// metrics
		metricsText = new JEditorPane();
		metricsText.setEditable(false);
		//Put the editor pane in a scroll pane.
		JScrollPane editorScrollPane = new JScrollPane(metricsText);
		editorScrollPane.setVerticalScrollBarPolicy(
		                JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
//		editorScrollPane.setPreferredSize(new Dimension(250, 145));
		editorScrollPane.setMinimumSize(new Dimension(10, 10));
		
		resetMetrics();
		
		// tabs for different panels
		tabbedPane = new JTabbedPane();
		tabbedPane.addTab("Query", panelDCBA);
		tabbedPane.addTab("Plan", planView);
		tabbedPane.addTab("Metrics", editorScrollPane);

		GumboMainFrame mainwindow = new GumboMainFrame(tabbedPane);


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



	private void resetMetrics() {
		updateMetrics("Run a query to view the metrics...");
		
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

		/* set partitioner */

		partitionerList.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {

			}

		});

		/* load demo files */
		demoList.addActionListener(new ActionListener() {

			@Override
			public void actionPerformed(ActionEvent e) {
				String dir = (String) demoList.getSelectedItem();
				File demodir = new File("./queries/demo/"+dir);

				for (File fileEntry : demodir.listFiles()) {
					if (fileEntry.isFile()) {

						// load query
						if (fileEntry.getName().endsWith("-Gumbo.txt")) {
							inputEditor.setText(loadFile(fileEntry));
						}

						// load input
						if (fileEntry.getName().endsWith("-Dir.txt")) {
							inputPathsText.setText(loadFile(fileEntry));
						}
					}
				}




			}
		});

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

				SwingWorker<Integer, Void> worker = new SwingWorker<Integer,Void>() {

					@Override
					protected Integer doInBackground() throws Exception {


						HadoopEngine engine = new HadoopEngine();
						try {
							// TODO add defaults to config
							// TODO recompile plan?
							engine.executePlan(plan,getConf());
							final String stats = engine.getCounters();

							SwingUtilities.invokeAndWait(new Runnable() {

								@Override
								public void run() {
									updateMetrics(stats);
								}

							});

						} catch (ExecutionException e1) {
							updateMetrics("No metrics available; reason: last execution failed.");
							// TODO Auto-generated catch block
							e1.printStackTrace();
						} 
						return 0;
					}


					@Override
					protected void done() {
						super.done();

						// TODO place statistics in textfield
						enableCompilerButton();
					}

				};

				disableButtons();
				worker.execute();



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

				disableButtons();
				// TODO why not write to stdout?
				textConsole.setText("");   
				resetMetrics();

				SwingWorker worker = new SwingWorker<Integer,Void>() {

					@Override
					protected Integer doInBackground() throws Exception {

						try {
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

							gumboQuery = new GumboQuery("Gumbo_"+demoList.getSelectedItem(),inputQuery, inputs, output,scratch); // TODO add date to name

							// create plan
							GFCompiler compiler = new GFCompiler(getPartitioner());
							plan = compiler.createPlan(gumboQuery);
							//							System.out.println(plan);

							// visualize plan
							GraphVizPlanVisualizer visualizer = new GraphVizPlanVisualizer();
							visualizer.savePlan(plan, "output/query.png");



						} catch (Exception e1) {
							System.out.println(e1.getMessage());
							e1.printStackTrace();
						} finally {


						}

						//						Thread.sleep(10000);
						return 0;
					}

					/* (non-Javadoc)
					 * @see javax.swing.SwingWorker#done()
					 */
					@Override
					protected void done() {
						super.done();
						// update plan tab
						planView.reloadImage();
						tabbedPane.revalidate();
						tabbedPane.repaint();
						enableButtons();
					}

				};

				worker.execute();
			}
		});

	}

	public void createDemoList() {
		JComboBox<String> demolist = new JComboBox<>();
		File folder = new File("queries/demo");
		for (File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				demolist.addItem(fileEntry.getName());
			}
		}

		this.demoList = demolist;
	}

	public String loadFile(File f) {

		List<String> lines;
		String output = "";
		try {
			lines = Files.readAllLines(f.toPath(),StandardCharsets.UTF_8);

			for (String line : lines) {
				output += line + System.lineSeparator();
			}
		} catch (IOException e) {
			System.out.println("Could not read demo file!");
			e.printStackTrace();
		}
		return output;
	}

	public void enableButtons() {
		buttonQC.setEnabled(true);
		buttonFH.setEnabled(true);
		buttonFS.setEnabled(true);
	}

	public void enableCompilerButton() {
		buttonQC.setEnabled(true);
	}

	public void disableButtons() {
		buttonQC.setEnabled(false);
		buttonFH.setEnabled(false);
		buttonFS.setEnabled(false);
	}

	public void disableExecuteButtons() {
		buttonFH.setEnabled(false);
		buttonFS.setEnabled(false);
	}


	public static void main(String[] args) throws Exception {

		// Let ToolRunner handle generic command-line options 
		int res = ToolRunner.run(new Configuration(), new GumboGUI(), args);

		//		System.exit(res);

	}

	public CalculationPartitioner getPartitioner() {
		String item = (String) partitionerList.getSelectedItem();

		// CLEAN this tis hardcoded stuff :)
		switch(item) {
		case "Unit":
			return new UnitPartitioner();
		case "Optimal":
			return new OptimalPartitioner();
		case "Height":
			return new HeightPartitioner();
		case "Depth":
			return new DepthPartitioner();
		default:
			return new UnitPartitioner();
		}
	}

	public void updateMetrics(String metrics) {
		metricsText.setText(metrics);
	}


	/* (non-Javadoc)
	 * @see org.apache.hadoop.util.Tool#run(java.lang.String[])
	 */
	@Override
	public int run(String[] args) throws Exception {
		try {
			UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
		} catch (ClassNotFoundException classNotFoundException) {
		} catch (InstantiationException instantiationException) {
		} catch (IllegalAccessException illegalAccessException) {
		} catch (UnsupportedLookAndFeelException unsupportedLookAndFeelException) {
		}

		SwingUtilities.invokeLater(new Runnable() {
			public void run() {
				createAndShowGUI();
			}
		});
		return 0;
	}

}
