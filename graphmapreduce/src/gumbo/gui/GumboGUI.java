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
import gumbo.engine.spark.SparkEngine;
import gumbo.gui.gumbogui.GumboMainFrame;
import gumbo.gui.gumbogui.PlanViewer;
import gumbo.gui.panels.ConsolePanel;
import gumbo.gui.panels.QueryInputDetails;
import gumbo.gui.panels.QueryInputField;
import gumbo.gui.panels.SettingsPanel;
import gumbo.input.GumboQuery;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.gfexpressions.GFExpression;
import gumbo.structures.gfexpressions.io.GFInfixSerializer;

import java.awt.BorderLayout;
import java.awt.Dimension;
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

import javax.swing.JComboBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.SwingUtilities;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author Tony Tan
 * @author Jonny Daenen
 *
 */
public class GumboGUI extends Configured implements Tool {

	private static final Log LOG = LogFactory.getLog(GumboGUI.class);

	// defaults
	//	private String defaultOutPath = "/users/jonny/output";
	//	private String defaultScratchPath = "/users/jonny/scratch";



	// GUI variables

	private QueryInputField inputQuery;
	private QueryInputDetails inputIO;
	private SettingsPanel settings;
	private ConsolePanel console;



	private GumboPlan plan;

	private PlanViewer planView;

	private JEditorPane metricsText;

	private JTabbedPane tabbedPane;

	private JComboBox<String> demoList;



	public GumboGUI() {
	}


	private void createAndShowGUI() {

	


		

		// create 4 panels
		inputQuery = new QueryInputField();
		inputIO = new QueryInputDetails();
		settings = new SettingsPanel();
		console = new ConsolePanel();

		// TODO check output redirection?
		JTextAreaOutputStream outPipe = new JTextAreaOutputStream(console.getConsoleField());
		System.setOut (new PrintStream (outPipe));

		// create splits
		JSplitPane topsplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,inputQuery,inputIO);
		JSplitPane bottomsplit = new JSplitPane(JSplitPane.HORIZONTAL_SPLIT,settings, console);
		JSplitPane split = new JSplitPane(JSplitPane.VERTICAL_SPLIT, topsplit, bottomsplit);
		// no borders
		topsplit.setBorder(null);
		bottomsplit.setBorder(null);
		split.setBorder(null);


		// create demo list
		createDemoList();
		// add demo button
		JPanel queryOverall = new JPanel(new BorderLayout());
		queryOverall.add(split,BorderLayout.CENTER);
		queryOverall.add(demoList,BorderLayout.NORTH);


		// plan tab
		planView = new PlanViewer();

		// metrics tab
		metricsText = new JEditorPane();
		metricsText.setEditable(false);
		metricsText.setBorder(null);
		//Put the editor pane in a scroll pane.
		JScrollPane editorScrollPane = new JScrollPane(metricsText);
		editorScrollPane.setVerticalScrollBarPolicy(
				JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		//		editorScrollPane.setPreferredSize(new Dimension(250, 145));
		editorScrollPane.setMinimumSize(new Dimension(10, 10));

		resetMetrics();

		// tabs for different panels
		tabbedPane = new JTabbedPane();
		tabbedPane.addTab("Query", queryOverall);
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

		// set splitters AFTER they are rendered on screen!
		topsplit.setDividerLocation(0.7);
		bottomsplit.setDividerLocation(0.3);
		split.setDividerLocation(0.5);
		

		// buttons
		disableExecuteButtons();
		// add demo data
		loadData();		


		LOG.info("Ready for action!");


	}



	private void resetMetrics() {
		updateMetrics("Run a query to view the metrics...");

	}


	/**
	 * 
	 */
	private void loadData() {

		inputQuery.getQueryField().setText("Out(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) : "
				+ "R(x1,x2,x3,x4,x5,x6,x7,x8,x9,x10) & "
				+ "(!S(x1) & !S(x2) & !S(x3) & !S(x4) & !S(x5) & !S(x6) & !S(x7) & !S(x8) & !S(x9) & !S(x10))");
		inputIO.getInputField().setText("R,10 - input/experiments/EXP_008/R, CSV;\n"
				+ "S,1 - input/experiments/EXP_008/S, CSV;");


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
		settings.getCompileButton().setEnabled(true);
		settings.getHadoopButton().setEnabled(true);
		settings.getSparkButton().setEnabled(true);
	}

	public void enableCompilerButton() {
		settings.getCompileButton().setEnabled(true);
	}

	public void disableButtons() {

		settings.getCompileButton().setEnabled(false);
		settings.getHadoopButton().setEnabled(false);
		settings.getSparkButton().setEnabled(false);
	}

	public void disableExecuteButtons() {
		settings.getHadoopButton().setEnabled(false);
		settings.getSparkButton().setEnabled(false);
	}


	public static void main(String[] args) throws Exception {

		// Let ToolRunner handle generic command-line options 
		ToolRunner.run(new Configuration(), new GumboGUI(), args);

		//		System.exit(res);

	}

	public CalculationPartitioner getPartitioner() {
		String item = (String) settings.getPartitionerList().getSelectedItem();

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


	protected class EngineWorker extends SwingWorker<Integer,Void> {



		private boolean hadoop;
		private JavaSparkContext sparkContext;


		public EngineWorker(boolean hadoop) {
			this.hadoop = hadoop;
		}

		@Override
		protected Integer doInBackground() throws Exception {






			try {
				if (hadoop) {
					HadoopEngine engine = new HadoopEngine();
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

				} else {
					if (sparkContext == null) {
						SparkConf sparkConf = new SparkConf().setAppName("Gumbo_Spark");
						sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
						//					sparkConf.set("spark.kryo.registrator", "org.kitesdk.examples.spark.AvroKyroRegistrator");
						//					sparkConf.registerKryoClasses(
						//							gumbo.structures.gfexpressions.operations.ExpressionSetOperations.class
						//							);
						// SparkConf sparkConf = new SparkConf().setAppName("Fronjo");
						sparkContext = new JavaSparkContext(sparkConf);
					}

					SparkEngine engine = new SparkEngine(sparkContext);
					engine.executePlan(plan, getConf());
				}

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

	
	protected void removeDir(String dir){
		FileSystem fs;
		try {
			LOG.info("Removing output dir: "+dir);
			fs = FileSystem.get(getConf());
			fs.delete(new Path(dir), true); // delete folder recursively
			LOG.info("Output dir removed.");
		} catch (IOException e) {
			LOG.error("Failed to delete output dir");
			e.printStackTrace();
		}
	}

	protected void removeOutputDir() {
		removeDir(this.inputIO.getOutputField().getText());
	}

	protected void removeScratchDir() {
		removeDir(this.inputIO.getScratchField().getText());
	}
	
	



	private void addActions() {


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
							inputQuery.getQueryField().setText(loadFile(fileEntry));
						}

						// load input
						if (fileEntry.getName().endsWith("-Dir.txt")) {
							inputIO.getInputField().setText(loadFile(fileEntry));
						}
					}
				}




			}
		});




		/* delete scratch */
		inputIO.getScratchDeleteButton().addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				console.getConsoleField().setText("");

				SwingWorker<Object, Object> worker = new SwingWorker<Object, Object>(){

					@Override
					protected Object doInBackground() throws Exception {
						disableButtons();
						removeScratchDir();
						return null;
					}
					@Override
					protected void done() {
						super.done();
						enableCompilerButton();
					}
				};

				worker.execute();
			}
		});

		/* delete output */
		inputIO.getOutputDeleteButton().addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				console.getConsoleField().setText("");

				SwingWorker<Object, Object> worker = new SwingWorker<Object, Object>(){

					@Override
					protected Object doInBackground() throws Exception {
						disableButtons();
						removeOutputDir();
						return null;
					}
					@Override
					protected void done() {
						super.done();
						enableCompilerButton();
					}
				};

				worker.execute();
			}
		});


		/* hadoop run */
		settings.getHadoopButton().addActionListener(new ActionListener() {

			public void actionPerformed(ActionEvent e) {

				console.getConsoleField().setText("");
				LOG.info("Evaluating the input query with Hadoop....\n");

				SwingWorker<Integer, Void> worker = new EngineWorker(true);

				disableButtons();
				worker.execute();



			}
		});


		/* spark run */
		settings.getSparkButton().addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {

				console.getConsoleField().setText("");
				LOG.info("Evaluating the input query with Spark on Hadoop....\n");

				SwingWorker<Integer, Void> worker = new EngineWorker(false);

				disableButtons();
				worker.execute();
			}
		});




		/* compiler button */
		settings.getCompileButton().addActionListener(new ActionListener() {
			private GumboQuery gumboQuery;

			public void actionPerformed(ActionEvent e) {

				disableButtons();
				// TODO why not write to stdout?
				console.getConsoleField().setText("");   
				resetMetrics();

				SwingWorker<Integer, Void> worker = new SwingWorker<Integer,Void>() {

					@Override
					protected Integer doInBackground() throws Exception {

						try {
							Set<GFExpression> inputQuery = new HashSet<>();
							Path output;
							Path scratch;
							RelationFileMapping inputs = new RelationFileMapping();

							String sout = inputIO.getOutputField().getText();
							if (sout.length() == 0) {
								throw new Exception("The output directory is empty");
							} else {
								LOG.info("The output directory is:" + sout);
								output = new Path(sout);
							}

							String sscratch = inputIO.getScratchField().getText();
							if (sscratch.length() == 0) {
								throw new Exception("The scratch directory is empty");
							} else {
								LOG.info("The scratch directory is:" + sscratch);
								scratch = new Path(sscratch);
							}


							LOG.info("Compiling the input queries...");

							//System.out.println("Testing, testing !!!!");

							GFInfixSerializer parser = new GFInfixSerializer();


							inputQuery = parser.GetGFExpression(GumboGUI.this.inputQuery.getQueryField().getText().trim());


							//textConsole.setText("");
							LOG.info("The following queries compiled:");

							for (GFExpression gf : inputQuery) {
								LOG.info(gf.toString());

							}



							LOG.info("Parsing the input directories...\n");


							String s = inputIO.getInputField().getText().trim();
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
							visualizer.setEdgeDetailsEnabled(settings.getEdgeDetailsEnabled());
							visualizer.setQueryDetailsEnabled(settings.getQueryDetailsEnabled());
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
}
