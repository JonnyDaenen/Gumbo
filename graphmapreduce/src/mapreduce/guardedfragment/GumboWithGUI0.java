package mapreduce.guardedfragment;

import java.awt.Dimension;
import java.awt.Font;
import java.awt.TextField;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.PrintStream;
import java.util.Set;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFileChooser;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import mapreduce.guardedfragment.executor.hadoop.HadoopExecutor;
import mapreduce.guardedfragment.executor.spark.SparkExecutor;
import mapreduce.guardedfragment.planner.structures.MRPlan;
import mapreduce.guardedfragment.planner.structures.RelationFileMapping;
import mapreduce.guardedfragment.planner.structures.RelationFileMappingException;
import mapreduce.guardedfragment.planner.structures.data.RelationSchemaException;
import mapreduce.guardedfragment.structure.gfexpressions.GFExpression;
import mapreduce.guardedfragment.structure.gfexpressions.io.GFInfixSerializer;

public class GumboWithGUI0 {
	
	
	private static JEditorPane editorIQ;
	private static JTextArea textConsole;
	
	private static TextField inPath;
	private static TextField outPath;
	
	private static JButton buttonQC;
	private static JButton buttonSche;
	private static JButton buttonFH;
	private static JButton buttonFS;
	
	private static JCheckBox cbLevel;
	
	
	// Gumbo's variable
	
	private static Set<GFExpression> inputQuery;
	MRPlan plan;
	HadoopExecutor hadoopExec;
	SparkExecutor SparkExec;
	


	public static void main(String[] args) {
		
		editorIQ = new JEditorPane();
		editorIQ.setEditable(true);
		editorIQ.setFont(new Font("Courier New",0,16));
		
		textConsole = new JTextArea();
		textConsole.setEditable(false);
		textConsole.setFont(new Font("Courier New",1,12));
		
		JFileChooser inPathChooser = new JFileChooser();
		inPathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);

		JFileChooser outPathChooser = new JFileChooser();
		outPathChooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
		
		inPath = new TextField(94);
		outPath = new TextField(94);

		
		buttonQC = new JButton("Query Compiler");		
		buttonSche = new JButton("Jobs constructor");
		buttonFH = new JButton("GUMBO-Hadoop");
		buttonFS = new JButton("GUMBO-Spark");
		cbLevel = new JCheckBox("with schedule");
		
		//GumboMainWindow mainwindow = new GumboMainWindow(editorIQ, editorIn, editorOut, textConsole,buttonQC,
		//		buttonSche,buttonFH,buttonFS,cbLevel);
		
		GumboMainWindow0 mainwindow = new GumboMainWindow0(editorIQ, textConsole,buttonQC,
				buttonSche,buttonFH,buttonFS,cbLevel, inPath, outPath);
			
		Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();

        int height = screenSize.height;
        int width = screenSize.width;
        screenSize.setSize(width*(0.9), height*(0.9));
        int newheight = screenSize.height;
        int newwidth = screenSize.width;
		
		mainwindow.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		mainwindow.setSize(newwidth, newheight);
		mainwindow.setVisible(true);
		
		
	    buttonQC.addActionListener(new ActionListener() {
	        public void actionPerformed(ActionEvent e) {
	        	textConsole.setText("");
	        	textConsole.append("Compiling the input queries...\n");
	        	

	       	}
	    });
		
		
		
	}

}


