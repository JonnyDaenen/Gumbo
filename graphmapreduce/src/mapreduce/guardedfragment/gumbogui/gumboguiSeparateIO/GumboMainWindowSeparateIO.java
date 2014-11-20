package mapreduce.guardedfragment.gumbogui.gumboguiSeparateIO;

import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JTextArea;

import mapreduce.guardedfragment.gumbogui.JTextAreaOutputStream;


public class GumboMainWindowSeparateIO extends JFrame {
		

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public GumboMainWindowSeparateIO(JEditorPane editorIQ, JEditorPane editorI, JEditorPane editorO, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
		
		
				
		TextWindowSeparateIO textWin = new TextWindowSeparateIO(
				new WindowOneSeparateIO(editorIQ),new WindowTwoSeparateIO(editorI, editorO,textConsole));	
				
		WindowThreeSeparateIO buttonWin = new WindowThreeSeparateIO(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
		setLayout(new FlowLayout());
		add(textWin);
		add(buttonWin);
        setVisible(true);
      
	}
	
	
	public GumboMainWindowSeparateIO(
			JEditorPane editorIQ, JEditorPane editorI, JEditorPane editorO, 
			JTextAreaOutputStream textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
				
		TextWindowSeparateIO textWin = new TextWindowSeparateIO(new WindowOneSeparateIO(editorIQ),new WindowTwoSeparateIO(editorI, editorO,textConsole));	
				
		WindowThreeSeparateIO buttonWin = new WindowThreeSeparateIO(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
				
		setLayout(new FlowLayout());
		add(textWin);
		add(buttonWin);
        setVisible(true); 
	}
	
}