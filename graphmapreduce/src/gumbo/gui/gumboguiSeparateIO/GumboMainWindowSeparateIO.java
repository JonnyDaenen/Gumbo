package gumbo.gui.gumboguiSeparateIO;

import gumbo.gui.JTextAreaOutputStream;

import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JTextArea;


public class GumboMainWindowSeparateIO extends JFrame {
		

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public GumboMainWindowSeparateIO(
			JEditorPane editorIQ, JEditorPane editorI, JEditorPane editorO, 
			JTextArea textConsole, JButton buttonQC, 
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
				
		
		TextWindowSeparateIO w1 = new TextWindowSeparateIO(new WindowOneSeparateIO(editorIQ),
				new WindowTwoSeparateIO(editorI, editorO,textConsole));
		WindowThreeSeparateIO w2 = new WindowThreeSeparateIO(buttonQC,buttonSche,
				buttonFH,buttonFS,cbLevel);
		GumboExtraPane w3 = new GumboExtraPane(w2);
		add(new GumboMainSplitPaneSeparateIO(w1,w3));
		
		setVisible(true);
	}
	

	
}