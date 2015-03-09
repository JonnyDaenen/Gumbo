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
			JEditorPane editorIQ, JEditorPane editorI,
			JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
		
		
				
		TextWindowSeparateIO textWin = new TextWindowSeparateIO(
				new WindowOneSeparateIO(editorIQ),new WindowTwoSeparateIO(editorI, textConsole));	
				
		WindowThreeSeparateIO buttonWin = new WindowThreeSeparateIO(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
		setLayout(new FlowLayout());
		add(textWin);
		add(buttonWin);
        setVisible(true);
      
	}
	
	
	public GumboMainWindowSeparateIO(TextWindowSeparateIO tw, WindowThreeSeparateIO bw) {
		super("GUMBO");
		
		setLayout(new FlowLayout());
		add(tw);
		add(bw);
        setVisible(true);
		
		
	}
	
	
	
	
	public GumboMainWindowSeparateIO(DirOutWindow dw,
			JEditorPane editorIQ, JEditorPane editorI, 
			JTextAreaOutputStream textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
				
		
		TextWindowSeparateIO w1 = new TextWindowSeparateIO(new WindowOneSeparateIO(editorIQ),
				new WindowTwoSeparateIO(editorI,textConsole));
		WindowThreeSeparateIO w2 = new WindowThreeSeparateIO(buttonQC,buttonSche,
				buttonFH,buttonFS,cbLevel);
		GumboExtraPane w3 = new GumboExtraPane(dw,w2);
		add(new GumboMainSplitPaneSeparateIO(w1,w3));
		
		setVisible(true);
	}
	

	
}