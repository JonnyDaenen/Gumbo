package gumbo.gui.gumbogui;

import gumbo.gui.JTextAreaOutputStream;

import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JTextArea;


public class GumboMainFrame extends JFrame {
		

	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	
	public GumboMainFrame(PanelDCBA p) {
		super("GUMBO");
		
		add(p);
		setVisible(true);
	}
	
	
	

	
}