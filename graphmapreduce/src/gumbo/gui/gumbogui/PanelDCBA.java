package gumbo.gui.gumbogui;


import gumbo.gui.JTextAreaOutputStream;

import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JSplitPane;

public class PanelDCBA extends JSplitPane {
	
	//private WindowOne fw;
	//private WindowTwo sw;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public PanelDCBA(PanelDC w2, PanelBA w1) {
		
		super(JSplitPane.VERTICAL_SPLIT,w1,w2);
		
		setOneTouchExpandable(false);
		//setDividerLocation(1400);
		setResizeWeight(0.9);
		setBorder(null);
		
	}


}
