package gumbo.gui.gumboguiSeparateIO;


import gumbo.gui.JTextAreaOutputStream;

import java.awt.FlowLayout;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JSplitPane;

public class GumboMainSplitPaneSeparateIO extends JSplitPane {
	
	//private WindowOne fw;
	//private WindowTwo sw;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	public GumboMainSplitPaneSeparateIO(TextWindowSeparateIO w1, GumboExtraPane w2) {
		
		super(JSplitPane.VERTICAL_SPLIT,w1,w2);
		
		setOneTouchExpandable(false);
		//setDividerLocation(1400);
		setResizeWeight(0.99);
		setBorder(null);
		

		
		
	}


}
