package gumbo.gui.gumbogui;

import javax.swing.JSplitPane;



public class PanelBA extends JSplitPane {
	
	//private WindowOne fw;
	//private WindowTwo sw;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public PanelBA(PanelB sw, PanelA fw) {
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);

		setOneTouchExpandable(false);
		setResizeWeight(0.5);
		setBorder(null);
		
	}
}