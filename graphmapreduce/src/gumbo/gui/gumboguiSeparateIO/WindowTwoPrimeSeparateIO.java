package gumbo.gui.gumboguiSeparateIO;

import javax.swing.JScrollPane;
import javax.swing.JSplitPane;

class WindowTwoPrimeSeparateIO extends JSplitPane {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public WindowTwoPrimeSeparateIO(JScrollPane eI, JScrollPane eO) {
			
		super(JSplitPane.VERTICAL_SPLIT,eI,eO);

		setOneTouchExpandable(false);
		setResizeWeight(0.5);
		setBorder(null);
		

	}
}