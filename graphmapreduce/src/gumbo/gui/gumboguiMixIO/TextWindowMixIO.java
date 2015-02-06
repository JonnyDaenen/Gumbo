package gumbo.gui.gumboguiMixIO;

import javax.swing.JSplitPane;

class TextWindowMixIO extends JSplitPane {
	

	private static final long serialVersionUID = 1L;

	public TextWindowMixIO(TextWindowMixIOUp fw, TextWindowMixIOBot sw) {
		
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);		
		

		setOneTouchExpandable(false);
		setResizeWeight(0.7);
		setBorder(null);
		
	}
}
