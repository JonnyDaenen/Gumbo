package mapreduce.guardedfragment.gumbogui.gumboguiMixIO;

import javax.swing.JSplitPane;

import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIOBot;
import mapreduce.guardedfragment.gumbogui.gumboguiMixIO.TextWindowMixIOUp;

class TextWindowMixIO extends JSplitPane {
	

	private static final long serialVersionUID = 1L;

	public TextWindowMixIO(TextWindowMixIOUp fw, TextWindowMixIOBot sw) {
		
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);		
		

		setOneTouchExpandable(false);
		setResizeWeight(0.7);
		setBorder(null);
		
	}
}
