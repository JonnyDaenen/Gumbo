package mapreduce.guardedfragment.gumbogui.gumboguiSeparateIO;

import javax.swing.JSplitPane;



class TextWindowSeparateIO extends JSplitPane {
	
	//private WindowOne fw;
	//private WindowTwo sw;
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public TextWindowSeparateIO(WindowOneSeparateIO fw, WindowTwoSeparateIO sw) {
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);

		setOneTouchExpandable(false);
		setResizeWeight(0.7);
		setBorder(null);
		
	}
}