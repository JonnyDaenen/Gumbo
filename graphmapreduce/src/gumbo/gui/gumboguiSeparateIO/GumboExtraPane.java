package gumbo.gui.gumboguiSeparateIO;

import java.awt.FlowLayout;

import javax.swing.JPanel;

public class GumboExtraPane extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GumboExtraPane (WindowThreeSeparateIO w) {
		
		setLayout(new FlowLayout());
		add(w);
		add(new JPanel());
		setVisible(true);
			
	}

}
