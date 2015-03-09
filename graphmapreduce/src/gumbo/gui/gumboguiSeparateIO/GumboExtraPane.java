package gumbo.gui.gumboguiSeparateIO;

import java.awt.FlowLayout;

import javax.swing.JPanel;

public class GumboExtraPane extends JPanel {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GumboExtraPane (DirOutWindow dw, WindowThreeSeparateIO w) {
		
		setLayout(new FlowLayout());
		add(dw);
		add(w);
		add(new JPanel());
		setVisible(true);
			
	}

}
