package mapreduce.guardedfragment.gumbogui.gumboguiMixIO;

import java.awt.TextField;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

public class GumboMainWindowMixIOwithScroll extends JFrame {
	
	public GumboMainWindowMixIOwithScroll(
			JEditorPane editorIQ, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel,
			TextField ip, TextField op, JButton browseIn, JButton browseOut) {
		
		super("GUMBO");
		
		GumboMainWindowMixIO mainwindow = new GumboMainWindowMixIO(editorIQ, textConsole, 
				buttonQC, buttonSche, buttonFH, buttonFS, cbLevel,
				ip, op, browseIn, browseOut);
		
		JScrollPane mainWin = new JScrollPane(mainwindow);
		
		add(mainWin);
		
	}

}
