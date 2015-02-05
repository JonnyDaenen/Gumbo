package mapreduce.guardedfragment.gumbogui.gumboguiSeparateIO;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.border.TitledBorder;

import mapreduce.guardedfragment.gumbogui.JTextAreaOutputStream;

class WindowTwoSeparateIO extends JPanel {
	
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;


	public WindowTwoSeparateIO(JEditorPane eI, JEditorPane eO, JTextArea tC) {
		super();
		setLayout(new GridLayout(1,2,6,3));	
		
		JScrollPane editorInScroll = new JScrollPane(eI);
		editorInScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorInScroll.setPreferredSize(new Dimension(300, 190));
		//editorInScroll.setPreferredSize(new Dimension(300, 190));
		editorInScroll.setMinimumSize(new Dimension(10, 10));
		editorInScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		JScrollPane editorOutScroll = new JScrollPane(eO);
		editorOutScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorOutScroll.setPreferredSize(new Dimension(300, 190));
		editorOutScroll.setMinimumSize(new Dimension(10, 10));
		editorOutScroll.setBorder(BorderFactory.createTitledBorder(null, "OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		WindowTwoPrimeSeparateIO wtp = new WindowTwoPrimeSeparateIO(editorInScroll,editorOutScroll);
		
		add(wtp);
					
/*
		JScrollPane editorIOScroll = new JScrollPane(eIO);
		editorIOScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIOScroll.setPreferredSize(new Dimension(620, 250));
		editorIOScroll.setMinimumSize(new Dimension(10, 10));
		editorIOScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT & OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(editorIOScroll);
		*/
				
		JScrollPane textConsoleScroll = new JScrollPane(tC);
		textConsoleScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textConsoleScroll.setPreferredSize(new Dimension(620, 400));
		textConsoleScroll.setMinimumSize(new Dimension(10, 10));
		textConsoleScroll.setBorder(BorderFactory.createTitledBorder(null, "CONSOLE", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(textConsoleScroll);
		
        setVisible(true);			
	}
	
	
	public WindowTwoSeparateIO(JEditorPane eI, JEditorPane eO, JTextAreaOutputStream tC) {
		super();
		setLayout(new GridLayout(1,2,6,3));	
		
		JScrollPane editorInScroll = new JScrollPane(eI);
		editorInScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorInScroll.setPreferredSize(new Dimension(300, 190));
		editorInScroll.setMinimumSize(new Dimension(10, 10));
		editorInScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		JScrollPane editorOutScroll = new JScrollPane(eO);
		editorOutScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorOutScroll.setPreferredSize(new Dimension(300, 190));
		editorOutScroll.setMinimumSize(new Dimension(10, 10));
		editorOutScroll.setBorder(BorderFactory.createTitledBorder(null, "OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		WindowTwoPrimeSeparateIO wtp = new WindowTwoPrimeSeparateIO(editorInScroll,editorOutScroll);
		
		add(wtp);
					
/*
		JScrollPane editorIOScroll = new JScrollPane(eIO);
		editorIOScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIOScroll.setPreferredSize(new Dimension(620, 250));
		editorIOScroll.setMinimumSize(new Dimension(10, 10));
		editorIOScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT & OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(editorIOScroll);
		*/
				
		JScrollPane textConsoleScroll = new JScrollPane(tC.giveDest());
		textConsoleScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textConsoleScroll.setPreferredSize(new Dimension(300, 400));
		textConsoleScroll.setMinimumSize(new Dimension(10, 10));
		textConsoleScroll.setBorder(BorderFactory.createTitledBorder(null, "CONSOLE", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(textConsoleScroll);
		
        setVisible(true);			
	}
	
}