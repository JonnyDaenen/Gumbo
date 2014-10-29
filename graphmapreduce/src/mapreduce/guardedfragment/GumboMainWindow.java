package mapreduce.guardedfragment;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Font;
import java.awt.GridLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.JCheckBox;
import javax.swing.JEditorPane;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTextArea;
import javax.swing.border.TitledBorder;




class GumboMainWindow extends JFrame {
		

	
	public GumboMainWindow(JEditorPane editorIQ, JEditorPane editorIO, JTextArea textConsole, JButton buttonQC, 
			JButton buttonSche, JButton buttonFH, JButton buttonFS, JCheckBox cbLevel) {
		super("GUMBO");
				
		TextWindow textWin = new TextWindow(new WindowOne(editorIQ),new WindowTwo(editorIO,textConsole));	
				
		WindowThree buttonWin = new WindowThree(buttonQC,buttonSche,buttonFH,buttonFS,cbLevel);
		
		setLayout(new FlowLayout());
		add(textWin);
		add(buttonWin);
        setVisible(true);
      
	}
}


class TextWindow extends JSplitPane {
	
	//private WindowOne fw;
	//private WindowTwo sw;
	
	public TextWindow(WindowOne fw, WindowTwo sw) {
		super(JSplitPane.VERTICAL_SPLIT,fw,sw);

		setOneTouchExpandable(false);
		setResizeWeight(0.5);		
		
	}
}


class WindowOne extends JPanel {
	
	//private JEditorPane editorIQ;
	
	public WindowOne(JEditorPane eIQ) {
		super();
		setLayout(new GridLayout(1,1,10,5));		
		

		JScrollPane editorIQScroll = new JScrollPane(eIQ);
		editorIQScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIQScroll.setPreferredSize(new Dimension(1240, 370));
		editorIQScroll.setMinimumSize(new Dimension(10, 10));
		
		
		editorIQScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT QUERIES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("courier new",1,14),Color.blue));
		
		add(editorIQScroll);
		
	}
}

class WindowTwo extends JPanel {
	
	
	public WindowTwo(JEditorPane eIO, JTextArea tC) {
		super();
		setLayout(new GridLayout(1,2,6,3));		
					

		JScrollPane editorIOScroll = new JScrollPane(eIO);
		editorIOScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIOScroll.setPreferredSize(new Dimension(620, 250));
		editorIOScroll.setMinimumSize(new Dimension(10, 10));
		editorIOScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT & OUTPUT FILES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(editorIOScroll);
				
		JScrollPane textConsoleScroll = new JScrollPane(tC);
		textConsoleScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		textConsoleScroll.setPreferredSize(new Dimension(620, 250));
		textConsoleScroll.setMinimumSize(new Dimension(10, 10));
		textConsoleScroll.setBorder(BorderFactory.createTitledBorder(null, "CONSOLE", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("Courier new",1,14),Color.blue));
		
		add(textConsoleScroll);
		
        setVisible(true);			
	}
}

class WindowThree extends JPanel {
	
	
	public WindowThree(JButton bQC, JButton bS, JButton bFH, JButton bFS, JCheckBox cbL) {
		super();
		setLayout(new GridLayout(2,4,20,-3));
		
		add(bQC);
		add(bS);
		add(bFH);
		add(bFS);	
		add(new JLabel(""));
		add(cbL);		
		add(new JLabel(""));
		add(new JLabel(""));
					
	}
		
}

