package mapreduce.guardedfragment.gumbogui.gumboguiMixIO;

import java.awt.Color;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.GridLayout;

import javax.swing.BorderFactory;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.border.TitledBorder;

class TextWindowMixIOUp extends JPanel {
	

	private static final long serialVersionUID = 1L;

	public TextWindowMixIOUp(JEditorPane eIQ) {
		super();
		setLayout(new GridLayout(1,1,10,5));
		
		JScrollPane editorIQScroll = new JScrollPane(eIQ);
		editorIQScroll.setVerticalScrollBarPolicy(JScrollPane.VERTICAL_SCROLLBAR_ALWAYS);
		editorIQScroll.setPreferredSize(new Dimension(1000, 260));
		//editorIQScroll.setPreferredSize(new Dimension(1240, 210));
		editorIQScroll.setMinimumSize(new Dimension(10, 10));
		
		
		editorIQScroll.setBorder(BorderFactory.createTitledBorder(null, "INPUT QUERIES", 
				TitledBorder.CENTER, TitledBorder.TOP, new Font("courier new",1,14),Color.blue));
		
		add(editorIQScroll);
		
	}
}