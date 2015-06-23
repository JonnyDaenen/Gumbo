package gumbo.gui.panels;

import gumbo.convertors.GFConversionException;
import gumbo.convertors.hive.GFHiveConverter;
import gumbo.convertors.hive.GFHiveConverterLong;
import gumbo.convertors.hive.GFHiveConverterWide;
import gumbo.input.GumboQuery;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.BorderFactory;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JEditorPane;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.border.EmptyBorder;

public class HivePanel extends JPanel {

	private static final long serialVersionUID = 1L;

	private JEditorPane _hivefield;
	private JPanel _options;
	private JButton _convertBtn;

	private GumboQuery _query;
	private Method _method;

	private enum Method {
		WIDE, 
		LONG
	} 


	public HivePanel() {
		_query = null;
		_method = Method.WIDE;
		setLayout(new BorderLayout());

		_hivefield = new JEditorPane();
		_hivefield.setEditable(false);
		_hivefield.setFont(new Font("monospaced", Font.PLAIN, 12));
		JScrollPane scroll = new JScrollPane(_hivefield);
		scroll.setPreferredSize(new Dimension(400,300));
		scroll.setMinimumSize(new Dimension(200, 200));


		ActionListener methodActionListener = new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				String method = e.getActionCommand();
				if (method == "wide")
					_method = Method.WIDE;
				else if (method == "long")
					_method = Method.LONG;
			}
		};

		JRadioButton radioBtnWide = new JRadioButton("Method 1: Wide Queryplan");
		radioBtnWide.setSelected(true);
		radioBtnWide.addActionListener(methodActionListener);
		radioBtnWide.setActionCommand("wide");
		JRadioButton radioBtnLong = new JRadioButton("Method 2: Long Queryplan");
		radioBtnLong.addActionListener(methodActionListener);
		radioBtnLong.setActionCommand("long");
		ButtonGroup optionsGroup = new ButtonGroup();
		optionsGroup.add(radioBtnWide);
		optionsGroup.add(radioBtnLong);		

		_convertBtn = new JButton("Generate Hive Script");
		_convertBtn.addActionListener(new ActionListener() {
			@Override
			public void actionPerformed(ActionEvent e) {
				convert();				
			}
		});

		_options = new JPanel();
		_options.setBorder(new EmptyBorder(10, 10, 10, 10));
		_options.setLayout(new BoxLayout(_options, BoxLayout.PAGE_AXIS));
		_options.add(radioBtnWide);
		_options.add(radioBtnLong);
		_options.add(_convertBtn);
		_options.setBorder( BorderFactory.createCompoundBorder(
				BorderFactory.createTitledBorder("Options"),
				BorderFactory.createEmptyBorder(5,5,5,5)));


		add(scroll, BorderLayout.CENTER);
		add(_options, BorderLayout.EAST);
	}

	public void setQuery(GumboQuery query) {
		_query = query;
	}


	private void convert() {
		if (_query == null) {
			_hivefield.setText("Please compile the query in the 'Query' tab before generating the Hive code.");
			return;
		}

		GFHiveConverter converter = null;
		if (_method == Method.WIDE)
			converter = new GFHiveConverterWide();
		else
			converter = new GFHiveConverterLong();

		String hivescript = "";
		try {
			hivescript = converter.convert(_query);
			_hivefield.setText(hivescript);				
		} catch (GFConversionException e) {
			e.printStackTrace();
			_hivefield.setText(e.getMessage());
		}
	}

}