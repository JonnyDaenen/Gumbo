package gumbo.gui.dialogs;

import gumbo.generator.GFGenerator;
import gumbo.generator.GFGeneratorException;
import gumbo.generator.GFGeneratorInput;
import gumbo.generator.GFGeneratorInput.Relation;
import gumbo.generator.QueryType;
import gumbo.gui.panels.QueryInputField;
import gumbo.input.parser.GumboExporter;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.Frame;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Vector;

import javax.swing.ButtonGroup;
import javax.swing.DefaultCellEditor;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JDialog;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JScrollPane;
import javax.swing.JSpinner;
import javax.swing.JTable;
import javax.swing.SpinnerNumberModel;
import javax.swing.SwingConstants;
import javax.swing.border.EmptyBorder;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GeneratorDialog extends JDialog implements ActionListener {
	
	private static final Log LOG = LogFactory.getLog(GeneratorDialog.class);
	
	private static final long serialVersionUID = 1L;
	
	private QueryInputField _queryField;
	private GFGeneratorInput _input;
	private JComboBox<String> _typeList;
	private JSpinner _arity;
	private JTable _table;
	private JButton _confirmButton;
	private JButton _cancelButton;

	public GeneratorDialog(Frame owner, QueryInputField inputQuery, GFGeneratorInput input, String title) {
		super(owner, title, true);
		_queryField = inputQuery;
		_input = input;
		makeLayout();
	}
	
	private void makeLayout() {
		setLayout(new BorderLayout());
		JPanel toppanel = new JPanel(new GridLayout(2, 1));
		toppanel.add(createTypeList());
		toppanel.add(createArityField());
		
		getContentPane().add(toppanel, BorderLayout.PAGE_START);
		
		getContentPane().add(createTable(), BorderLayout.CENTER);
		getContentPane().add(createButtonPanel(), BorderLayout.PAGE_END);
    }

    /**
     * Pack and display the dialog
     */
    public void start() {
        pack();
        setLocationRelativeTo(null);
        setVisible(true);
    }
    
    public JPanel createTypeList() {
    	JPanel wrapper = new JPanel(new BorderLayout());
    	wrapper.setBorder(new EmptyBorder(10, 10, 10, 10));
    	
    	_typeList = new JComboBox<>();
		_typeList.addItem("AND");
		_typeList.addItem("OR");
		_typeList.addItem("XOR");
		_typeList.addItem("Negated AND");
		_typeList.addItem("Negated OR");
		_typeList.addItem("Negated XOR");
		_typeList.addItem("Unique");
		_typeList.addItemListener(new ItemListener() {
			
			@Override
			public void itemStateChanged(ItemEvent e) {
				if (e.getStateChange() == ItemEvent.SELECTED) {
					String item = (String) e.getItem();
					_arity.setEnabled(!item.equals("Unique"));
			    }
			}
		});
		
		wrapper.add(_typeList);

		return wrapper;
	}
    
    public JPanel createArityField() {
    	JPanel wrapper = new JPanel(new BorderLayout());
    	wrapper.setBorder(new EmptyBorder(10, 20, 10, 20));
    	
    	_arity = new JSpinner(new SpinnerNumberModel(0, 0, 1000, 1));
		
		JLabel label = new JLabel("Number of guarded atoms:");
		
		wrapper.add(label, BorderLayout.CENTER);
		wrapper.add(_arity, BorderLayout.EAST);

		return wrapper;
	}
    
    public JPanel createTable() {
    	_table = new JTable(new GeneratorTableModel(_input));
    	_table.setPreferredScrollableViewportSize(new Dimension(500, 70));
    	_table.setFillsViewportHeight(true);
    	_table.setRowSelectionAllowed(false);
    	_table.setCellSelectionEnabled(false);
    	_table.setColumnSelectionAllowed(false);
    	_table.setRowHeight(20);
    	_table.getColumn("Guard").setCellRenderer(
    	        new RadioButtonRenderer());
    	_table.getColumn("Guard").setCellEditor(
    	        new RadioButtonEditor(new JCheckBox()));
    	
    	JPanel wrapper = new JPanel(new BorderLayout());
    	wrapper.setBorder(new EmptyBorder(10, 20, 10, 20));
        wrapper.add(new JScrollPane(_table));
        
        return wrapper;
    }
    
    /**
     * Create the panel with cancel and add buttons
     * @return              Button panel
     */
    private JPanel createButtonPanel() {
        JPanel buttonPanel = new JPanel(new BorderLayout());
        buttonPanel.setBorder(new EmptyBorder(10, 10, 10, 10));
        _confirmButton = makeButton("OK");
        _cancelButton = makeButton("Cancel");
        JPanel panel = new JPanel(new GridLayout(1,2));
        panel.add(_cancelButton);
        panel.add(_confirmButton);
        buttonPanel.add(panel, BorderLayout.LINE_END);
        return buttonPanel;
    }
    
    /**
     * Create a button
     * @param text          Button text
     * @return              Styled button
     */
    private JButton makeButton(String text) {
        JButton button = new JButton(text);
        button.addActionListener(this);
        button.setPreferredSize(new Dimension(80, 30));
        button.setAlignmentX(JComponent.RIGHT_ALIGNMENT);
        return button;
    }

    @Override
    public void actionPerformed(ActionEvent e) {
        if(e.getSource() == _confirmButton)
            confirmInput();
        else if(e.getSource() == _cancelButton)
            dispose();
    }

    private void confirmInput() {
    	GFGenerator generator = new GFGenerator();
    	GeneratorTableModel model = (GeneratorTableModel) _table.getModel();
    	
    	Relation guard = model.getGuard();
    	generator.addGuardRelation(guard.name, guard.arity, guard.path, guard.format);
    	
    	Vector<Relation> guardeds = model.getGuardeds();
    	for (Relation guarded : guardeds) {
    		generator.addGuardedRelation(guarded.name, guarded.arity, guarded.path, guarded.format);
		}
    	

    	try {
    		String typeString = (String) _typeList.getSelectedItem();
    		if (typeString.equalsIgnoreCase("Unique")) {
    			generator.addUniqueQuery();
    		} else {
    			generator.addQuery(stringToType(typeString), (int) _arity.getValue()); 
    		}
    	} catch (GFGeneratorException e) {
    		LOG.error(e.getMessage());
    		dispose();
    	}
    	
    	GumboExporter exporter = new GumboExporter();
    	String script = exporter.export(generator.generate(generateName()));

    	_queryField.getQueryField().setText(script);    	
    	
        dispose();
    }
    
    private String generateName() {
		Format formatter = new SimpleDateFormat("yyyyMMd_HHmmss");
		String name = "query_" + formatter.format(new Date());
		
		return name;
	}
    
    private QueryType stringToType(String string) {
		if (string.equalsIgnoreCase("and"))
			return QueryType.AND;
		else if (string.equalsIgnoreCase("or"))
			return QueryType.OR;
		else if (string.equalsIgnoreCase("xor"))
			return QueryType.XOR;
		else if (string.equalsIgnoreCase("negated and"))
			return QueryType.NEGATED_AND;
		else if (string.equalsIgnoreCase("negated or"))
			return QueryType.NEGATED_OR;
		else if (string.equalsIgnoreCase("negated xor"))
			return QueryType.NEGATED_XOR;
		else 
			return QueryType.UNKNOWN;
	}

    class GeneratorTableModel extends AbstractTableModel {

		private static final long serialVersionUID = 1L;

		private String[] columnNames = {"Name",
                						"Path",
                						"Arity",
                                        "Guard",
                                        "Guarded"};
        
        private GFGeneratorInput _data;
        private JRadioButton[] _guard;
        private ButtonGroup _btnGroup;
        
        public GeneratorTableModel(GFGeneratorInput input) {
        	_data = input;
        	_guard = new JRadioButton[_data.getRelations().size()];
        	_btnGroup = new ButtonGroup();
        	for (int i = 0; i < _data.getRelations().size(); i++) {
        		JRadioButton button = new JRadioButton();
        		button.setHorizontalAlignment(SwingConstants.CENTER);
        		_guard[i] = button;
        		button.setBackground(Color.white);
        		_btnGroup.add(button);
        	}
        	
        	if (_guard.length > 0)
        		_guard[0].setSelected(true);
        }

        public int getColumnCount() {
            return columnNames.length;
        }

        public int getRowCount() {
            return _data.getRelations().size();
        }

        public String getColumnName(int col) {
            return columnNames[col];
        }

        public Object getValueAt(int row, int col) {
        	switch (col) {
        	case 0:
				return _data.getRelations().get(row).name;
        	case 1:
				return _data.getRelations().get(row).path;
        	case 2:
				return _data.getRelations().get(row).arity;
        	case 3:
				return _guard[row];//_data.getRelations().get(row).guard;
        	case 4:
				return _data.getRelations().get(row).guarded;
			default:
				return "";
			}
        }

        /*
         * JTable uses this method to determine the default renderer/
         * editor for each cell.  If we didn't implement this method,
         * then the last column would contain text ("true"/"false"),
         * rather than a check box.
         */
        public Class getColumnClass(int c) {
            return getValueAt(0, c).getClass();
        }

        /*
         * Don't need to implement this method unless your table's
         * editable.
         */
        public boolean isCellEditable(int row, int col) {
            //Note that the data/cell address is constant,
            //no matter where the cell appears onscreen.
            if (col < 3) {
                return false;
            } else {
                return true;
            }
        }

        /*
         * Don't need to implement this method unless your table's
         * data can change.
         */
        public void setValueAt(Object value, int row, int col) {
        	switch (col) {
        	case 0:
				_data.getRelations().get(row).name = (String) value;
				break;
        	case 1:
				_data.getRelations().get(row).path = (String) value;
				break;
        	case 2:
				_data.getRelations().get(row).arity = (int) value;
				break;
        	case 3:
        		_guard[row] = (JRadioButton) value;
				break;
        	case 4:
        		_data.getRelations().get(row).guarded = (Boolean) value;
				break;
        	default:
        		break;
        	}
        	fireTableCellUpdated(row, col);
        }
        
        public Relation getGuard() {
        	for (int i = 0; i < _guard.length; i++) {
				if (_guard[i].isSelected())
					return _data.getRelations().get(i);
			}
        	
        	return null;
        }
        
        public Vector<Relation> getGuardeds() {
        	Vector<Relation> guardeds = new Vector<>();
        	for (int i = 0; i < _data.getRelations().size(); i++) {
				if (_data.getRelations().get(i).guarded.booleanValue())
					guardeds.add(_data.getRelations().get(i));
			}
        	
        	return guardeds;
        }

    }

    class RadioButtonRenderer implements TableCellRenderer {
    	public Component getTableCellRendererComponent(JTable table, Object value,
    			boolean isSelected, boolean hasFocus, int row, int column) {
    		if (value == null)
    			return null;
    		return (Component) value;
    	}
    }

    class RadioButtonEditor extends DefaultCellEditor implements ItemListener {
		private static final long serialVersionUID = 1L;
		private JRadioButton button;

    	public RadioButtonEditor(JCheckBox checkBox) {
    		super(checkBox);
    	}

    	public Component getTableCellEditorComponent(JTable table, Object value,
    			boolean isSelected, int row, int column) {
    		if (value == null)
    			return null;
    		button = (JRadioButton) value;
    		button.addItemListener(this);
    		return (Component) value;
    	}

    	public Object getCellEditorValue() {
    		button.removeItemListener(this);
    		return button;
    	}

    	public void itemStateChanged(ItemEvent e) {
    		super.fireEditingStopped();
    	}
    }
}
