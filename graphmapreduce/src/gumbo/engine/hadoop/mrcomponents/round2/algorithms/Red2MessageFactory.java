package gumbo.engine.hadoop.mrcomponents.round2.algorithms;

import gumbo.engine.hadoop.mrcomponents.round2.reducers.GumboRed2Counter;
import gumbo.engine.hadoop.settings.HadoopExecutorSettings;
import gumbo.structures.data.RelationSchema;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class Red2MessageFactory {


	Text keyText;
	Text valueText;
	

	protected MultipleOutputs<Text, Text> mos;

	private Counter OUTR;
	private Counter OUTB;
	
	private Reducer<Text, Text, Text, Text>.Context context;

	// components
	private ExpressionSetOperations eso;

	// data
	Tuple t;
	String tRef;
	String proofBytes;
	String filename;

	public Red2MessageFactory(Reducer<Text, Text, Text, Text>.Context context, HadoopExecutorSettings settings, ExpressionSetOperations eso) {
		keyText = new Text();
		valueText = new Text();

		// ---
		this.context = context;
		this.eso = eso;

		// ---


		// ---
		OUTR = context.getCounter(GumboRed2Counter.RED2_OUT_RECORDS);
		OUTB = context.getCounter(GumboRed2Counter.RED2_OUT_BYTES);
		

		proofBytes = settings.getProperty(HadoopExecutorSettings.PROOF_SYMBOL);
		

		mos = new MultipleOutputs<>(context);


	}
	
	public void loadValue(Tuple t) {
		this.filename = generateFileName(t);
		valueText.clear();
		valueText.set(t.toString());
	}
	
	public void sendOutput() throws InterruptedException, IOException {
		OUTR.increment(1);
		OUTB.increment(valueText.getLength());
		sendMessage();
	}



	protected void sendMessage() throws InterruptedException, IOException{
		mos.write((Text)null, valueText, filename);
	}


	public void cleanup() throws IOException, InterruptedException {
		mos.close();
	}


	protected String generateFileName(Tuple t) {
		RelationSchema rs = new RelationSchema(t.getName(),t.size());
		// OPTIMIZE add cache
		Set<Path> paths = eso.getFileMapping().getPaths(rs);
		for (Path path: paths) {
			return path.toString() + "/" + rs.getName();
		}
		return ""; // FIXME fallback system + duplicate code in other reducer2
		
	}

	public Tuple getTuple(String value) {
		// OPTIMIZE this may be slow
		return new Tuple(value.substring(1));
	}

	public boolean isTuple(String value) {
		return value.startsWith(proofBytes);
	}





}
