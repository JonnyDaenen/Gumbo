package gumbo.engine.general.algorithms;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gumbo.engine.general.messagefactories.Red2MessageFactoryInterface;
import gumbo.engine.general.settings.AbstractExecutorSettings;
import gumbo.structures.booleanexpressions.BEvaluationContext;
import gumbo.structures.booleanexpressions.BExpression;
import gumbo.structures.booleanexpressions.BVariable;
import gumbo.structures.conversion.GFBooleanMapping;
import gumbo.structures.conversion.GFBooleanMapping.AtomNotFoundException;
import gumbo.structures.data.Tuple;
import gumbo.structures.gfexpressions.GFAtomicExpression;
import gumbo.structures.gfexpressions.GFExistentialExpression;
import gumbo.structures.gfexpressions.operations.ExpressionSetOperations;
import gumbo.structures.gfexpressions.operations.GFAtomProjection;

public class Red2Algorithm implements ReduceAlgorithm {

	private static final Log LOG = LogFactory.getLog(Red2Algorithm.class);

	Red2MessageFactoryInterface msgFactory;
	ExpressionSetOperations eso;



	private boolean guardRefOn;
	private boolean atomIdOn;


	// post-init vars
	private Tuple guardTuple;
	private GFBooleanMapping mapGFtoB;
	private BEvaluationContext booleanContext;
	private String key;
	private boolean lookingForGuard;
	boolean keyFound;


	public Red2Algorithm(ExpressionSetOperations eso, AbstractExecutorSettings settings, Red2MessageFactoryInterface msgFactory) {
		this.msgFactory = msgFactory;
		this.eso = eso;

		// --- opts
		guardRefOn = settings.getBooleanProperty(AbstractExecutorSettings.guardReferenceOptimizationOn);
		atomIdOn = settings.getBooleanProperty(AbstractExecutorSettings.requestAtomIdOptimizationOn);

		// counters

	}

	public void initialize(String key) throws AlgorithmInterruptedException {
		try {
			
//			System.out.println(key);

			mapGFtoB = eso.getBooleanMapping();
			booleanContext = new BEvaluationContext();
			lookingForGuard = guardRefOn;

			guardTuple = null;
			if (!lookingForGuard)
				guardTuple = new Tuple(key.getBytes(),key.length());

			this.key = key;

		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

	/**
	 * 
	 * @param split
	 * @return false if next values for this key may be skipped
	 * @throws AlgorithmInterruptedException
	 */
	public boolean processTuple(String val) throws AlgorithmInterruptedException {


//		System.out.println("\t" + val);

		try {

			// record guard tuple if found
			if (lookingForGuard && msgFactory.isTuple(val)) {
				if (lookingForGuard) {
					guardTuple = msgFactory.getTuple(val);
					lookingForGuard = false;
				} else
					return true;
			} 
			// otherwise we keep track of true atoms
			else {
				// extract atom reference and set it to true
				try {

					String [] values = val.split(":");

					for (String value : values) {
						String atomRef = value;
						BVariable atom;
						if (atomIdOn) {
							int id = Integer.parseInt(atomRef);
							GFAtomicExpression atomExp = eso.getAtom(id); 
							atom = mapGFtoB.getVariableIfExists(atomExp);
						} else {


							Tuple atomTuple = new Tuple(atomRef);
							GFAtomicExpression dummy = new GFAtomicExpression(atomTuple.getName(), atomTuple.getAllData());
							atom = mapGFtoB.getVariableIfExists(dummy);
						}
						booleanContext.setValue(atom, true);
					}
				} catch (AtomNotFoundException e) {
					// ignore bad values
					LOG.warn(e);
				}


			}


			return true;
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

	public void finish() throws AlgorithmInterruptedException {

		// if guard tuple is not present yet, throw exception
		if (lookingForGuard) {
			msgFactory.incrementExcept(1);
			throw new AlgorithmInterruptedException("There was no guard tuple found for key "+ key);
		} else {
			msgFactory.incrementTuples(1);
		}



		try {
			/* evaluate all formulas */
			for (GFExistentialExpression formula : eso.getExpressionSet()) {

				// only if applicable
				GFAtomicExpression guard = formula.getGuard();
				if (guard.matches(guardTuple)) {

					// get associated boolean expression
					BExpression booleanChildExpression = eso.getBooleanChildExpression(formula);


					// evaluate
					boolean eval = booleanChildExpression.evaluate(booleanContext);

					if (eval) {

						// calculate output tuple
						GFAtomProjection p = eso.getOutputProjection(formula);

						// send it
						msgFactory.loadValue(p.project(guardTuple));
						msgFactory.sendOutput();

						msgFactory.incrementTrue(1);

					} else {
						msgFactory.incrementFalse(1);
					}

				}
			}

		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

	public void cleanup() throws AlgorithmInterruptedException {
		try {
			msgFactory.cleanup();
		} catch(Exception e) {
			throw new AlgorithmInterruptedException(e);
		}
	}

}
