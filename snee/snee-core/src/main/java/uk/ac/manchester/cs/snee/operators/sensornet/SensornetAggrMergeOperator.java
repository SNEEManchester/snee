package uk.ac.manchester.cs.snee.operators.sensornet;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.queryplan.DAF;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.AcquireOperator;
import uk.ac.manchester.cs.snee.operators.logical.AggregationOperator;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;
import uk.ac.manchester.cs.snee.operators.logical.DeliverOperator;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

public class SensornetAggrMergeOperator extends SensornetIncrementalAggregationOperator {

	private static Logger logger 
	= Logger.getLogger(SensornetAggrMergeOperator.class.getName());
	
	public SensornetAggrMergeOperator(LogicalOperator op, CostParameters costParams)
	throws SNEEException, SchemaMetadataException {
		super(op, costParams);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensornetAggrMergeOperator() " + op);
			logger.debug("Attribute List: " + op.getAttributes());
			logger.debug("Expression List: " + op.getExpressions());
		}	
		this.setNesCTemplateName("aggriter");
		this.setOperatorName("SensornetAGGRIter");
		if (logger.isDebugEnabled()) {
			logger.debug("RETURN SensornetAggrMergeOperator()");
		}		
	}

	/** {@inheritDoc} 
	 * @throws OptimizationException */
    public final double getTimeCost(final CardinalityType card, 
    		final Site node, final DAF daf) throws OptimizationException {
		final int tuples 
			= ((SensornetOperator)this.getInput(0)).getCardinality(card, node, daf);
		return getOverheadTimeCost()
			+ costParams.getDoCalculation() * tuples
			+ costParams.getCopyTuple();
    }
}
