package uk.ac.manchester.cs.snee.compiler.queryplan;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.operators.logical.LogicalOperator;

/**
 * Query Plan for execution on SNEE Evaluator.
 */
public class EvaluatorQueryPlan extends QueryExecutionPlan {

    /**
     * Logger for this class.
     */
    private static  Logger logger = Logger.getLogger(
    		EvaluatorQueryPlan.class.getName());
	
    /**
     * Constructor for EvaluatorQueryPlan.
     * @param dlaf
     * @param queryName
     */
	public EvaluatorQueryPlan(DLAF dlaf, String queryName)
	throws SchemaMetadataException, TypeMappingException {
		super(dlaf, queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER EvaluatorQueryPlan()");	
		
		LogicalOperator rootOperator = dlaf.getLAF().getRootOperator();
		metadata = new QueryPlanMetadata(rootOperator.getAttributes());
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN EvaluatorQueryPlan()");	
	}
}
