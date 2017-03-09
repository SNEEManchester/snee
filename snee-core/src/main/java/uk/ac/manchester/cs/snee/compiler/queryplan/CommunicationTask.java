/****************************************************************************\ 
*                                                                            *
*  SNEE (Sensor NEtwork Engine)                                              *
*  http://code.google.com/p/snee                                             *
*  Release 1.0, 24 May 2009, under New BSD License.                          *
*                                                                            *
*  Copyright (c) 2009, University of Manchester                              *
*  All rights reserved.                                                      *
*                                                                            *
*  Redistribution and use in source and binary forms, with or without        *
*  modification, are permitted provided that the following conditions are    *
*  met: Redistributions of source code must retain the above copyright       *
*  notice, this list of conditions and the following disclaimer.             *
*  Redistributions in binary form must reproduce the above copyright notice, *
*  this list of conditions and the following disclaimer in the documentation *
*  and/or other materials provided with the distribution.                    *
*  Neither the name of the University of Manchester nor the names of its     *
*  contributors may be used to endorse or promote products derived from this *
*  software without specific prior written permission.                       *
*                                                                            *
*  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS   *
*  IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, *
*  THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR    *
*  PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR          *
*  CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,     *
*  EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,       *
*  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR        *
*  PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF    *
*  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING      *
*  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS        *
*  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.              *
*                                                                            *
\****************************************************************************/
package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.util.HashSet;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.metadata.CostParameters;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.operators.logical.CardinalityType;

/**
 * This class represents a tasks which involves communication between two nodes in the query plan.
 * @author Ixent Galpin
 *
 */
public class CommunicationTask extends Task {

	//TODO move to task.
    //The site which is transmitting data
    private Site sourceNode;

    //The site which is receiving data
    private Site destNode;

    //The exchange components involved
    private HashSet<ExchangePart> exchangeComponents;

    //specifies whether task is sending or receiving
    private int mode;

    public static final int RECEIVE = 0;

    public static final int TRANSMIT = 1;

    private long alpha;

    private long beta;
    
    /**
     * Create an instance of a CommunicationTask.
     * @param startTime
     * @param sourceNode
     * @param destNode
     * @param exchangeComponents
     * @param mode
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */
    public CommunicationTask(final long startTime, final Site sourceNode,
	    final Site destNode,
	    final HashSet<ExchangePart> exchangeComponents,
	    final int mode, final long alpha, final long bufferingFactor, final DAF daf,  
	    CostParameters costParams) throws OptimizationException, SchemaMetadataException, TypeMappingException {
	super(startTime, costParams);
	this.sourceNode = sourceNode;
	this.destNode = destNode;
	this.exchangeComponents = exchangeComponents;
	this.beta = bufferingFactor;
	this.endTime = startTime + this.getTimeCost(daf);
	this.mode = mode;
	this.alpha = alpha;
    }

    public final Site getSourceNode() {
	return this.sourceNode;
    }

    public final String getSourceID() {
	return this.sourceNode.getID();
    }

    public final Site getDestNode() {
	return this.destNode;
    }

    public final String getDestID() {
	return this.destNode.getID();
    }

    public final int getMode() {
	return this.mode;
    }

    @Override
	public final Site getSite() {
	if (this.mode == RECEIVE) {
	    return this.destNode;
	} else {
	    return this.sourceNode;
	}
    }

    @Override
	public final String getSiteID() {
	return this.getSite().getID();
    }

    /**
     * Calculates the time overhead involved in a radio exchange.
     * 
     * Based on the time estimates provided in the OperatorsMetaData file.
     * 
     * @return time overhead of a radio message. 
     */
    public static long getTimeCostOverhead(CostParameters costParams) {
    	return (long) Math.ceil(costParams.getCallMethod()
    			+ costParams.getSignalEvent()
    			+ costParams.getTurnOnRadio()
    			+ costParams.getRadioSyncWindow() * 2
    			+ costParams.getTurnOffRadio());
    	}
        
//    /**
//     * Calculate the time to do thus Exchange.
//     * Includes overhead such as turning radio on and off. 
//     * @param card CardinalityType The type of cardinality to be considered.
//     * @param daf Distributed query plan this operator is part of.
//	 * @param round Defines if rounding reserves should be included or not
//     * @return Expression for the time cost.
//     */
//    private AlphaBetaExpression getTimeExpression(
//    		final CardinalityType card, final DAF daf, final boolean round) {
//    	AlphaBetaExpression result = new AlphaBetaExpression();
//    	final Iterator<ExchangePart> exchCompIter 
//			= this.exchangeComponents.iterator();
//    	while (exchCompIter.hasNext()) {
//    		result.add(exchCompIter.next().getTimeExpression(card, daf, round));
//    	}
//    	result.add(getTimeCostOverhead());
//    	return result;
//    }
//    
    /**
     * Returns the duration of this task.
     * @throws TypeMappingException 
     * @throws SchemaMetadataException 
     * @throws OptimizationException 
     */
    @Override
	protected final long getTimeCost(final DAF daf) throws OptimizationException, SchemaMetadataException, TypeMappingException {
    	long result = 0;
	
    	final Iterator<ExchangePart> exchCompIter 
    		= this.exchangeComponents.iterator();
    	while (exchCompIter.hasNext()) {
    		final ExchangePart exchComp = exchCompIter.next();
    		if ((exchComp.getComponentType() 
    					== ExchangePartType.PRODUCER)
    				|| (exchComp.getComponentType() 
    					== ExchangePartType.RELAY)) {
    			result += exchComp.getTimeCost(daf, beta, costParams);
    		}
    	}
    	result += getTimeCostOverhead(costParams);
//IG: Might be best to have this check...
//    	assert (result <= getTimeExpression(
//    			CardinalityType.PHYSICAL_MAX, daf, true)
//    			.evaluate(alpha, this.bufferingFactor));
    	return result;
    	
    	/*
	 	BetaExpression expr = this.getTXTimeBetaExpression(qp);
	 	return (long)expr.evaluate(bufferingFactor);		
    	*/
    }


    /**
     * String representation.
     */
    @Override
	public final String toString() {
	if (this.mode == RECEIVE) {
	    return "RX " + this.sourceNode.getID() + "_" + this.destNode.getID();
	}
	return "TX " + this.sourceNode.getID() + "_" + this.destNode.getID();
    }

    
    public HashSet<ExchangePart> getExchangeComponents() {
    	return this.exchangeComponents;
    }

}
