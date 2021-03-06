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

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.graph.Tree;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Path;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.RadioLink;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Topology;

/**
 * Class to represent a Routing Tree, data structure used to determine the 
 * data flows in the sensor network.
 *
 */
public class RT extends SNEEAlgebraicForm {

    /**
     * Logger for this class.
     */
    private Logger logger = Logger.getLogger(
    		RT.class.getName());    
    
    /**
     * Counter to assign unique id to different candidates.
     */
    protected static int candidateCount = 0;
	
    /**
     * The underlying site tree.
     */
	Tree siteTree;
	
	/**
	 * The PAF associated with this routing tree.
	 */
	PAF paf;
	
    /**
     * Constructor for the RoutingTree class.
     * @param queryName the name of the routing tree
     * @param inRootSite the sink node for the query
     */
	public RT(PAF paf, final String queryName, final Tree rt) {
		super(queryName);
		if (logger.isDebugEnabled())
			logger.debug("ENTER RT()"); 
		this.paf = paf;
		this.siteTree = rt;
		if (logger.isDebugEnabled())
			logger.debug("ENTER RT()"); 
	}
    
    /**
     * Resets the counter; use prior to compiling the next query.
     */
    public static void resetCandidateCounter() {
    	candidateCount = 0;
    }
	
	 /** {@inheritDoc} */
    protected String generateID(final String queryName) {
//		if (logger.isTraceEnabled())
//			logger.trace("ENTER generateID()"); 
    	candidateCount++;
//		if (logger.isTraceEnabled())
//			logger.trace("ENTER generateID()"); 
    	return queryName + "-RT-" + candidateCount;
    }
	
    /**
     * Gets the root site of the routing tree (i.e., the sink for the query). 
     * @return the root site/sink
     */
    public Site getRoot() {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER getRoot()");
    	if (logger.isDebugEnabled())
			logger.debug("RETURN getRoot()");
    	return (Site) this.siteTree.getRoot();
    }
    
    /**
     * Get site with given identifier.
     * @param id
     * @return
     */
    public final Site getSite(String id) {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER getSite()");
    	if (logger.isDebugEnabled())
			logger.debug("RETURN getSite()");
    	return (Site) this.siteTree.getNode(id);
    }

    /**
     * Given a source site, and a destination site downstream (i.e., further up
     * the tree), returns the path from the source site to the destination site.
     * @param sourceID the id for the source site
     * @param destID the id for the destination site
     * @return the path from the source to destination site
     */
    public final Path getPath(final String sourceID, final String destID) {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER getPath()");
    	Path path = new Path();
    	Site source = this.getSite(sourceID);
    	Site current = source;
    	while (current != null) {
    		path.append(current);
    		if (current.getID().equals(destID)) {
    			break;
    		}
    		current = (Site) current.getOutput(0);
    	}
    	
    	// a path was not found from the source and dest
    	if (path.getLastSite() == null) {
    		return new Path();
    	}
    	if (!path.getLastSite().getID().equals(destID)) {
    		return new Path();
    	}
    	if (logger.isDebugEnabled())
			logger.debug("RETURN getPath()");
    	return path;
    }
    
    /**
     * Iterator to traverse the routing tree.
     * The structure of the routing tree may not be modified during iteration
     * @param traversalOrder the order to traverse the routing tree
     * @return an iterator for the routing tree
     */    
    public final Iterator<Site> siteIterator(
    		final TraversalOrder traversalOrder) {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER siteIterator()");
    	if (logger.isDebugEnabled())
			logger.debug("RETURN siteIterator()");
    	return this.siteTree.nodeIterator(traversalOrder);
    }

    /**
     * Gets a list of sites.
     * @param desiredSites
     * @return
     */
	public ArrayList<Site> getSites(ArrayList<String> desiredSites) {
    	if (logger.isDebugEnabled())
			logger.debug("ENTER getSites()");
		ArrayList<Site> result = new ArrayList<Site>();
    	Iterator<String> siteIDIter = desiredSites.iterator();
		while (siteIDIter.hasNext()) {
			result.add(this.getSite(siteIDIter.next()));
		}
    	if (logger.isDebugEnabled())
			logger.debug("RETURN getSites()");
		return result;
	}    
    
	public int getMaxSiteID() {
		int maxSiteID = 0;
		Iterator<Site> sites = this.siteIterator(TraversalOrder.PRE_ORDER);
		while (sites.hasNext()) {
			Site site = sites.next();
			int siteID = new Integer(site.getID());
			if (siteID > maxSiteID) {
				maxSiteID = siteID;
			}
		}
		return maxSiteID;
	}
		
	 /** {@inheritDoc} */
	public String getDescendantsString() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getDescendantsString()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getDescendantsString()");
		return this.getID()+"-"+this.paf.getDescendantsString();
	}
	
	/**
	 * Gets the underlying PAF.
	 * @return
	 */
	public PAF getPAF() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getPAF()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getPAF()");
		return this.paf;
	}

	/**
	 * Returns the tree of sites.
	 * @return
	 */
	public Tree getSiteTree() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER getSiteTree()");
		if (logger.isDebugEnabled())
			logger.debug("RETURN getSiteTree()");
		return this.siteTree;
	}

	public Site getSite(int id) {
		return (Site) this.getSiteTree().getNode(id);
	}
	
    public RadioLink getRadioLink(final Site source, final Site dest) {
    	String id = siteTree.generateEdgeID(
    			source.getID(), dest.getID());
    	RadioLink link = (RadioLink) siteTree.getEdge(id);
    	return link;
    }
}
