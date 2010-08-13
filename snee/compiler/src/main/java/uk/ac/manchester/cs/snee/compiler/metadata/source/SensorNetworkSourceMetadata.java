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
package uk.ac.manchester.cs.snee.compiler.metadata.source;

import java.util.List;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import uk.ac.manchester.cs.snee.MetadataException;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.Topology;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReader;
import uk.ac.manchester.cs.snee.compiler.metadata.source.sensornet.TopologyReaderException;

/**
 * Maintains metadata required about a sensor network capable of
 * in-network query processing
 */
public class SensorNetworkSourceMetadata extends SourceMetadata {

	Logger logger = 
		Logger.getLogger(SensorNetworkSourceMetadata.class.getName());

	//TODO: Ixent doesn't understand what this is for.
	private int _cardinality = 100;

	private int[] _sourceNodes;

	//Sink node id of the sensor network
	private int[] _gateways;

	//Sensor Network topology
	private Topology _topology;
	
	//Maps extent names to source nodes
	private TreeMap<String,int[]> _extentToSitesMapping = 
		new TreeMap<String,int[]>(); 
	
	public SensorNetworkSourceMetadata(String sourceName, 
			List<String> extentNames, Element xml,
			String topFile, String resFile, int[] gateways) 
	throws SourceMetadataException, TopologyReaderException {
		super(sourceName, extentNames, SourceType.SENSOR_NETWORK);
		if (logger.isDebugEnabled()) {
			logger.debug("ENTER SensorNetworkSourceMetadata()");
		}
		if (gateways.length==0) {
			throw new SourceMetadataException("No gateway nodes specified "+
					"for sensor network "+sourceName);
		}
		if (gateways.length>1) {
			throw new SourceMetadataException("More than one gateway node " +
					"specified for sensor network "+sourceName + "; this is " +
					"currently not supported.");
		}
		this._gateways = gateways;
		this._topology = TopologyReader.readNetworkTopology(topFile, resFile);
		setSourceSites(xml.getElementsByTagName("extent"));
		verifyGateways();
		
		if (logger.isDebugEnabled())
			logger.debug("RETURN SensorNetworkSourceMetadata()");
	}

//	public boolean equals(Object ob) {
//		boolean result = false;
//		if (ob instanceof SensorNetworkSourceMetadata) {
//			SensorNetworkSourceMetadata source = (SensorNetworkSourceMetadata) ob;
//			//XXX: Not necessarily a complete check of source metadata equality
//			/*
//			 * Equality assumed if source refer to the same source type
//			 * and the same extent.
//			 */
//			if (logger.isTraceEnabled())
//				logger.trace("Testing " + source + "\nwith " + this);
//			if (source.getName().equals(_name) &&
//					source.getSourceType() == _sourceType)
//					result = true;
//		}
//		return result;
//	}
	
	private void verifyGateways() throws SourceMetadataException {

		for (int i=0; i<_gateways.length; i++) {
			int g = _gateways[i];
			if (_topology.getNode(g)==null) {
				throw new SourceMetadataException("Gateway node id "+g+
						" specified in the physical schema not found in "+
						"the topology file.");
			}
		}
		
	}

	/**
	 * Set which sites the sensed extents are available from
	 * @param nodesxml configuration information
	 * @throws SourceMetadataException
	 */
	private void setSourceSites(NodeList nodesxml) 
	throws SourceMetadataException {
		if (logger.isTraceEnabled())
			logger.trace("ENTER setSourceSites() for " +
					"number of extents " + nodesxml.getLength());
		StringBuffer sourceSitesText = new StringBuffer();
		for (int i = 0; i < nodesxml.getLength(); i++) {
			Node extentElem = nodesxml.item(i);
			NamedNodeMap attrs = extentElem.getAttributes();
			String extentName = attrs.getNamedItem("name").getNodeValue();
			logger.trace("extentName="+extentName);
			Node sitesElem = extentElem.getChildNodes().item(1);
			String sitesText = sitesElem.getFirstChild().getNodeValue();
			logger.trace("sites="+sitesText);
			if (sourceSitesText.length()==0) {
				sourceSitesText.append(sitesText);
			} else {
				sourceSitesText.append("," + sitesText);				
			}
			int[] sites = SourceMetadataUtils.convertNodes(sitesText);
			if (sites.length == 0) {
				String message = "No sites information found for "+extentName;
				logger.warn(message);
				throw new SourceMetadataException(message);
			}
			_extentToSitesMapping.put(extentName, sites);
			logger.trace("Extent "+extentName+": added source sites "+
					sites.toString());
		}
		if (logger.isTraceEnabled())
			logger.trace("sites text " + sourceSitesText);
		_sourceNodes = SourceMetadataUtils.convertNodes(
				sourceSitesText.toString());
		_cardinality = _sourceNodes.length;
		if (logger.isTraceEnabled())
			logger.trace("RETURN setSourceSites()");
	}


	/**
	 * Count the number of tokens.
	 * @param tokens
	 * @return
	 * @throws SourceMetadataException No tokens exist
	 */

	protected void setCardinality (int cardinality)
	{
		_cardinality = cardinality;
	}

	public int getCardinality() {
		return _cardinality;
	}

	@Override
	public String toString() {
		StringBuffer s = new StringBuffer(super.toString());
		s.append("   Cardinality: " + _cardinality);
		return s.toString();
	}

	/**
	 * Returns the nodes in the sensor network which are capable of
	 * sensing data.
	 * @return an array of node identifiers
	 */
	public int[] getSourceNodes() {
		return _sourceNodes;
	}

}
