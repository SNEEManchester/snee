package uk.ac.manchester.cs.snee.compiler.queryplan;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.log4j.Logger;

import uk.ac.manchester.cs.snee.common.SNEEProperties;
import uk.ac.manchester.cs.snee.common.SNEEPropertyNames;

public class SensorNetworkQueryPlanUtils {

	/**
	 * Logger for this class.
	 */
	private Logger logger = 
		Logger.getLogger(SensorNetworkQueryPlanUtils.class.getName());
	
	private SensorNetworkQueryPlan snqp;
	
	public SensorNetworkQueryPlanUtils(SensorNetworkQueryPlan snqp) {
		this.snqp = snqp;
	}
	
	public void generateQoSMetricsFile() {
		if (logger.isDebugEnabled())
			logger.debug("ENTER generateQoSMetricsFile()");
		try {
			String sep = System.getProperty("file.separator");
			String outputDir = SNEEProperties.getSetting(
					SNEEPropertyNames.GENERAL_OUTPUT_ROOT_DIR) +
					sep + snqp.getAgenda().getQueryName();
			String qosMetricsFilePath = outputDir + sep + "qos-metrics.json";
			generateQoSMetricsFile(qosMetricsFilePath);
		} catch (Exception e) {
		    logger.warn("Problem generating LAF image: ", e);
		}
		if (logger.isDebugEnabled())
			logger.debug("RETURN generateQoSMetricsFile()");
		}

	private void generateQoSMetricsFile(String qosMetricsFilePath) {
		try {
			PrintWriter out = new PrintWriter(new BufferedWriter(
					new FileWriter(qosMetricsFilePath)));
			out.println("{");
			out.println("\t\"alpha_ms\": \""+snqp.getAgenda().getAcquisitionInterval_ms()+"\"");
			out.println("\t\"beta_ms\": \""+snqp.getAgenda().getBufferingFactor()+"\"");
			out.println("\t\"delta_ms\": \""+snqp.getAgenda().getDeliveryTime_ms()+"\"");
			out.println("\t\"epsilon_J\": \""+snqp.getAgenda().getTotalEnergy()+"\"");
			out.println("\t\"lambda_s\": \""+snqp.getAgenda().getLifetime()+"\"");
			out.println("}");
			out.close();
		} catch (final IOException e) {
			logger.warn("Unable to produce QoS Metrics file", e);
		}
	}	
	
}
