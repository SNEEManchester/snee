package uk.ac.manchester.cs.snee.sncb.tos;

import uk.ac.manchester.cs.snee.sncb.CodeGenTarget;
import uk.ac.manchester.cs.snee.sncb.SensorType;

public class SensorComponentUtils {

	
	public static String getNesCComponentName(SensorType sensorType, 
			CodeGenTarget platform) {
		
		if (platform==CodeGenTarget.TELOSB_T2) 
			return getTelosBNesCComponentName(sensorType);
		else if (platform==CodeGenTarget.AVRORA_MICA2_T2 || 
				platform==CodeGenTarget.AVRORA_MICAZ_T2) {
			return getAvroraNesCComponentName(sensorType);
		} else if (platform==CodeGenTarget.TOSSIM_T2) {
			return getTossimNesCComponentName(sensorType);
		}
		return null;
	}

	private static String getTelosBNesCComponentName(SensorType sensorType) {
		
		if (sensorType==SensorType.LIGHT) 
			return "HamamatsuS10871TsrC";
		if (sensorType==SensorType.PHOTO_SYNTHETIC_RADIATION) 
			return "HamamatsuS1087ParC";
		if (sensorType==SensorType.TOTAL_SOLAR_RADIATION) 
			return "HamamatsuS10871TsrC";
		if (sensorType==SensorType.TEMPERATURE) 
			return "SensirionSht11C";
		if (sensorType==SensorType.PRESSURE) 
			return "SensirionSht11C";
		if (sensorType==SensorType.VOLTAGE) 
			return "VoltageC";
		if (sensorType==SensorType.SEA_LEVEL) 
			return "SeaLevelC";
		
		return "HamamatsuS10871TsrC";
	}
	
	private static String getAvroraNesCComponentName(SensorType sensorType) {
		
		if (sensorType==SensorType.LIGHT) 
			return "PhotoC";
		if (sensorType==SensorType.PHOTO_SYNTHETIC_RADIATION) 
			return "PhotoC";
		if (sensorType==SensorType.TOTAL_SOLAR_RADIATION) 
			return "PhotoC";
		if (sensorType==SensorType.TEMPERATURE) 
			return "TempC";
		if (sensorType==SensorType.PRESSURE) 
			return "PhotoC";
		if (sensorType==SensorType.VOLTAGE) 
			return "VoltageC";
		if (sensorType==SensorType.SEA_LEVEL) 
			return "PhotoC";
		
		return "PhotoC";
	}

	private static String getTossimNesCComponentName(SensorType sensorType) {
		return "RandomSensorC";
	}

	public static String getNesCInterfaceName(SensorType sensorType, 
			CodeGenTarget platform) {
		
		if (platform==CodeGenTarget.TELOSB_T2) 
			return getTelosBNesCInterfaceName(sensorType);
		else if (platform==CodeGenTarget.AVRORA_MICA2_T2 || 
				platform==CodeGenTarget.AVRORA_MICAZ_T2) {
			return getAvroraNesCInterfaceName(sensorType);
		} else if (platform==CodeGenTarget.TOSSIM_T2) {
			return getTossimNesCInterfaceName(sensorType);
		}
		return null;
	}

	private static String getTelosBNesCInterfaceName(SensorType sensorType) {
		if (sensorType==SensorType.TEMPERATURE) 
			return "TempC";
		if (sensorType==SensorType.PRESSURE) 
			return "PhotoC";
		
		return "Read";
	}

	private static String getAvroraNesCInterfaceName(SensorType sensorType) {
		return "Read";
	}
	
	private static String getTossimNesCInterfaceName(SensorType sensorType) {
		return "Read";
	}
}
