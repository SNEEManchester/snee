package uk.ac.manchester.cs.snee.sncb;

import java.io.BufferedInputStream;

import org.apache.log4j.Logger;

import net.tinyos.message.Message;

import uk.ac.manchester.cs.snee.common.Utils;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.operators.sensornet.SensornetDeliverOperator;

public class TinyOS_SNCB_Avrora extends TinyOS_SNCB implements SNCB 
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -8862168638276451033L;
  private Process avrora = null;
  private static final Logger logger = Logger.getLogger(TinyOS_SNCB_Avrora.class.getName());
  
  public TinyOS_SNCB_Avrora(double duration)throws SNCBException 
  {
    this.duration = duration;
    setup();
  }
  
  public TinyOS_SNCB_Avrora() throws SNCBException 
  {
    setup();
  }
  
  public SerialPortMessageReceiver register(SensorNetworkQueryPlan qep,
      String queryOutputDir, MetadataManager costParams)
      throws SNCBException {
    if (logger.isDebugEnabled())
      logger.debug("ENTER register()");
    SerialPortMessageReceiver mr = null;
    try {
      if (demoMode) {
        System.out.println("Query compilation complete.\n");
        System.in.read();
      }

      logger.trace("Generating TinyOS/nesC code for query plan.");
      System.out.println("Generating TinyOS/nesC code for query plan.");
      generateNesCCode(qep, queryOutputDir, costParams);

      if (demoMode) {
        System.out.println("nesC code generation complete.\n");
        System.in.read();
      }

      logger.trace("Compiling TinyOS/nesC code into executable images.");
      System.out
          .println("Compiling TinyOS/nesC code into executable images.");
      compileNesCCode(queryOutputDir);

      if (demoMode) {
        System.out.println("nesC code compilation complete.\n");
        System.in.read();
      }

      String avroraCommand = "";
      if (!this.useNodeController || this.serialPort==null) {
        System.out.println("Not using node controller, or no mote "+
            "plugged in, so unable to send query plan using" +
            "Over-the-air Programmer. ");
        System.out.println("Please proceed using manual commands.\n");
        if (this.target == CodeGenTarget.AVRORA_MICA2_T2 ||
            this.target == CodeGenTarget.AVRORA_MICAZ_T2) {
          avroraCommand = TinyOS_SNCB_Utils.printAvroraCommands(queryOutputDir, qep, 
              this.targetDirName, this.target);         
        }
        
        //set up running Avrora
        /*
        AvroraWrapper avrora = new AvroraWrapper(avroraCommand);
        Thread avroraThread = new Thread(avrora);
        avroraThread.setPriority(1);
        Thread.currentThread().setPriority(2);
        
        avroraThread.start();
        */
      //  String nescOutputDir = System.getProperty("user.dir") + "/"
       // + queryOutputDir + targetDirName;
        
        Runtime rt = Runtime.getRuntime();
        avrora = rt.exec("java avrora.Main " + avroraCommand);
        BufferedInputStream avroraReader = new BufferedInputStream(avrora.getInputStream());
        String outputString = "";
        System.out.println("waiting for avrora to initialise");
        boolean found = false;
        while(!found)
        {
          byte [] output;
          output = new byte[avroraReader.available()];
          avroraReader.read(output);
          String currentOutputString = new String(output);
          outputString = outputString.concat(currentOutputString);
          String test = "Waiting for serial connection on port 2390...";
          if(outputString.contains(test))
            found = true;
          else
          {
            Thread.currentThread();
            Thread.sleep(5000);
          }
        }
        System.out.println("avrora ready");
        avroraReader.close();
        //System.exit(0);
        mr = setUpResultCollector(qep, queryOutputDir);
        return mr;
      }
      else
      {

      logger.trace("Disseminating Query Plan images");
      System.out.println("Disseminating Query Plan images");
      disseminateQueryPlanImages(qep, queryOutputDir);

      if (demoMode) {
        System.out
            .println("Query plan image dissemination complete.\n");
        System.in.read();
      }

      logger.trace("Setting up result collector");
      System.out.println("Setting up result collector");
      mr = setUpResultCollector(qep, queryOutputDir);

      if (demoMode) {
        System.out.println("Serial port listener for query results ready.");
        System.in.read();
      }
      }

    } catch (Exception e) {
      if(avrora != null)
        avrora.destroy();
      e.printStackTrace();
      logger.warn(e.getLocalizedMessage(), e);
      throw new SNCBException(e.getLocalizedMessage(), e);
    }
    if (logger.isDebugEnabled())
      logger.debug("RETURN register()");
    return mr;
  }

  protected SerialPortMessageReceiver setUpResultCollector(
      SensorNetworkQueryPlan qep, String queryOutputDir) throws Exception {
    if (logger.isTraceEnabled())
      logger.trace("ENTER setUpResultCollector()");
    // TODO: need to set up plumbing for query result collection (using
    // mig?)
    String nescOutputDir = System.getProperty("user.dir") + "/"
        + queryOutputDir + targetDirName;
    String nesCHeaderFile = nescOutputDir + "/mote" + qep.getGateway()
        + "/QueryPlan.h";
    String outputJavaFile = System.getProperty("user.dir") + "/"
        + queryOutputDir + "DeliverMessage.java";
    String params[] = { "java", "-target=null",
        "-java-classname=DeliverMessage", nesCHeaderFile,
        "DeliverMessage", "-o", outputJavaFile };
    Utils.runExternalProgram("mig", params, this.tinyOSEnvVars, workingDir);
    String deliverMessageJavaClassContent = Utils
        .readFileToString(outputJavaFile);
    logger.trace("deliverMessageJavaClassContent="
        + deliverMessageJavaClassContent);
    // logger.trace("Using null;");
    // ClassLoader parentClassLoader = null;
    logger.trace("Using this.getClass().getClassLoader();");
    ClassLoader parentClassLoader = this.getClass().getClassLoader();
    // logger.trace("Using Thread.currentThread().getContextClassLoader();");
    // ClassLoader parentClassLoader =
    // Thread.currentThread().getContextClassLoader();
    // logger.trace("Using parentClassLoader=ClassLoader.getSystemClassLoader()");
    // ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader();
    // String messageJavaClassContent = Utils.readFileToString(
    // System.getProperty("user.dir")+"/src/mai)");
    MemoryClassLoader mcl = new MemoryClassLoader("DeliverMessage",
        deliverMessageJavaClassContent, parentClassLoader);
    Class<?> msgClass = mcl.loadClass("DeliverMessage");
    // Class msgClass = Class.forName("DeliverMessage", true, mcl);
    Object msgObj = msgClass.newInstance();
    // Message msg = new DeliverMessage(); // needed for web service, for
    // now.
    Message msg = (Message) msgObj;
    SensornetDeliverOperator delOp = (SensornetDeliverOperator) qep.getDAF()
        .getRootOperator();
    mr = new SerialPortMessageReceiver(null, delOp);
    mr.addMsgType(msg);
    if (logger.isTraceEnabled())
      logger.trace("RETURN setUpResultCollector()");
    return mr;
  }
  
  public void stop(SensorNetworkQueryPlan qep) throws SNCBException 
  {
    isStarted = false;
    avrora.destroy();
  }
  
  public void deregister(SensorNetworkQueryPlan qep) throws SNCBException {
    if (logger.isDebugEnabled())
      logger.debug("ENTER deregister()");
    if (logger.isDebugEnabled())
      logger.debug("RETURN deregister()");
  }

  
  public void start() throws SNCBException
  {
    
  }

  @Override
  public void waitForQueryEnd() throws InterruptedException
  {
    avrora.waitFor();
  }
  
}
