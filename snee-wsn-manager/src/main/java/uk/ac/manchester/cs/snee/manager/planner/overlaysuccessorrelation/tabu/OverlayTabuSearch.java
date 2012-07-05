package uk.ac.manchester.cs.snee.manager.planner.overlaysuccessorrelation.tabu;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.logging.Logger;

import com.rits.cloning.Cloner;

import uk.ac.manchester.cs.snee.SNEEException;
import uk.ac.manchester.cs.snee.common.SNEEConfigurationException;
import uk.ac.manchester.cs.snee.compiler.OptimizationException;
import uk.ac.manchester.cs.snee.compiler.WhenSchedulerException;
import uk.ac.manchester.cs.snee.compiler.queryplan.SensorNetworkQueryPlan;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.AutonomicManagerImpl;
import uk.ac.manchester.cs.snee.manager.common.Adaptation;
import uk.ac.manchester.cs.snee.manager.common.AutonomicManagerComponent;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.common.StrategyAbstract;
import uk.ac.manchester.cs.snee.manager.failednode.FailedNodeStrategyLocal;
import uk.ac.manchester.cs.snee.manager.failednode.cluster.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.manager.planner.ChoiceAssessor;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessor;
import uk.ac.manchester.cs.snee.manager.planner.common.OverlaySuccessorPath;
import uk.ac.manchester.cs.snee.metadata.MetadataManager;
import uk.ac.manchester.cs.snee.metadata.schema.SchemaMetadataException;
import uk.ac.manchester.cs.snee.metadata.schema.TypeMappingException;
import uk.ac.manchester.cs.snee.metadata.source.SourceMetadataAbstract;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;
import uk.ac.manchester.cs.snee.sncb.CodeGenerationException;

public class OverlayTabuSearch extends AutonomicManagerComponent
{
  private static final long serialVersionUID = -7392168097799519214L;
  private OverlaySuccessorPath bestPath = null;
  private OverlaySuccessor InitialSuccessor= null;
  private OverlaySuccessor currentBestSuccessor = null;
  private OverlayTABUList TABUList;
  private OverlayNeighbourhoodGenerator generator;
  private OverlayTABUSearchUtils Utils;
  private HashMap<String, RunTimeSite> initalSitesEnergy;
	private int currentNumberOfIterationsWithoutImprovement = 0;
	private SourceMetadataAbstract _metadata;
	private MetadataManager _metaManager;
	private File TABUOutputFolder = null;
	
  /**
   * constructor
   * @param alternativePlans
   * @param runningSites
   * @throws IOException 
   */
  public OverlayTabuSearch(AutonomicManagerImpl autonomicManager, 
                    HashMap<String, RunTimeSite> runningSites, 
                    SourceMetadataAbstract _metadata,
                    MetadataManager _metaManager, File outputFolder) 
  throws IOException
  {
    this.manager = autonomicManager;
	  this.initalSitesEnergy = runningSites;
	  this._metadata = _metadata;
	  this._metaManager = _metaManager; 
	  this.TABUOutputFolder = outputFolder;
	  this.Utils = new OverlayTABUSearchUtils(TABUOutputFolder.toString());
	  this.TABUList = new OverlayTABUList();
	  this.generator = new OverlayNeighbourhoodGenerator(TABUList, autonomicManager, _metaManager,
	                                              runningSites,_metadata, outputFolder);
  }
  
  /**
   * searches the search space looking for the best path from initial to final plan
   * @param initialPoint
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws CodeGenerationException 
   * @throws IOException 
   * @throws WhenSchedulerException 
   * @throws SNEEException 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  public OverlaySuccessorPath findSuccessorsPath(SensorNetworkQueryPlan initialPoint) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  IOException, CodeGenerationException,
  NumberFormatException, SNEEConfigurationException,
  SNEEException, WhenSchedulerException
  {
    //generate initial overlay network
    OverlaySuccessorPath currentPath = null;
    FailedNodeStrategyLocal localNodeFailureStrategy = new FailedNodeStrategyLocal(manager, _metadata, _metaManager);
    localNodeFailureStrategy.initilise(initialPoint, 48);
    LogicalOverlayNetwork network = localNodeFailureStrategy.getLogicalOverlay();
    initalSitesEnergy = this.updateRunningSites(initalSitesEnergy, initialPoint);
    InitialSuccessor = new OverlaySuccessor(initialPoint, this.initalSitesEnergy, 0, network);
    
    TABUList.addToTABUList(InitialSuccessor, 0, true);
    ArrayList<OverlaySuccessor> initialList = new ArrayList<OverlaySuccessor>();
    initialList.add(InitialSuccessor);
    currentPath = new OverlaySuccessorPath(initialList);
    bestPath = new OverlaySuccessorPath(initialList);
    currentBestSuccessor = InitialSuccessor;
    ArrayList<OverlaySuccessor> neighbourHood = new ArrayList<OverlaySuccessor>();
    try{
    int iteration = 0;
    int iterationsFailedAtInitial = 0;
    
    while(!OverlayStoppingCriteria.satisifiesStoppingCriteria(iteration, iterationsFailedAtInitial, 
                                                       this.bestPath.successorLength(), 
                                                       neighbourHood.size()))
    {
        neighbourHood = 
        generator.generateNeighbourHood(currentBestSuccessor, currentPath.successorLength() -1, iteration);
      
        OverlaySuccessor bestNeighbourHoodSuccessor = locateBestSuccessor(neighbourHood, iteration);
      if(bestNeighbourHoodSuccessor != null)
      {
        iterationsFailedAtInitial = 0;
        checkNewSuccessor(bestNeighbourHoodSuccessor, iteration, currentPath, neighbourHood);
      }
      else
      {
        if(currentPath.successorLength() -1 == 0)
        {
          iterationsFailedAtInitial ++;
        }
        possibleDiversitySetoff(currentPath, iteration, neighbourHood);
      }
      Utils.outputTABUList(iteration, TABUList);
      iteration++;
    }
    Utils.close();
    return bestPath;
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return null;
    }
  }
  
  /**
   * searches the search space looking for the best path from initial to final plan
   * @param initialPoint
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   * @throws CodeGenerationException 
   * @throws IOException 
   * @throws WhenSchedulerException 
   * @throws SNEEException 
   * @throws SNEEConfigurationException 
   * @throws NumberFormatException 
   */
  public OverlaySuccessorPath findSuccessorsPath(OverlaySuccessorPath initialPath)
  throws OptimizationException, SchemaMetadataException, TypeMappingException,
  IOException, CodeGenerationException,
  NumberFormatException, SNEEConfigurationException,
  SNEEException, WhenSchedulerException
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    OverlaySuccessorPath original = cloner.deepClone(initialPath);
    
    OverlaySuccessorPath currentPath = initialPath;
    initalSitesEnergy = initialPath.getSuccessorList().get(initialPath.successorLength() -1).getCopyOfRunTimeSites();
    InitialSuccessor = initialPath.getSuccessorList().get(initialPath.successorLength() -1);
    
    //add each successor to the tabuList
    ArrayList<OverlaySuccessor> successors = initialPath.getSuccessorList();
    for(int TABUpositionIndex = 1; TABUpositionIndex < initialPath.successorLength(); TABUpositionIndex++)
    {
      for(int successionIndex = 0; successionIndex < TABUpositionIndex;successionIndex ++)
      {
        TABUList.addToTABUList(successors.get(successionIndex), TABUpositionIndex, false);
      }
    }
    TABUList.addToTABUList(successors.get(0), 0, true);
    ArrayList<OverlaySuccessor> initialList = new ArrayList<OverlaySuccessor>();
    initialList.add(InitialSuccessor);
    bestPath = original;
    currentBestSuccessor = InitialSuccessor;
    ArrayList<OverlaySuccessor> neighbourHood = new ArrayList<OverlaySuccessor>();
    try{
    int iteration = 0;
    int iterationsFailedAtInitial = 0;
    
    while(!OverlayStoppingCriteria.satisifiesStoppingCriteria(iteration, iterationsFailedAtInitial, 
                                                       this.bestPath.successorLength(), 
                                                       neighbourHood.size()))
    {
        neighbourHood = 
        generator.generateNeighbourHood(currentBestSuccessor, currentPath.successorLength() -1, iteration);
      
      OverlaySuccessor bestNeighbourHoodSuccessor = locateBestSuccessor(neighbourHood, iteration);
      if(bestNeighbourHoodSuccessor != null)
      {
        iterationsFailedAtInitial = 0;
        checkNewSuccessor(bestNeighbourHoodSuccessor, iteration, currentPath, neighbourHood);
      }
      else
      {
        if(currentPath.successorLength() -1 == 0)
        {
          iterationsFailedAtInitial ++;
        }
        possibleDiversitySetoff(currentPath, iteration, neighbourHood);
      }
      Utils.outputTABUList(iteration, TABUList);
      iteration++;
    }
    Utils.close();
    return bestPath;
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return null;
    }
  }

  private void possibleDiversitySetoff(OverlaySuccessorPath currentPath, int iteration,
                                       ArrayList<OverlaySuccessor> neighbourHood)
  throws IOException, OptimizationException, SchemaMetadataException, TypeMappingException
  {
    currentNumberOfIterationsWithoutImprovement++;
    Utils.writeNoSuccessor(iteration);
    if(currentNumberOfIterationsWithoutImprovement >= allowance(currentPath.successorLength()))
    {
      if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
      {
        bestPath.updateList(currentPath.getSuccessorList());
      }
        currentBestSuccessor = 
          TABUList.engageDiversificationTechnique(neighbourHood, currentBestSuccessor, 
                                                  currentPath, iteration, Utils);
      //reset counter
      currentNumberOfIterationsWithoutImprovement = 0;
    }
  }

  private int allowance(int successorLength)
  {
    if(successorLength < 2)
      return 2;
    else
      return successorLength;
  }

  private void checkNewSuccessor(OverlaySuccessor bestNeighbourHoodSuccessor,
                                 int iteration, OverlaySuccessorPath currentPath, 
                                 ArrayList<OverlaySuccessor> neighbourHood) 
  throws SchemaMetadataException, TypeMappingException, IOException, OptimizationException, 
  CodeGenerationException
  {
    try{
    if(fitness(bestNeighbourHoodSuccessor, currentBestSuccessor, iteration) > currentBestSuccessor.getEstimatedLifetimeInAgendas())
    {
      Utils.writeNewSuccessor(bestNeighbourHoodSuccessor, iteration, currentBestSuccessor, currentPath);
      currentBestSuccessor = bestNeighbourHoodSuccessor;
      TABUList.addToTABUList(bestNeighbourHoodSuccessor, currentPath.successorLength() -1, false);
      currentPath.add(bestNeighbourHoodSuccessor);
      TABUList.addAllPathIntoTABUList(currentPath, currentPath.successorLength() -1, this.InitialSuccessor);
      
      if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
        bestPath.updateList(currentPath.getSuccessorList());
      currentNumberOfIterationsWithoutImprovement = 0;
    }
    else
    {
      Utils.writeFailedSuccessor(bestNeighbourHoodSuccessor, iteration,currentBestSuccessor , currentPath);
      TABUList.addToTABUList(bestNeighbourHoodSuccessor, currentPath.successorLength() -1, false);
      currentNumberOfIterationsWithoutImprovement++;
      if(currentNumberOfIterationsWithoutImprovement >= allowance(currentPath.successorLength()))
      {
        if(currentPath.overallAgendaLifetime() > bestPath.overallAgendaLifetime())
        {
          bestPath.updateList(currentPath.getSuccessorList());
        }
        currentBestSuccessor = 
           TABUList.engageDiversificationTechnique(neighbourHood, currentBestSuccessor, 
                                                  currentPath, iteration, Utils);
        //reset counter
        currentNumberOfIterationsWithoutImprovement = 0;
      }
    }
    }catch(Exception e)
    {
      e.printStackTrace();
    }
    
  }

  /**
   * determines the lifetime of the successor after adapting. 
   * @param successor
   * @return lifetime of the successor
   * @throws Exception 
   */
  private int fitness(OverlaySuccessor successor, OverlaySuccessor currentPosition, int iteration) 
  throws Exception
  {
    Adaptation adapt = StrategyAbstract.generateAdaptationObject(currentPosition.getQep(), successor.getQep());
    File assessorFolder = new File(this.TABUOutputFolder.toString() + sep + "Successor");
    if(assessorFolder.exists())
    {
      manager.deleteFileContents(assessorFolder);
      assessorFolder.mkdir();
    }
    else
    {
      assessorFolder.mkdir();
    }
    ChoiceAssessor assessAdaptation = new ChoiceAssessor(_metadata, _metaManager, assessorFolder, true);
    System.out.println("assessing successor " + successor.toString());
    try
    {
      assessAdaptation.assessChoice(adapt, successor.getTheRunTimeSites(), false);
      successor.substractAdaptationCostOffRunTimeSites(adapt);
      return successor.getEstimatedLifetimeInAgendas();
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return 0;
    }
  }

  /**
   * locates the best successor out of the neighbourhood
   * @param neighbourHood
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws CodeGenerationException 
   * @throws OptimizationException 
   * @throws IOException 
   */
  private OverlaySuccessor locateBestSuccessor(ArrayList<OverlaySuccessor> neighbourHood, int iteration) 
  throws SchemaMetadataException, TypeMappingException,
  IOException, OptimizationException, CodeGenerationException
  {
    Iterator<OverlaySuccessor> neighbourHoodIterator = neighbourHood.iterator();
    int currentBestLifetime = Integer.MIN_VALUE;
    OverlaySuccessor bestSuccessor = null;
    while(neighbourHoodIterator.hasNext())
    {
    	OverlaySuccessor successor = neighbourHoodIterator.next();
      int successorLifetimeTime = successor.getEstimatedLifetimeInAgendas();
      if(currentBestLifetime < successorLifetimeTime)
      {
        currentBestLifetime = successorLifetimeTime;
        bestSuccessor = successor;
      }
    }
    return bestSuccessor;
  }

  /**
   * updates a copy of the runTimeSites with the plans QEP.
   * @param runTimeSites
   * @return
   * @throws TypeMappingException 
   * @throws SchemaMetadataException 
   * @throws OptimizationException 
   */
  private HashMap<String, RunTimeSite> updateRunningSites(HashMap<String, RunTimeSite> sites,
                                                          SensorNetworkQueryPlan altPlan) 
  throws OptimizationException, SchemaMetadataException, TypeMappingException
  {
    Cloner cloner = new Cloner();
    cloner.dontClone(Logger.class);
    HashMap<String, RunTimeSite> runTimeSites = cloner.deepClone(sites);
    
    Iterator<Site> siteIter = altPlan.getIOT().getRT().siteIterator(TraversalOrder.POST_ORDER);
    while (siteIter.hasNext()) 
    {
      Site site = siteIter.next();
      double siteEnergyCons = altPlan.getAgendaIOT().getSiteEnergyConsumption(site); // J
      runTimeSites.get(site.getID()).setQepExecutionCost(siteEnergyCons);
    }
    return runTimeSites;
  }

}
