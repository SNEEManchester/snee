package uk.ac.manchester.cs.snee.manager.common;

import java.io.Serializable;

public class RunTimeSite implements Serializable
{
  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 2685187341478842025L;
  
  private Double currentEnergy = new Double(0); //J
  private Double currentAdaptationEnergyCost = new Double (0); //J
  private Double qepExecutionCost = new Double(0);;  //J
  //private Site representativeSite; 
  private String siteid = "";
  private Double thresholdEnergy = new Double(0);
  
  public RunTimeSite(Double currentEnergy, String siteID, Double qepExecutionCost)
  {
    this.currentEnergy = currentEnergy;
    this.siteid = siteID;
   // this.representativeSite = representativeSite;
    this.qepExecutionCost = qepExecutionCost;
    setThresholdEnergy(Math.max(currentEnergy / 100, qepExecutionCost));
  }
  
  public Double getQepExecutionCost()
  {
    return qepExecutionCost;
  }

  public void setQepExecutionCost(Double qepExecutionCost)
  {
    this.qepExecutionCost = qepExecutionCost;
  }
  
  public Double getCurrentEnergy()
  {
    return currentEnergy;
  }

  public void setCurrentEnergy(Double currentEnergy)
  {
    this.currentEnergy = currentEnergy;
  }

  public Double getCurrentAdaptationEnergyCost()
  {
    return currentAdaptationEnergyCost;
  }

  public void addToCurrentAdaptationEnergyCost(Double currentAdaptationEnergyCost)
  {
    this.currentAdaptationEnergyCost += currentAdaptationEnergyCost;
  }
  
  public void resetCurrentAdaptationEnergyCost()
  {
    this.currentAdaptationEnergyCost = new Double(0);
  }
  
  public void resetQEPExecutionEnergyCost()
  {
    this.qepExecutionCost = new Double(0);
  }
  
  public void resetEnergyCosts()
  {
    resetQEPExecutionEnergyCost();
    resetCurrentAdaptationEnergyCost();
  }
  
/*  public Site getRepresentativeSite()
  {
    return representativeSite;
  }

  public void setRepresentativeSite(Site representativeSite)
  {
    this.representativeSite = representativeSite;
  }*/

  public void removeQEPExecutionCost()
  {
    currentEnergy = currentEnergy - qepExecutionCost;
  }
  
  public void removeReprogrammingCostCost()
  {
    currentEnergy = currentEnergy - currentAdaptationEnergyCost;
  }
  
  public String toString()
  {
    return this.siteid;
  }

  public void setThresholdEnergy(Double thresholdEnergy)
  {
    this.thresholdEnergy = thresholdEnergy;
  }

  public Double getThresholdEnergy()
  {
    return thresholdEnergy;
  }

  public void resetAdaptEnergyCosts()
  {
    resetCurrentAdaptationEnergyCost();
  }

  public void removeDefinedCost(Double cost)
  {
    currentEnergy = currentEnergy - cost;
  }
  
}
