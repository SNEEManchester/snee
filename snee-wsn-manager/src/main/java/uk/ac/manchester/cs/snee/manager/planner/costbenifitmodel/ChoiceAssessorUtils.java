package uk.ac.manchester.cs.snee.manager.planner.costbenifitmodel;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Iterator;

import uk.ac.manchester.cs.snee.compiler.queryplan.RT;
import uk.ac.manchester.cs.snee.compiler.queryplan.RTUtils;
import uk.ac.manchester.cs.snee.compiler.queryplan.TraversalOrder;
import uk.ac.manchester.cs.snee.manager.common.RunTimeSite;
import uk.ac.manchester.cs.snee.manager.failednodestrategies.logicaloverlaynetwork.logicaloverlaynetworkgenerator.LogicalOverlayNetwork;
import uk.ac.manchester.cs.snee.metadata.source.sensornet.Site;

public class ChoiceAssessorUtils extends RTUtils
{
   private HashMap<String, RunTimeSite> runningSites;
   private LogicalOverlayNetwork logicalOverlayNetwork = null;
   private String sep = System.getProperty("file.separator");
   
   public ChoiceAssessorUtils(HashMap<String, RunTimeSite> runningSites, RT rt)
   {
     super(rt);
     this.runningSites = runningSites;
   }
   
   public ChoiceAssessorUtils(LogicalOverlayNetwork logicalOverlayNetwork, 
                              HashMap<String, RunTimeSite> runningSites, RT rt)
   {
     super(rt);
     this.runningSites = runningSites;
     this.logicalOverlayNetwork = logicalOverlayNetwork;
   }
   
   public void exportWithEnergies(String fname, String label)
   { 
     if(label == null)
       label = "";
     
     if(logicalOverlayNetwork == null)
       exportRTWithEnergies( fname, label);
     else
       exportOverlayWithEnergies(fname, label);
       
   }
   
   
   private void exportOverlayWithEnergies(String fname, String label)
  {
     DecimalFormat df = new DecimalFormat("#.#####");
     try {
         final PrintWriter out = new PrintWriter(new BufferedWriter(
           new FileWriter(fname)));
         String edgeSymbol;
       out.println("digraph \"" + this.rt.getID() + "\" {");
       edgeSymbol = "->";
         out.println("size = \"8.5,11\";"); // do not exceed size A4
         out.println("label = \"" + label + "\";");
         out.println("rankdir=\"BT\";");
         Iterator<String> siteIDIter = this.logicalOverlayNetwork.siteIdIterator();
           this.tree.nodeIterator(TraversalOrder.POST_ORDER);
         while (siteIDIter.hasNext()) {
         final String currentSite = siteIDIter.next();
         out.print(currentSite + " [fontsize=20 ");
         out.print("label = \"" + currentSite);
         out.print("\\n energy stock: " + df.format(runningSites.get(currentSite).getCurrentEnergy()) + "\\n ");
         out.print("QEP Cost: " + df.format(runningSites.get(currentSite).getQepExecutionCost()) + "\\n ");
         out.print("Adapt Cost: " + df.format(runningSites.get(currentSite).getCurrentAdaptationEnergyCost()) + "\\n ");
         out.println("\"];");
         }
         //traverse the edges now
         Iterator<Site> siteIter = logicalOverlayNetwork.getQep().getIOT().siteIterator(TraversalOrder.POST_ORDER);
         while (siteIter.hasNext()) {
         final Site currentSite = siteIter.next();
         if (currentSite.getOutputs().length==0)
           continue;
         Site parentSite = (Site) currentSite.getOutput(0);
         
         out.print("\"" + currentSite.getID() + "\""
             + edgeSymbol + "\""
             + parentSite.getID() + "\" ");
             out.print("[");

         out.println("style = dashed]; ");
       }       out.println("}");
         out.close();

     }
     catch (Exception e) 
     {
     }
    
  }

  private void exportRTWithEnergies(String fname, String label)
   { 
     DecimalFormat df = new DecimalFormat("#.#####");
   try {
       final PrintWriter out = new PrintWriter(new BufferedWriter(
         new FileWriter(fname)));
       String edgeSymbol;
     out.println("digraph \"" + this.rt.getID() + "\" {");
     edgeSymbol = "->";
       out.println("size = \"8.5,11\";"); // do not exceed size A4
       out.println("label = \"" + label + "\";");
       out.println("rankdir=\"BT\";");
       Iterator<Site> siteIter = 
         this.tree.nodeIterator(TraversalOrder.POST_ORDER);
       while (siteIter.hasNext()) {
       final Site currentSite = siteIter.next();
       out.print(currentSite.getID() + " [fontsize=20 ");
       if (currentSite.isSource()) {
         out.print("shape = doublecircle ");
       }
       out.print("label = \"" + currentSite.getID());
       out.print("\\n energy stock: " + df.format(runningSites.get(currentSite.getID()).getCurrentEnergy()) + "\\n ");
       out.print("QEP Cost: " + df.format(runningSites.get(currentSite.getID()).getQepExecutionCost()) + "\\n ");
       out.print("Adapt Cost: " + df.format(runningSites.get(currentSite.getID()).getCurrentAdaptationEnergyCost()) + "\\n ");
       out.println("\"];");
       }
       //traverse the edges now
       siteIter = this.tree.nodeIterator(TraversalOrder.POST_ORDER);
       while (siteIter.hasNext()) {
       final Site currentSite = siteIter.next();
       if (currentSite.getOutputs().length==0)
         continue;
       Site parentSite = (Site) currentSite.getOutput(0);
       
       out.print("\"" + currentSite.getID() + "\""
           + edgeSymbol + "\""
           + parentSite.getID() + "\" ");
           out.print("[");

       out.println("style = dashed]; ");
     }       out.println("}");
     out.flush();
     out.close();

   }
   catch (Exception e) 
   {
     e.printStackTrace();
   }
   }
  
  /**
   * ouputs the energies left by the network once the qep has failed
   * @param successor
   */
  public void networkEnergyReport(HashMap<String, RunTimeSite> runtimeSites, File outputFolder)
  {
    try 
    {
      DecimalFormat formatter = new DecimalFormat("#.000000");
      final PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(outputFolder.toString() + sep + "energyReport")));
      Iterator<String> keys = runtimeSites.keySet().iterator();
      while(keys.hasNext())
      {
        String key = keys.next();
        RunTimeSite site = runtimeSites.get(key);
        Double leftOverEnergy = site.getCurrentEnergy();
        out.println("Node " + key + " has residual energy " + 
            formatter.format(leftOverEnergy) + " and qep Cost of " + site.getQepExecutionCost()) ; 
      }
      out.flush();
      out.close();
    }
    catch(Exception e)
    {
      System.out.println("couldnt write the energy report");
    }
  }
   
   
  
}
