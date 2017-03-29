package org.healthnlp.thyme.eval;

import java.io.File;
import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.ctakes.core.cc.XmiWriterCasConsumerCtakes;
import org.apache.ctakes.coreference.ae.DeterministicMarkableAnnotator;
import org.apache.ctakes.coreference.ae.MarkableHeadTreeCreator;
import org.apache.ctakes.coreference.ae.MarkableSalienceAnnotator;
import org.apache.ctakes.coreference.ae.MentionClusterCoreferenceAnnotator;
import org.apache.ctakes.temporal.ae.BackwardsTimeAnnotator;
import org.apache.ctakes.temporal.ae.DocTimeRelAnnotator;
import org.apache.ctakes.temporal.ae.EventAnnotator;
import org.apache.ctakes.temporal.ae.EventEventRelationAnnotator;
import org.apache.ctakes.temporal.ae.EventTimeRelationAnnotator;
import org.apache.ctakes.temporal.ae.THYMEQAAnaforaXMLReader;
import org.apache.ctakes.temporal.eval.EvaluationOfBothEEAndETRelations.AddClosure;
import org.apache.ctakes.temporal.eval.Evaluation_ImplBase.UriToDocumentTextAnnotatorCtakes;
import org.apache.ctakes.temporal.pipelines.FullTemporalExtractionPipeline.CopyPropertiesToTemporalEventAnnotator;
import org.apache.ctakes.temporal.pipelines.TemporalExtractionPipeline_ImplBase;
import org.apache.ctakes.typesystem.type.relation.CollectionTextRelation;
import org.apache.ctakes.typesystem.type.relation.RelationArgument;
import org.apache.ctakes.typesystem.type.relation.TemporalTextRelation;
import org.apache.ctakes.typesystem.type.syntax.ConllDependencyNode;
import org.apache.ctakes.typesystem.type.textsem.EventMention;
import org.apache.ctakes.typesystem.type.textsem.Markable;
import org.apache.ctakes.typesystem.type.textsem.TimeMention;
import org.apache.ctakes.utils.struct.MapFactory;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.component.ViewCreatorAnnotator;
import org.apache.uima.fit.factory.AggregateBuilder;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.pipeline.JCasIterable;
import org.apache.uima.fit.util.FSCollectionFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.jcas.tcas.Annotation;
import org.cleartk.util.ViewUriUtil;
import org.cleartk.util.ae.XmiWriter;
import org.cleartk.util.cr.UriCollectionReader;


public class EvaluationOfQa  extends
TemporalExtractionPipeline_ImplBase {

  public static void main(String[] args) throws Exception {
    File outDir = null;

    if(args.length < 2){
      System.err.println("Error: Two required arguments: <input dir> <anafora dir> [xmi output dir (optional)]");
      System.exit(-1);
    }

    if(args.length == 3){
      outDir = new File(args[2]);
      if(!outDir.isDirectory()){
        System.err.println("Error: Third argument must be an existing directory.");
        System.exit(-1);
      }
    }

    CollectionReaderDescription reader = UriCollectionReader.getDescriptionFromDirectory(new File(args[0]), XmlExcludingFileFilter.class, null);

    AggregateBuilder builder = new AggregateBuilder();
    builder.add(AnalysisEngineFactory.createEngineDescription(ViewCreatorAnnotator.class, ViewCreatorAnnotator.PARAM_VIEW_NAME, "GoldView"));
    builder.add(UriToDocumentTextAnnotatorCtakes.getDescription());
    builder.add(UriToDocumentTextAnnotatorCtakes.getDescription(), CAS.NAME_DEFAULT_SOFA, "GoldView");
    //		builder.add(AnalysisEngineFactory.createEngineDescription(THYMEQAAnaforaXMLReader.class, THYMEQAAnaforaXMLReader.PARAM_ANAFORA_DIRECTORY, new File(args[1])), );
    builder.add(THYMEQAAnaforaXMLReader.getDescription(new File(args[1])), CAS.NAME_DEFAULT_SOFA, "GoldView");
    builder.add(getPreprocessorAggregateBuilder().createAggregateDescription() );
    builder.add(EventAnnotator.createAnnotatorDescription());
    builder.add(AnalysisEngineFactory.createEngineDescription(CopyPropertiesToTemporalEventAnnotator.class));
    builder.add(DocTimeRelAnnotator.createAnnotatorDescription("/org/apache/ctakes/temporal/ae/doctimerel/model.jar"));
    builder.add(BackwardsTimeAnnotator.createAnnotatorDescription("/org/apache/ctakes/temporal/ae/timeannotator/model.jar"));
    builder.add(EventTimeRelationAnnotator.createAnnotatorDescription("/org/apache/ctakes/temporal/ae/eventtime/model.jar"));
    builder.add(EventEventRelationAnnotator.createAnnotatorDescription("/org/apache/ctakes/temporal/ae/eventevent/model.jar"));

    builder.add(AnalysisEngineFactory.createEngineDescription(DeterministicMarkableAnnotator.class));

    builder.add(MarkableSalienceAnnotator.createAnnotatorDescription("/org/apache/ctakes/temporal/ae/salience/model.jar"));
    builder.add(AnalysisEngineFactory.createEngineDescription(MarkableHeadTreeCreator.class));
    builder.add(MentionClusterCoreferenceAnnotator.createAnnotatorDescription("/org/apache/ctakes/coreference/models/mention-cluster/model.jar"));

//    builder.add(AnalysisEngineFactory.createEngineDescription(CoreferenceRelationClosure.class));
//    builder.add(AnalysisEngineFactory.createEngineDescription(AddClosure.class));

    // doctimerel things to track:
    int missingEvent = 0;
    int numGoldEvents = 0;
    int dtrWrong = 0;
    int dtrCorrect = 0;

    // Relation error types
    int numGoldRels = 0;
    int missingArg1 = 0;
    int missingArg2 = 0;
    int noTimex = 0;
    int noEvent = 0;
    int arg1NoRel = 0;
    int arg2NoRel = 0;
    int differentRel = 0;
    int wrongRelCat = 0;
    int correct = 0;

    for(JCas jcas : new JCasIterable(reader, builder.createAggregateDescription())){
      XmiCasSerializer.serialize(jcas.getCas(), new FileOutputStream(new File(outDir, new File(ViewUriUtil.getURI(jcas)).getName())));
      // Get the view containing gold standard events and relations:
      JCas goldView = jcas.getView("GoldView");

      // build a map from arguments to the relations they're in for later:
      Map<Annotation, List<TemporalTextRelation>> goldArgsToRel = new HashMap<>();
      Map<Annotation, List<TemporalTextRelation>> sysArgsToRel = new HashMap<>();
      for(TemporalTextRelation rel : JCasUtil.select(goldView, TemporalTextRelation.class)){
        addArgsToMap(goldArgsToRel, rel);
      }
      for(TemporalTextRelation rel : JCasUtil.select(jcas, TemporalTextRelation.class)){
        addArgsToMap(sysArgsToRel, rel);
      }

      for(CollectionTextRelation qaRel: JCasUtil.select(goldView, CollectionTextRelation.class)){
        String question = qaRel.getCategory();
        System.out.println("Question annotation: " + question );
        for(TOP answer : JCasUtil.select(qaRel.getMembers(), TOP.class)){
          if(answer instanceof TemporalTextRelation){
            TemporalTextRelation rel = (TemporalTextRelation) answer;
            System.out.println("Answer is a relation: " + rel.getCategory() + "(" + rel.getArg1().getArgument().getCoveredText() + ", " + rel.getArg2().getArgument().getCoveredText() + ")");
          }else if(answer instanceof EventMention) {
            if (goldArgsToRel.containsKey(answer)) {
              // the event answer is in a relation so the relation is the answer:
              List<TemporalTextRelation> rels = goldArgsToRel.get(answer);
              System.out.println("Answer is " + rels.size() + " relation(s), first one: " + rels.get(0).getCategory() + "(" + rels.get(0).getArg1().getArgument().getCoveredText() + ", " + rels.get(0).getArg2().getArgument().getCoveredText() + ")");
            } else {
              EventMention event = (EventMention) answer;
              System.out.println("Answer is a doctimerel: " + event.getEvent().getProperties().getDocTimeRel());
            }
          }else if(answer instanceof TimeMention) {
            TimeMention timex = (TimeMention) answer;
            System.out.println("Answer is a timex: " + timex.getCoveredText());
          }else{
            System.out.println("Answer is something else.");
          }
        }
      }

      // See how many of the gold events 1) are not in relations, and 2) have matching system events with same DTR
      for(EventMention goldEvent : JCasUtil.select(goldView, EventMention.class)){
        // first, ignore gold events that map to relations:
        if(goldArgsToRel.containsKey(goldEvent)) continue;

        List<EventMention> sysEvents = JCasUtil.selectCovered(jcas, EventMention.class, goldEvent.getBegin(), goldEvent.getEnd());
        if(sysEvents == null || sysEvents.size() == 0) {
          missingEvent++;
          System.out.println("System did not find gold DTR event with covered text: " + goldEvent.getCoveredText());
        }else {
          // find the event mention that is of exact class EventMention (rather than 1-word UMLS event)
          EventMention sysEvent = null;
          for (EventMention e : sysEvents) {
            if (e.getClass() == EventMention.class) {
              sysEvent = e;
              break;
            }
          }
          if (sysEvent == null) {
            missingEvent++;
            System.out.println("System did not find gold DTR event with covered text: " + goldEvent.getCoveredText());
          } else if (!goldArgsToRel.containsKey(sysEvent)) {
            String goldDtr = goldEvent.getEvent().getProperties().getDocTimeRel();
            String sysDtr = sysEvent.getEvent().getProperties().getDocTimeRel();
            if (goldDtr.equals(sysDtr)) {
              dtrCorrect++;
            } else {
              dtrWrong++;
            }
            System.out.println("Gold dtr is: " + goldDtr + " while sysDtr is: " + sysDtr);
          }
        }
        numGoldEvents++;
      }

      // See how many of the gold relations have matching relations in the system cas:
      // Need to find both events in system, both need to map to the same relation, and
      // then the category of the relation needs to be the same
      // we'll keep track of the different error modes:
      for(TemporalTextRelation goldRel : JCasUtil.select(goldView, TemporalTextRelation.class)){
        numGoldRels++;
        Annotation goldArg1 = goldRel.getArg1().getArgument();
        Annotation goldArg2 = goldRel.getArg2().getArgument();
        Annotation sysArg1=null, sysArg2 = null;

        boolean argError = false;
        // check for arg1
        List<? extends Annotation> sysEvents = JCasUtil.selectCovered(jcas, goldArg1.getClass(), goldArg1.getBegin(), goldArg1.getEnd());
        if(sysEvents.size() == 0){
          missingArg1++;
          argError = true;
          if(goldArg1 instanceof TimeMention){
            noTimex++;
            System.out.println("System did not find relation arg1 timex: " + goldArg1.getCoveredText());
          }
          if(goldArg1 instanceof EventMention){
            noEvent++;
            System.out.println("System did not find relation arg1 event: " + goldArg1.getCoveredText());
          }
        }else{
          for(Annotation e : sysEvents){
            if(e instanceof TimeMention || e.getClass() == EventMention.class){
              sysArg1 = e;
              break;
            }
          }
        }

        // check for arg2:
        sysEvents = JCasUtil.selectCovered(jcas, goldArg2.getClass(), goldArg2.getBegin(), goldArg2.getEnd());
        if(sysEvents.size() == 0){
          missingArg2++;
          argError = true;
          if(goldArg2 instanceof TimeMention){
            noTimex++;
            System.out.println("System did not find relation arg2 timex: " + goldArg2.getCoveredText());
          }
          if(goldArg2 instanceof EventMention){
            noEvent++;
            System.out.println("System did not find relation arg2 event: " + goldArg2.getCoveredText());
          }
        }else{
          for(Annotation e : sysEvents){
            if(e instanceof TimeMention || e.getClass() == EventMention.class){
              sysArg2 = e;
              break;
            }
          }
        }

        if(argError) continue;
        boolean noArgRel = false;

        // check for arg1 having a relation:
        if(!sysArgsToRel.containsKey(sysArg1)){
          arg1NoRel++;
          noArgRel = true;
          System.out.println("System did not have any links containing found arg1: " + sysArg1.getCoveredText());
        }
        List<TemporalTextRelation> rels1 = sysArgsToRel.get(sysArg1);

        // chcek for arg2 having a relation:
        if(!sysArgsToRel.containsKey(sysArg2)){
          arg2NoRel++;
          noArgRel = true;
          System.out.println("System did not have any links containing found arg2: " + sysArg2.getCoveredText());
        }
        if(noArgRel) continue;

        List<TemporalTextRelation> rels2 = sysArgsToRel.get(sysArg2);

        boolean argsMatch = false;
        boolean catMatch = false;
        for(TemporalTextRelation rel1 : rels1){
          for(TemporalTextRelation rel2 : rels2){
            if(rel1 == rel2){
              argsMatch = true;
              if(rel1.getCategory().equals(goldRel.getCategory())){
                catMatch = true;
              }
            }
          }
        }
        // check for both args being in the same relation:
        if(!argsMatch){
          differentRel++;
          System.out.println("System did not link related items: " + sysArg1.getCoveredText() + " and " + sysArg2.getCoveredText());
          continue;
        }

        // check for the relation having the right category:
        if(!catMatch){
          wrongRelCat++;
          continue;
        }

        correct++;
      }
    }

    //		System.out.println("Correctly identified correct DTR in " + dtrCorrect + " out of " + numGoldEvents + " events.");
    System.out.println(String.format("DTR Breakdown: MissingEvent=%d, WrongDtr=%d, CorrectDtr=%d, TotalEvents=%d", missingEvent, dtrWrong, dtrCorrect, numGoldEvents));
    System.out.println(String.format("Relation errors: NoTimex=%d, NoEvent=%d, NoArg1=%d, NoArg2=%d, NoRelArg1=%d, NoRelArg2=%d, DifferentRel=%d, WrongRelCat=%d, Correct=%d, TotalGoldRels=%d", 
        noTimex, noEvent, missingArg1, missingArg2, arg1NoRel, arg2NoRel, differentRel, wrongRelCat, correct, numGoldRels));
  }

  private static void addArgsToMap(Map<Annotation,List<TemporalTextRelation>> map, TemporalTextRelation rel){
    Annotation arg1 = rel.getArg1().getArgument();
    Annotation arg2 = rel.getArg2().getArgument();

    if(!map.containsKey(arg1)){
      map.put(arg1, new ArrayList<>());
    }
    map.get(arg1).add(rel);
    if(!map.containsKey(arg2)){
      map.put(arg2, new ArrayList<>());
    }
    map.get(arg2).add(rel);
  }

  public static class CoreferenceRelationClosure extends JCasAnnotator_ImplBase {

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
      // Iterate over every coreference chain -- for every element that is in a relation, add the relation to all other elements.
      // Will be a pain because coreference elements are type Markable, which is only used in coreference, while
      // relation types are RelationArguments that point to IdentifiedAnnotations.
      // We need a map from args to relations and then a map from markables to covering identifiedannotations with the same span.
      Map<Annotation,Set<TemporalTextRelation>> arg2rel = new HashMap<>();
      for(TemporalTextRelation rel : JCasUtil.select(jCas, TemporalTextRelation.class)){
        Annotation arg = rel.getArg1().getArgument();
        if(!arg2rel.containsKey(arg)){
          arg2rel.put(arg, new HashSet<>());
        }
        arg2rel.get(arg).add(rel);
        arg = rel.getArg2().getArgument();
        if(!arg2rel.containsKey(arg)){
          arg2rel.put(arg, new HashSet<>());
        }
        arg2rel.get(arg).add(rel);
      }

      Map<Markable, Set<Annotation>> markable2timeEnt = new HashMap<>();
      for(EventMention event : JCasUtil.select(jCas, EventMention.class)){
        if(event.getClass() == EventMention.class){

        }
      }
      
      for(CollectionTextRelation chain : JCasUtil.select(jCas, CollectionTextRelation.class)){
        if(!chain.getCategory().equals("Identity")) continue;
        
        List<Markable> members = new ArrayList<>(FSCollectionFactory.create(chain.getMembers(), Markable.class));
        Map<TemporalTextRelation,Integer> rel2Index = new HashMap<>();
        
        for(int i = 0; i < members.size(); i++){
          Markable member = members.get(i);
          for(EventMention event : JCasUtil.selectCovered(EventMention.class, member)){
            // are any of the events covered by this markable part of a relation?
            if(arg2rel.containsKey(event)){
              // this event belongs to a relation and is covered by this markable -- create new relations for all the
              // other markables in the chain:
              for(TemporalTextRelation rel : arg2rel.get(event)){
                for(int j = 0; j < members.size(); j++){
                  if(i == j) continue;
                  
                  Markable sibling = members.get(j);
                  // if we get it's head before getting covered events we make sure we get single token events (ignore larger span umls events)
                  ConllDependencyNode head = MapFactory.get(MarkableHeadTreeCreator.getKey(jCas), sibling);
                  List<EventMention> siblingEvents = new ArrayList<>(JCasUtil.selectCovered(EventMention.class, head));
                  EventMention siblingEvent = null;
                  if(siblingEvents.size() == 0) continue;
                  else if(siblingEvents.size() == 1){
                    siblingEvent = siblingEvents.get(0);
                  }
                  RelationArgument arg1 = new RelationArgument(jCas);
                  RelationArgument arg2 = new RelationArgument(jCas);

                  if(rel.getArg1().getArgument() == event){
                    // create arg1 from event covered by markable
                    arg1.setArgument(siblingEvent);
                    // steal arg2 from existing relation:
                    arg2.setArgument(rel.getArg2().getArgument());
                  }else{
                    // steal arg1 from existing relation:
                    arg1.setArgument(rel.getArg1().getArgument());
                    arg2.setArgument(siblingEvent);
                  }
                  
                  TemporalTextRelation transRel = new TemporalTextRelation(jCas);
                  transRel.setCategory(rel.getCategory());
                  transRel.setDiscoveryTechnique(5); // FIXME -- has no meaning other than for debugging. delete before checking in.
                  transRel.setArg1(arg1);
                  transRel.setArg2(arg2);
                  transRel.addToIndexes();
                }
              }
            }
          } 
        }       
      }
    }
  }

  public static class XmlExcludingFileFilter implements IOFileFilter {

    @Override
    public boolean accept(File file) {
      return !file.getName().endsWith(".xml") && file.getName().startsWith("ID");
    }

    @Override
    public boolean accept(File dir, String name) {
      return !name.endsWith(".xml") && name.startsWith("ID");
    }

  }
}

