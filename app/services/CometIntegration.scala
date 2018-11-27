package services

import breeze.linalg.DenseMatrix
import org.apache.jena.rdf.model.{ModelFactory, Property, RDFNode, ResourceFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.vocabulary.RDF
import play.Logger
import play.api.libs.json.JsObject

import scala.concurrent._
import ExecutionContext.Implicits.global
import collection.mutable.{HashMap, MultiMap, Set}
import scala.collection.mutable

/**
  * Created by mtasnim on 10/22/2018
  */
object CometIntegration {

  def bool2int(b:Boolean): Int = if (b) 1 else 0
  case class Predicate(predicate: Property, value: RDFNode)

  var Contexts  = new mutable.HashMap[String, mutable.Set[String]] with mutable.MultiMap[String, String]
  var FCAPropertyMap: Map[Int, Predicate] = Map[Int, Predicate]()
  var FCAMoleculeMap: Map[Int, Molecule] = Map[Int, Molecule]()
  var FP : Int = 0
  var TP : Int = 0
  var TN : Int = 0
  var FN : Int = 0

  def convertMoleculeToFCAMatrix(molecules: Seq[Molecule]): Array[Array[Int]] = {
    //Filtered according to context properties
    val properties = applyContextFilter(getUniqueProperties(molecules))
    Logger.info(s"Properties size: ${properties.length}")
    //Pointer back to molecule and property to know which FCA index is which molecule/property
    var mIndex = 0
    var pIndex = 0
    Logger.info(s"Molecules size: ${molecules.length}")
    var results = Array[Array[Int]]()
    molecules.foreach( m =>{
      FCAMoleculeMap = FCAMoleculeMap + ( mIndex -> m)
      mIndex +=1
      var row = Array[Int]()
      properties.foreach { prop =>
        FCAPropertyMap = FCAPropertyMap + ( pIndex -> prop)
        pIndex += 1
        row = row :+ bool2int(m.uri.hasProperty(prop.predicate, prop.value))
      }
      results = results :+ row
    })
    Logger.info(s"Results size: ${results.length}")
    results
  }

  def applyContextFilter(properties : Seq[Predicate]): Seq[Predicate] = {
    loadContexts()
    Logger.info(s"${Contexts.keySet}")
    var filteredProps = Seq[Predicate]()
    properties.foreach{ p =>
      //property check
      if(Contexts.keySet.contains(p.predicate.getURI)){
        //value check
        if(Contexts.entryExists(p.predicate.getURI, _ == "") || Contexts.entryExists(p.predicate.getURI, _ == p.value.toString)){
          filteredProps = filteredProps :+ p
        }
      }
    }
    Logger.info(s"$filteredProps")
    filteredProps
  }

  def getUniqueProperties(molecules : Seq[Molecule]): Seq[Predicate] = {
    var stmt = mutable.Set[Predicate]()
    molecules.foreach{ m =>
      val iterator = m.uri.listProperties()
      while(iterator.hasNext){
        val s = iterator.nextStatement()
        stmt += Predicate(s.getPredicate, s.getObject)
      }
    }
    stmt.toSeq
  }

  def loadContexts(): Unit = {
    val iterator = RDFDataMgr.loadModel("conf/context.nt").listStatements()
    while(iterator.hasNext){
      val stmt = iterator.nextStatement()
      var obj = ""
      if(!stmt.getObject.isAnon){
        obj = stmt.getObject.toString
      }
      Contexts.addBinding(stmt.getPredicate.getURI, obj)
    }
  }

  val applyFCA : Future[String] = scala.concurrent.Future {
    Logger.info("Calling FCA Service")
    val matrix = convertMoleculeToFCAMatrix(loadDatasets())
    val minteFca = new FCAService(DenseMatrix(matrix: _*))
    val results = minteFca.resultsToJson(minteFca.computeFca())
    val (tp, fp, fn, tn) = getMetrics(results)

    val precision = tp.toFloat / (tp + fp)
    val recall = tp.toFloat / (tp + fn)

    s"True Positives: $tp, \nFalse Positives: $fp, \nFalse Negatives: $fn, \nTrue Negatives: $tn, \nPrecision: $precision, Recall: $recall"
  }

  def containsAllContexts(pArray: Array[Int]) : Boolean = {
    var contextKeys = Contexts.keySet
    var resultContexts = mutable.Set[String]()
    pArray.foreach{ pIndex =>
      resultContexts += FCAPropertyMap(pIndex).predicate.getURI
    }

    (contextKeys -- resultContexts).isEmpty
  }

  def getMetrics(results: List[JsObject]) : (Int, Int, Int, Int) = {
    Logger.info("Inside Metrics")
    var tnMap = FCAMoleculeMap.keySet
    Logger.info(s"Number of elements: ${tnMap.size}")
    Logger.info(s"$results")
    results.foreach{ result =>
      val mArray = (result \ "molecules").as[Array[Int]]
      val pArray = (result \ "properties").as[Array[Int]]
      if(mArray.length > 1 && pArray.length > 0){
        //only if all context properties are present in FCA
        if(containsAllContexts(pArray)){
          Logger.info("Contains all contexts")
          val cmb = mArray.combinations(2).map{ case Array(x, y) => (x, y) }.toList
          cmb.foreach{ pair =>
            if(getEqualityUnderContext(FCAMoleculeMap(pair._1), FCAMoleculeMap(pair._2), "profession")){
              //True Positive
              TP += 1
            }
            else{
              FP += 1
            }
          }
          mArray.foreach(m => tnMap -= m)
        }
      }
    }

    Logger.info(s"Number of elements not integrated: ${tnMap.size}")

    tnMap.toSeq.combinations(2).map{ case Seq(x, y) => (x, y) }.toList.foreach{ pair =>
      if(getEqualityUnderContext(FCAMoleculeMap(pair._1), FCAMoleculeMap(pair._2), "profession")){
        FN += 1
      }
      else{
        TN += 1
      }
    }

    (TP, FP, FN, TN)
  }

  def parseFCAResults(result : List[JsObject]) : String = {
    var message = ""
    result.foreach{ obj =>
      val mArray = (obj \ "molecules").as[Array[Int]]
      val pArray = (obj \ "properties").as[Array[Int]]
      mArray.foreach( i => message += s"Molecule(${FCAMoleculeMap(i).uri.getURI}) ,")
      pArray.foreach( i => message += s"Has same property( ${FCAPropertyMap(i).predicate.getURI} ), ")
      message += "\n"
    }
    message
  }

  def createDummyMolecule() : Seq[Molecule] = {
    val model = ModelFactory.createDefaultModel()
    val res1 = model.createResource("abox:baspirin")
    res1.addProperty(ResourceFactory.createProperty("tbox:prod"), "abox:binc")
    res1.addProperty(ResourceFactory.createProperty("owl:sameAs"), "X")
    res1.addProperty(ResourceFactory.createProperty("tbox:chem"), "abox:aspirin")
    res1.addProperty(ResourceFactory.createProperty("rdf:type"), "Drug")


    val res2 = model.createResource("abox:caspirin")
    res2.addProperty(ResourceFactory.createProperty("tbox:prod"), "abox:cinc")
    res2.addProperty(ResourceFactory.createProperty("owl:sameAs"), "X")
    res2.addProperty(ResourceFactory.createProperty("tbox:chem"), "abox:aspirin")
    res2.addProperty(ResourceFactory.createProperty("rdf:type"), "Drug")

    Seq(Molecule(res1, None), Molecule(res2, None))
  }

  def loadDatasets() : Seq[Molecule] = {
    val aModel = DataCache.getModel("actors").get
    val pModel = DataCache.getModel("politicians").get
    //val mModel = DataCache.getModel("mixed").get

    MoleculeManager.convertToMolecules(aModel) ++ MoleculeManager.convertToMolecules(pModel) //++ MoleculeManager.convertToMolecules(mModel)
  }

  val propActor : (Property, String) = (RDF.`type`, "http://dbpedia.org/class/yago/Actor109765278")
  val propPol : (Property, String) = (RDF.`type`, "http://dbpedia.org/class/yago/Politician110450303")

  def getEqualityUnderContext(m1: Molecule, m2: Molecule, context: String) : Boolean = {
    context match {
      case "subject" => m1.uri.getURI.equals(m2.uri.getURI)
      case "profession" => m1.uri.getURI.equals(m2.uri.getURI) && ((m1.uri.hasProperty(propActor._1, propActor._2) && m2.uri.hasProperty(propActor._1, propActor._2)) || (m1.uri.hasProperty(propPol._1, propPol._2) && m2.uri.hasProperty(propPol._1, propPol._2)))
      case _ => false
    }
  }

}