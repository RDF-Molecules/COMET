package services

import com.typesafe.config.ConfigFactory
import org.apache.jena.rdf.model.{Model, ModelFactory, Resource, ResourceFactory, Statement}
import org.apache.jena.util.ResourceUtils
import play.Logger

case class Molecule(uri:Resource, var status : Option[LinkStatus])
case class LinkStatus(link: Molecule, similarity: Double)

trait Similarity {
  def getSimilarity(m1: Molecule, m2: Molecule): Double
}

object MoleculeManager extends Similarity {
  def convertToMolecules(model: Model): Seq[Molecule] = {
    val subjects = model.listSubjects()
    var molecules: Seq[Molecule] = Seq()
    while (subjects.hasNext) {
      molecules = molecules :+ Molecule(subjects.nextResource(), None)
    }
    molecules
  }

  def convertToModel(molecules : Seq[Molecule]) : Model = {
    var model = ModelFactory.createDefaultModel()
    molecules.foreach{ molecule =>
      model = model.add(molecule.uri.listProperties())
    }
    model
  }

  override def getSimilarity(m1: Molecule, m2: Molecule): Double = {
    ConfigFactory.load.getString("merge.similarity.metric") match {
      case "random" => scala.util.Random.nextFloat()
      case "label" => {
        val titleProperty = ResourceFactory.createProperty("http://schema.org/title")
        if (m1.uri.hasProperty(titleProperty) && m2.uri.hasProperty(titleProperty)) {
          val title_1 = m1.uri.getProperty(titleProperty).getObject.toString
          val title_2 = m2.uri.getProperty(titleProperty).getObject.toString
          val maxVal = math.max(title_1.length, title_2.length)
          val minVal = math.abs(title_1.length - title_2.length)
          val dist = (distance(title_1, title_2) - minVal).toFloat / (maxVal - minVal)
          1 - dist
        }
        else {
          0.0
        }
      }
      case _ => 0.0
    }
  }

  /*
  * levenshtein distance function
  */
  def distance(s1: String, s2: String): Int = {
    val dist = Array.tabulate(s2.length + 1, s1.length + 1) { (j, i) => if (j == 0) i else if (i == 0) j else 0 }
    @inline
    def minimum(i: Int*): Int = i.min

    for {j <- dist.indices.tail
         i <- dist(0).indices.tail} dist(j)(i) =
      if (s2(j - 1) == s1(i - 1)) dist(j - 1)(i - 1)
      else minimum(dist(j - 1)(i) + 1, dist(j)(i - 1) + 1, dist(j - 1)(i - 1) + 1)
    dist(s2.length)(s1.length)
  }

  def applySimilarityMetric(molecules: Seq[Molecule], mergedResults: Seq[Molecule]) : Seq[Molecule] = {
    molecules.foreach{ molecule=>
      mergedResults.foreach{ resultMolecule =>
        //compute similarity
        var similarity = MoleculeManager.getSimilarity(molecule, resultMolecule)
        if(similarity > ConfigFactory.load.getDouble("merge.similarity.threshold")){
          if(molecule.status.isEmpty || (molecule.status.isDefined && molecule.status.get.similarity < similarity)){
            Logger.info("Similar molecules! " + molecule.uri.getURI + " and " + resultMolecule.uri.getURI )
            //store the molecule link with highest similarity value
            molecule.status = Some(LinkStatus(resultMolecule, similarity))
          }
        }
      }
    }

    molecules
  }

  def addLinkedMolecules(moleculesWithLinks : Seq[Molecule], currentMoleculeSet : Seq[Molecule]) : Seq[Molecule] = {
    if(currentMoleculeSet.isEmpty){
      Logger.info("This is the first wrapper")
      moleculesWithLinks
    }
    else{
      Logger.info("Other wrappers")
      var datamap = Map(currentMoleculeSet map { m => m.uri -> m }: _*)
      moleculesWithLinks.foreach{ molecule =>
        if(molecule.status.isDefined){
          val mergedMolecule = merge(molecule, molecule.status.get.link, ConfigFactory.load.getString("merge.fusion.policy"))
          datamap = datamap - molecule.status.get.link.uri
          datamap = datamap + (mergedMolecule.uri -> mergedMolecule)
        }
        else{
          datamap = datamap + (molecule.uri -> molecule)
        }
      }
      val merged = datamap.values.toSeq
      merged
    }
  }

  def merge(m1: Molecule, m2: Molecule, fusionPolicy : String) : Molecule = {
    val uid = "http://vocab.lidakra.de/minte/merged_entity/" + java.util.UUID.randomUUID.toString
    val uris = (m1.uri.getURI, m2.uri.getURI)
    val mergedMolecule1 = ResourceUtils.renameResource(m1.uri, uid)
    val mergedMolecule2 = ResourceUtils.renameResource(m2.uri, uid)
    val it = mergedMolecule2.listProperties()
    while(it.hasNext){
      val prop = it.nextStatement()
      mergedMolecule1.addProperty(prop.getPredicate, prop.getObject)
    }
    if(fusionPolicy.equals("union")){
      mergedMolecule1.addProperty(ResourceFactory.createProperty("http://vocab.lidakra.de/fuhsen/origin"), uris._1)
      mergedMolecule1.addProperty(ResourceFactory.createProperty("http://vocab.lidakra.de/fuhsen/origin"), uris._2)
    }
    val merged = Molecule(mergedMolecule1, None)
    merged
  }

}
