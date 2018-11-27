package services

/**
  * Created by dcollarana on 6/5/2016.
  */

import org.apache.jena.rdf.model.Model

trait StorageTrait {

  def saveModel(model: Model, key: String)

  def getModel(key: String): Option[Model]

}

object DataCache extends StorageTrait {

  private var _model: Map[String, Option[Model]] = Map()

  override def saveModel(model: Model, key: String) {
    _model += (key -> Some(model))
  }

  def getModel(key: String): Option[Model] = _model(key)

}
