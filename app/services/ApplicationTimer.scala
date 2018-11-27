package services

import java.io.FileReader
import java.time.{Clock, Instant}

import javax.inject._
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.{Lang, RDFDataMgr}
import play.api.Logger
import play.api.cache._
import play.api.inject.ApplicationLifecycle

import scala.concurrent.Future

/**
 * This class demonstrates how to run code when the
 * application starts and stops. It starts a timer when the
 * application starts. When the application stops it prints out how
 * long the application was running for.
 *
 * This class is registered for Guice dependency injection in the
 * [[play.api.inject.Module]] class. We want the class to start when the application
 * starts, so it is registered as an "eager singleton". See the code
 * in the [[play.api.inject.Module]] class to see how this happens.
 *
 * This class needs to run code when the server stops. It uses the
 * application's [[ApplicationLifecycle]] to register a stop hook.
 */
@Singleton
class ApplicationTimer @Inject() (clock: Clock, appLifecycle: ApplicationLifecycle, environment: play.api.Environment, cache: AsyncCacheApi) {

  // This code is called when the application starts.
  private val start: Instant = clock.instant
  Logger.info(s"ApplicationTimer demo: Starting application at $start.")

  //paths to data files
  val actorsPath = "conf/data/dummy_d1.rdf"
  val polPath = "conf/data/dummy_d2.rdf"
  //val mixPath = "conf/data/e1_mixed.rdf"

  loadFileIntoCache(actorsPath, "actors")
  loadFileIntoCache(polPath, "politicians")
  //loadFileIntoCache(mixPath, "mixed")


  // When the application starts, register a stop hook with the
  // ApplicationLifecycle object. The code inside the stop hook will
  // be run when the application stops.
  appLifecycle.addStopHook { () =>
    val stop: Instant = clock.instant
    val runningTime: Long = stop.getEpochSecond - start.getEpochSecond
    Logger.info(s"ApplicationTimer demo: Stopping application at ${clock.instant} after ${runningTime}s.")
    Future.successful(())
  }

  //helper method to load ttl file into jena model and store into cache
  def loadFileIntoCache(path : String, key : String) : Unit  = {
    DataCache.saveModel(RDFDataMgr.loadModel(path), key)
  }
}
