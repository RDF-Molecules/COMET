package services
import breeze.linalg._
import play.api.Logger
import play.api.libs.json.Json

import scala.collection.mutable.ListBuffer
import util.control.Breaks._

class FCAService(ctx: DenseMatrix[Int]){

  val rows = for (i <- Range(0, ctx.cols))
    yield ctx(::, i).toArray
      .zipWithIndex
      .filter(_._1 == 1).map(_._2)

  val n = ctx.cols
  val Y = getY() //DenseVector(0,1,2,3,4,5,6,7)
  val X = getX() //DenseVector(0,1,2,3,4)

  val intents = new ListBuffer[DenseVector[Int]]()

  def print() = {
    Logger.info(s"Matrix: $ctx")
    Logger.info(s"Rows Size: ${rows.size}")
    Logger.info(s"n: $n")
  }

  def compute_closure(B : DenseVector[Int], y: Int): DenseVector[Int] = {
    //println(s"Computing Closure with B: $B and y: $y")
    val iB = B//initInternalB(B)
    // 1. for j from 0 upto n do
    // 2.  set D[j] to 1;
    val D = DenseVector.ones[Int](n)
    //4. foreach i in rows[y] do
    rows(y).foreach { i =>
      //println(s"Starting to work D$i")
      //5. set match to true;
      var iMatch = true
      //6. for j from 0 upto n do
      for(j <- Range(0, n)) {
        breakable {
          //7. if B[j] = 1 and context[i, j] = 0 then
          //println(s"Validation context[$i,$j] = ${ctx(i, j)}")
          if (iB(j) == 1 && ctx(i, j) == 0) {
            //8. set match to false;
            //9. break for loop
            iMatch = false
            break
          }
        }
      }
      //12. if match = true then
      if (iMatch) {
        //println(s"We have a match for Object $i")
        //13. for j from 0 upto n do
        for(j <- Range(0, n)) {
          //14. if context[i, j] = 0 then
          if(ctx(i,j) == 0) {
            //15. set D[j] to 0;
            D(j) = 0
          }
        }
      }
    }
    D
    //val dIndexes = D.toArray
    //  .zipWithIndex
    //  .filter(_._1 == 1).map(_._2)
    //DenseVector(dIndexes)
  }

  private def initInternalB(B: DenseVector[Int] ) : DenseVector[Int] = {
    val iB = DenseVector.zeros[Int](n)
    B.map{ m => iB(m) = 1 }
    iB
  }

  def generate_from (B : DenseVector[Int], y: Int): Unit = {
    //1. process B (e.g., print B on screen);
    //println(s"Intent B${y+1}: ${transformToIndexes(B)}")
    intents += B
    //2. if B = Y or y > n then
    //3.    return
    if (B == initInternalB(Y) || y > n ) {
      println("Returning due to condition: if B = Y or y > n ")
    }
    else {
      //5. for j from y upto n do
      for(j <- Range(y, n)) {
        //6. if B[j] = 0 then
        if (B(j) == 0) {
          //7. set B[j] to 1;
          B(j) = 1
          //8. set D to compute closure(B, j);
          val D = compute_closure(B, j)
          //9. set skip to false;
          var skip = false
          //10. for k from 0 upto j −1 do
          for(k <- Range(0, (j-1) )) {
            breakable {
              //11. if D[k] <> B[k] then
              if (D(k) != B(k)) {
                //12. set skip to true;
                skip = true
                //13. break for loop ;
                break
              }
            }
          }
          //16. if skip = false then
          if (!skip) {
            //17. generate_from(D, j +1);
            generate_from(D, j+1)
          }
          //19. set B[j] to 0 ;
          B(j) = 0
        }
      }
    }
  }

  def compute_galois(B : DenseVector[Int]) : DenseVector[Int] = {
    val result = X.map{ x =>
      var insert = true
      B.foreach{ y =>
        breakable {
          if (ctx(x, y) == 0) {
            insert = false
            break
          }
        }
      }
      if (insert)
        x
      else
        None
    }.findAll( x1 => x1.isInstanceOf[Int]).toArray
    DenseVector(result)
  }

  private def transformToIndexes(d: DenseVector[Int]) : DenseVector[Int] = {
    val dIndexes = d.toArray
      .zipWithIndex
      .filter(_._1 == 1).map(_._2)
    DenseVector(dIndexes)
  }

  private def getY() : DenseVector[Int] = {
    val y = DenseVector.zeros[Int](ctx.cols)
    for(k <- Range(0, ctx.cols)) {
      y(k) = k
    }
    y
  }

  private def getX() : DenseVector[Int] = {
    val x = DenseVector.zeros[Int](ctx.rows)
    for(k <- Range(0, ctx.rows)) {
      x(k) = k
    }
    x
  }

  def computeFca() : List[(DenseVector[Int],DenseVector[Int])] = {
    this.generate_from(DenseVector.zeros[Int](n), 0)
    intents.map{ intent =>
      val indexes = transformToIndexes(intent)
      (compute_galois(indexes) , indexes)
    }.toList.distinct
  }

  def printFca(values: List[(DenseVector[Int],DenseVector[Int])]) = {
    values.map( r =>
      r._1.toString.replace("DenseVector","").replace("(","{").replace(")","}")+","
        +
        r._2.toString.replace("DenseVector","").replace("(","{").replace(")","}")
    )
  }

  def resultsToJson(values: List[(DenseVector[Int], DenseVector[Int])]) = {
    values.map(r =>
      Json.obj(
        "molecules" -> Json.toJson(r._1.toArray),
        "properties" -> Json.toJson(r._2.toArray)
      )
    )
  }

}
