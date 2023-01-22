package mongodb.embedded

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import mongo4cats.bson.{Document, ObjectId}
import mongo4cats.client.MongoClient
import mongo4cats.database.MongoDatabase
import mongo4cats.bson.syntax._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AsyncWordSpec

import scala.concurrent.Future

class EmbeddedMongoSpec extends AsyncWordSpec with Matchers with EmbeddedMongo {

  override protected val mongoPort: Int = 27018

  val testDoc = Document("_id" := ObjectId(), "stringField" := "string", "intField" := 1)

  "An EmbeddedMongo" should {

    "start embedded mongodb instance in the background" in withEmbeddedMongoDatabase { db =>
      val result = for {
        coll <- db.getCollection("coll")
        insertResult <- coll.insertOne(testDoc)
        foundDoc <- coll.find.first
      } yield (insertResult, foundDoc)

      result.map { case (insertRes, foundDoc) =>
        foundDoc mustBe Some(testDoc)
        insertRes.wasAcknowledged() mustBe true
      }
    }
  }

  def withEmbeddedMongoDatabase[A](test: MongoDatabase[IO] => IO[A]): Future[A] =
    withRunningEmbeddedMongo { mongoConnection =>
      MongoClient
        .fromConnection[IO](mongoConnection)
        .use { client =>
          for {
            db <- client.getDatabase("db")
            res <- test(db)
          } yield res
        }
    }.unsafeToFuture()(IORuntime.global)
}
