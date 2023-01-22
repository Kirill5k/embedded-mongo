package mongodb.embedded

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.mongodb.MongoCommandException
import mongo4cats.bson.{Document, ObjectId}
import mongo4cats.client.MongoClient
import mongo4cats.bson.syntax._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class EmbeddedMongoSpec extends AsyncWordSpec with Matchers with EmbeddedMongo {

  override protected val mongoPort: Int = 27018

  val testDoc = Document("_id" := ObjectId(), "stringField" := "string", "intField" := 1)

  "An EmbeddedMongo" should {

    "start embedded mongodb instance in the background" in
      withRunningEmbeddedMongo(20717) { connection =>
        MongoClient
          .fromConnection[IO](connection)
          .use { client =>
            for {
              db           <- client.getDatabase("db")
              coll         <- db.getCollection("coll")
              insertResult <- coll.insertOne(testDoc)
              foundDoc     <- coll.find.first
            } yield (insertResult, foundDoc)
          }
          .map { case (insertRes, foundDoc) =>
            foundDoc mustBe Some(testDoc)
            insertRes.wasAcknowledged() mustBe true
          }
      }.unsafeToFuture()(IORuntime.global)

    "not allow to connect to embedded instance without password" in
      withRunningEmbeddedMongo(20717, "user", "password") { connection =>
        MongoClient
          .fromConnectionString[IO](s"mongodb://${connection.toString().split("@")(1)}")
          .use { client =>
            for {
              db           <- client.getDatabase("db")
              coll         <- db.getCollection("coll")
              insertResult <- coll.insertOne(testDoc)
            } yield insertResult
          }
          .map(_ => fail("should not reach this"))
          .handleError(_ mustBe a[MongoCommandException])
      }.unsafeToFuture()(IORuntime.global)
  }

}
