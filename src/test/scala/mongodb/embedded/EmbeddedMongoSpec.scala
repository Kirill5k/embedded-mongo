package mongodb.embedded

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.mongodb.{MongoCommandException, MongoSecurityException}
import mongo4cats.bson.{Document, ObjectId}
import mongo4cats.client.MongoClient
import mongo4cats.bson.syntax._
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class EmbeddedMongoSpec extends AsyncWordSpec with Matchers with EmbeddedMongo {

  val testDoc = Document("_id" := ObjectId(), "stringField" := "string", "intField" := 1)

  "An EmbeddedMongo" should {

    "start embedded mongodb instance on random port" in
      withRunningEmbeddedMongo { address =>
        MongoClient
          .fromConnectionString[IO](address.connectionString)
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

    "start embedded mongodb instance on specified port" in
      withRunningEmbeddedMongo(20717) { _ =>
        MongoClient
          .fromConnectionString[IO]("mongodb://localhost:20717")
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
      withRunningEmbeddedMongo(20717, "user", "password") { address =>
        MongoClient
          .fromConnectionString[IO](s"mongodb://${address.host}:${address.port}")
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

    "return error if user doesn't exist" in
      withRunningEmbeddedMongo(20717) { address =>
        MongoClient
          .fromConnectionString[IO](s"mongodb://foo:bar@${address.host}:${address.port}")
          .use { client =>
            for {
              db           <- client.getDatabase("db")
              coll         <- db.getCollection("coll")
              insertResult <- coll.insertOne(testDoc)
            } yield insertResult
          }
          .map(_ => fail("should not reach this"))
          .handleError(_ mustBe a[MongoSecurityException])
      }.unsafeToFuture()(IORuntime.global)

    "start embedded mongodb instance with authed user" in
      withRunningEmbeddedMongo(20717, "user", "password") { address =>
        MongoClient
          .fromConnectionString[IO](address.connectionString)
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

    "start embedded mongodb instance with authed user on random port" in
      withRunningEmbeddedMongo("user", "password") { address =>
        MongoClient
          .fromConnectionString[IO](address.connectionString)
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
  }

}
