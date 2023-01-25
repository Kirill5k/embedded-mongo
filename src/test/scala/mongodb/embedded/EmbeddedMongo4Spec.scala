package mongodb.embedded

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import com.mongodb.client.result.InsertOneResult
import com.mongodb.{MongoCommandException, MongoSecurityException}
import mongo4cats.bson.syntax._
import mongo4cats.bson.{Document, ObjectId}
import mongo4cats.client.MongoClient
import org.scalatest.matchers.must.Matchers
import org.scalatest.wordspec.AsyncWordSpec

class EmbeddedMongo4Spec extends AsyncWordSpec with Matchers with EmbeddedMongo {

  val testDoc = Document("_id" := ObjectId(), "stringField" := "string", "intField" := 1)

  "An EmbeddedMongo4" should {

    (1 to 10).foreach { i =>
      s"start embedded mongodb instance $i" in
        withRunningEmbeddedMongo {
          MongoClient
            .fromConnectionString[IO](s"mongodb://localhost:$mongoPort")
            .use(insertAndRetrieveTestDoc)
            .map { case (insertRes, foundDoc) =>
              foundDoc mustBe Some(testDoc)
              insertRes.wasAcknowledged() mustBe true
            }
        }.unsafeToFuture()(IORuntime.global)

      s"start embedded mongodb instance on specified port $i" in
        withRunningEmbeddedMongo(20719) {
          MongoClient
            .fromConnectionString[IO]("mongodb://localhost:20719")
            .use(insertAndRetrieveTestDoc)
            .map { case (insertRes, foundDoc) =>
              foundDoc mustBe Some(testDoc)
              insertRes.wasAcknowledged() mustBe true
            }
        }.unsafeToFuture()(IORuntime.global)

      s"not allow to connect to embedded instance without password $i" in
        withRunningEmbeddedMongo(20720, "user", "password") {
          MongoClient
            .fromConnectionString[IO]("mongodb://localhost:20720")
            .use(insertAndRetrieveTestDoc)
            .map(_ => fail("should not reach this"))
            .handleError(_ mustBe a[MongoCommandException])
        }.unsafeToFuture()(IORuntime.global)

      s"return error if user doesn't exist $i" in
        withRunningEmbeddedMongo(20721) {
          MongoClient
            .fromConnectionString[IO]("mongodb://foo:bar@localhost:20721")
            .use(insertAndRetrieveTestDoc)
            .map(_ => fail("should not reach this"))
            .handleError(_ mustBe a[MongoSecurityException])
        }.unsafeToFuture()(IORuntime.global)

      s"start embedded mongodb instance with authed user $i" in
        withRunningEmbeddedMongo(20722, "user", "password") {
          MongoClient
            .fromConnectionString[IO]("mongodb://user:password@localhost:20722")
            .use(insertAndRetrieveTestDoc)
            .map { case (insertRes, foundDoc) =>
              foundDoc mustBe Some(testDoc)
              insertRes.wasAcknowledged() mustBe true
            }
        }.unsafeToFuture()(IORuntime.global)

      s"start embedded mongodb instance with authed user on default port $i" in
        withRunningEmbeddedMongo("user", "password") {
          MongoClient
            .fromConnectionString[IO](s"mongodb://user:password@localhost:$mongoPort")
            .use(insertAndRetrieveTestDoc)
            .map { case (insertRes, foundDoc) =>
              foundDoc mustBe Some(testDoc)
              insertRes.wasAcknowledged() mustBe true
            }
        }.unsafeToFuture()(IORuntime.global)
    }
  }

  private def insertAndRetrieveTestDoc(client: MongoClient[IO]): IO[(InsertOneResult, Option[Document])] =
    for {
      db <- client.getDatabase("db")
      coll <- db.getCollection("coll")
      insertResult <- coll.insertOne(testDoc)
      foundDoc <- coll.find.first
    } yield (insertResult, foundDoc)
}
