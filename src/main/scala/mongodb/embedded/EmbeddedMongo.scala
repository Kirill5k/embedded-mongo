package mongodb.embedded

import cats.effect.{Async, Resource}
import cats.syntax.apply._
import cats.syntax.flatMap._
import cats.syntax.functor._
import de.flapdoodle.embed.mongo.commands.MongodArguments
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import de.flapdoodle.reverse.{Listener, TransitionWalker}
import mongo4cats.bson.Document
import mongo4cats.bson.syntax._
import mongo4cats.client.MongoClient

final case class EmbeddedMongoInstanceAddress(
    host: String,
    port: Int,
    username: Option[String],
    password: Option[String]
) {
  def connectionString: String = {
    val creds = (username, password).mapN((u, p) => s"$u:$p@").getOrElse("")
    s"mongodb://$creds$host:$port"
  }
}

trait EmbeddedMongo {
  protected val mongoPort: Int                = 27017
  protected val mongoUsername: Option[String] = None
  protected val mongoPassword: Option[String] = None

  def withRunningEmbeddedMongo[F[_]: Async, A](test: EmbeddedMongoInstanceAddress => F[A]): F[A] =
    runMongo(mongoPort, mongoUsername, mongoPassword)(test)

  def withRunningEmbeddedMongo[F[_]: Async, A](
      mongoPort: Int
  )(
      test: EmbeddedMongoInstanceAddress => F[A]
  ): F[A] =
    runMongo(mongoPort, mongoUsername, mongoPassword)(test)

  def withRunningEmbeddedMongo[F[_]: Async, A](
      mongoPort: Int,
      mongoUsername: String,
      mongoPassword: String
  )(
      test: EmbeddedMongoInstanceAddress => F[A]
  ): F[A] =
    runMongo(mongoPort, Some(mongoUsername), Some(mongoPassword))(test)

  private def runMongo[F[_]: Async, A](
      port: Int,
      username: Option[String],
      password: Option[String]
  )(
      test: EmbeddedMongoInstanceAddress => F[A]
  ): F[A] =
    EmbeddedMongo
      .start[F](port, username, password)
      .use(test(_))
}

object EmbeddedMongo {

  def start[F[_]](
      port: Int,
      username: Option[String],
      password: Option[String]
  )(implicit F: Async[F]): Resource[F, EmbeddedMongoInstanceAddress] = {
    val withAuth = username.isDefined && password.isDefined
    Resource
      .fromAutoCloseable(F.delay(startMongod(port, withAuth)))
      .evalTap(runningProcess => F.whenA(withAuth)(insertUser(runningProcess, username.get, password.get)))
      .map(getAddress(username, password))
  }

  private def startMongod(mongoPort: Int, withAuth: Boolean, listeners: Listener*): TransitionWalker.ReachedState[RunningMongodProcess] =
    Mongod
      .builder()
      .net(Start.to(classOf[Net]).initializedWith(Net.defaults().withPort(mongoPort)))
      .mongodArguments(Start.to(classOf[MongodArguments]).initializedWith(MongodArguments.defaults().withAuth(withAuth)))
      .build()
      .start(Version.Main.V5_0, listeners: _*)

  private def getAddress(
      username: Option[String],
      password: Option[String]
  )(
      runningProcess: TransitionWalker.ReachedState[RunningMongodProcess]
  ): EmbeddedMongoInstanceAddress = {
    val address = runningProcess.current().getServerAddress
    EmbeddedMongoInstanceAddress(address.getHost, address.getPort, username, password)
  }

  private def insertUser[F[_]](
      runningProcess: TransitionWalker.ReachedState[RunningMongodProcess],
      username: String,
      password: String
  )(implicit F: Async[F]): F[Unit] = {
    val createUser = Document(
      "createUser" := username,
      "pwd"        := password,
      "roles"      := List("userAdminAnyDatabase", "dbAdminAnyDatabase", "readWriteAnyDatabase")
    )
    MongoClient
      .fromConnectionString(getAddress(None, None)(runningProcess).connectionString)
      .use { client =>
        for {
          db <- client.getDatabase("admin")
          _  <- db.runCommand(createUser.toBsonDocument())
        } yield ()
      }
  }
}
