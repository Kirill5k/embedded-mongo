package mongodb.embedded

import cats.effect.{Async, Resource}
import cats.syntax.apply._
import de.flapdoodle.embed.mongo.commands.MongodArguments
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import de.flapdoodle.reverse.{Listener, TransitionWalker}
import mongo4cats.models.client.{MongoConnection, MongoCredential}

trait EmbeddedMongo {
  protected val mongoPort: Int                = 27017
  protected val mongoUsername: Option[String] = None
  protected val mongoPassword: Option[String] = None

  def withRunningEmbeddedMongo[F[_]: Async, A](test: MongoConnection => F[A]): F[A] =
    runMongo(mongoPort, mongoUsername, mongoPassword)(test)

  def withRunningEmbeddedMongo[F[_] : Async, A](mongoPort: Int)(test: MongoConnection => F[A]): F[A] =
    runMongo(mongoPort, mongoUsername, mongoPassword)(test)

  private def runMongo[F[_]: Async, A](port: Int, username: Option[String], password: Option[String])(test: MongoConnection => F[A]): F[A] =
    EmbeddedMongo
      .start[F](port, username, password)
      .use(test(_))
}

object EmbeddedMongo {

  def start[F[_]: Async](
      port: Int,
      username: Option[String],
      password: Option[String]
  ): Resource[F, MongoConnection] = {
    val withAuth = username.isDefined && password.isDefined
    Resource
      .fromAutoCloseable(Async[F].delay(startMongod(port, withAuth)))
      .map(getMongoConnection(username, password))
  }

  private def startMongod(mongoPort: Int, withAuth: Boolean, listeners: Listener*): TransitionWalker.ReachedState[RunningMongodProcess] =
    Mongod
      .builder()
      .net(Start.to(classOf[Net]).initializedWith(Net.defaults().withPort(mongoPort)))
      .mongodArguments(Start.to(classOf[MongodArguments]).initializedWith(MongodArguments.defaults().withAuth(withAuth)))
      .build()
      .start(Version.Main.V5_0, listeners: _*)

  private def getMongoConnection(
      username: Option[String],
      password: Option[String]
  )(
      runningProcess: TransitionWalker.ReachedState[RunningMongodProcess]
  ): MongoConnection = {
    val address    = runningProcess.current().getServerAddress
    val credential = (username, password).mapN((u, p) => MongoCredential(u, p))
    MongoConnection.classic(address.getHost, address.getPort, credential)
  }
}
