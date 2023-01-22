package mongodb.embedded

import cats.effect.{Async, Resource}
import de.flapdoodle.embed.mongo.commands.MongodArguments
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import de.flapdoodle.reverse.{Listener, TransitionWalker}
import mongo4cats.models.client.ServerAddress

trait EmbeddedMongo {
  protected val mongoPort: Int                = 27017
  protected val mongoUsername: Option[String] = None
  protected val mongoPassword: Option[String] = None

  def withRunningEmbeddedMongo[F[_] : Async, A](test: ServerAddress => F[A]): F[A] =
    runMongo(mongoPort, mongoUsername, mongoPassword)(test)

  private def runMongo[F[_] : Async, A](port: Int, username: Option[String], password: Option[String])(test: ServerAddress => F[A]): F[A] =
    EmbeddedMongo
      .start[F](port, username, password)
      .use(test(_))
}

object EmbeddedMongo {

  def start[F[_]: Async](
      port: Int,
      username: Option[String],
      password: Option[String]
  ): Resource[F, ServerAddress] = {
    val withAuth = username.isDefined && password.isDefined
    Resource
      .fromAutoCloseable(Async[F].delay(startMongod(port, withAuth)))
      .map(getServerAddress)
  }



  private def startMongod(mongoPort: Int, withAuth: Boolean, listeners: Listener*): TransitionWalker.ReachedState[RunningMongodProcess] =
    Mongod
      .builder()
      .net(Start.to(classOf[Net]).initializedWith(Net.defaults().withPort(mongoPort)))
      .mongodArguments(Start.to(classOf[MongodArguments]).initializedWith(MongodArguments.defaults().withAuth(withAuth)))
      .build()
      .start(Version.Main.V5_0, listeners: _*)

  private def getServerAddress(runningProcess: TransitionWalker.ReachedState[RunningMongodProcess]): ServerAddress = {
    val address = runningProcess.current().getServerAddress
    ServerAddress(address.getHost, address.getPort)
  }
}
