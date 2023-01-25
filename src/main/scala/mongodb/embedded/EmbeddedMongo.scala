package mongodb.embedded

import cats.effect.{Async, Resource}
import cats.syntax.functor._
import com.mongodb.client.MongoClients
import de.flapdoodle.embed.mongo.commands.MongodArguments
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import de.flapdoodle.reverse.{Listener, StateID, TransitionWalker}
import org.bson.Document

trait EmbeddedMongo {
  protected val mongoPort: Int = 27017
  protected val mongoUsername: Option[String] = None
  protected val mongoPassword: Option[String] = None

  def withRunningEmbeddedMongo[F[_]: Async, A](test: => F[A]): F[A] =
    EmbeddedMongo.start[F](mongoPort, mongoUsername, mongoPassword).use(_ => test)

  def withRunningEmbeddedMongo[F[_]: Async, A](
      mongoUsername: String,
      mongoPassword: String
  )(
      test:  => F[A]
  ): F[A] =
    EmbeddedMongo.start[F](mongoPort, Some(mongoUsername), Some(mongoPassword)).use(_ => test)

  def withRunningEmbeddedMongo[F[_]: Async, A](
      mongoPort: Int
  )(
      test: => F[A]
  ): F[A] =
    EmbeddedMongo.start[F](mongoPort, mongoUsername, mongoPassword).use(_ => test)

  def withRunningEmbeddedMongo[F[_]: Async, A](
      mongoPort: Int,
      mongoUsername: String,
      mongoPassword: String
  )(
      test: => F[A]
  ): F[A] =
    EmbeddedMongo.start[F](mongoPort, Some(mongoUsername), Some(mongoPassword)).use(_ => test)
}

object EmbeddedMongo {

  def start[F[_]](
      port: Int,
      username: Option[String],
      password: Option[String],
      remainingAttempts: Int = 10
  )(implicit F: Async[F]): Resource[F, Unit] =
    Resource
      .fromAutoCloseable(F.delay(startMongod(port, username, password)))
      .void
      .handleErrorWith[Unit, Throwable] { error =>
        if (remainingAttempts < 0) Resource.raiseError(error)
        else start[F](port, username, password, remainingAttempts - 1)
      }

  private def startMongod(
      port: Int,
      username: Option[String],
      password: Option[String]
  ): TransitionWalker.ReachedState[RunningMongodProcess] = {
    val withAuth = username.isDefined && password.isDefined
    val listener = if (withAuth) Some(insertUserListener(username.get, password.get)) else None
    Mongod.builder()
      .net(Start.to(classOf[Net]).initializedWith(Net.defaults().withPort(port)))
      .mongodArguments(Start.to(classOf[MongodArguments]).initializedWith(MongodArguments.defaults().withAuth(withAuth)))
      .build()
      .start(Version.Main.V5_0, listener.toList: _*)
  }

  private def insertUserListener(username: String, password: String): Listener =
    Listener
      .typedBuilder()
      .onStateReached[RunningMongodProcess](
        StateID.of(classOf[RunningMongodProcess]),
        { runningProcess =>
          val createUser = new Document("createUser", username)
            .append("pwd", password)
            .append("roles", java.util.Arrays.asList("userAdminAnyDatabase", "dbAdminAnyDatabase", "readWriteAnyDatabase"))

          val address = runningProcess.getServerAddress
          val client  = MongoClients.create(s"mongodb://${address.getHost}:${address.getPort}")
          try {
            val db = client.getDatabase("admin")
            db.runCommand(createUser)
            ()
          } finally client.close()
        }
      )
      .build()
}
