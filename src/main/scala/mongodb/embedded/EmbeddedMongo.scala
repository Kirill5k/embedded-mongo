package mongodb.embedded

import cats.effect.{Async, Resource}
import cats.syntax.apply._
import com.mongodb.client.MongoClients
import de.flapdoodle.embed.mongo.commands.MongodArguments
import de.flapdoodle.embed.mongo.config.Net
import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.transitions.{Mongod, RunningMongodProcess}
import de.flapdoodle.reverse.transitions.Start
import de.flapdoodle.reverse.{Listener, StateID, TransitionWalker}
import org.bson.Document

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
    val listener = if (withAuth) Some(insertUserListener(username.get, password.get)) else None
    Resource
      .fromAutoCloseable(F.delay(startMongod(port, withAuth, listener)))
      .map(getAddress(username, password))
  }

  private def startMongod(
      mongoPort: Int,
      withAuth: Boolean,
      listener: Option[Listener]
  ): TransitionWalker.ReachedState[RunningMongodProcess] =
    Mongod
      .builder()
      .net(Start.to(classOf[Net]).initializedWith(Net.defaults().withPort(mongoPort)))
      .mongodArguments(Start.to(classOf[MongodArguments]).initializedWith(MongodArguments.defaults().withAuth(withAuth)))
      .build()
      .start(Version.Main.V5_0, listener.toList: _*)

  private def getAddress(
      username: Option[String],
      password: Option[String]
  )(
      runningProcess: TransitionWalker.ReachedState[RunningMongodProcess]
  ): EmbeddedMongoInstanceAddress = {
    val address = runningProcess.current().getServerAddress
    EmbeddedMongoInstanceAddress(address.getHost, address.getPort, username, password)
  }

  private def insertUserListener(username: String, password: String): Listener = {
    val expectedState = StateID.of(classOf[RunningMongodProcess])
    Listener
      .typedBuilder()
      .onStateReached[RunningMongodProcess](
        expectedState,
        { runningProcess =>
          val createUser = new Document("createUser", username)
            .append("pwd", password)
            .append("roles", java.util.Arrays.asList("userAdminAnyDatabase", "dbAdminAnyDatabase", "readWriteAnyDatabase"))

          val address = runningProcess.getServerAddress
          val client  = MongoClients.create(EmbeddedMongoInstanceAddress(address.getHost, address.getPort, None, None).connectionString)
          try {
            val db = client.getDatabase("admin")
            db.runCommand(createUser)
            ()
          } finally client.close()
        }
      )
      .build()
  }
}
