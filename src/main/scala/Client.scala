import scalaz.stream._
import scalaz.stream.Process._
import scalaz.netty._

object Client {

  val mainProcess = for {
    // Get the user's nick
    _ <- emit("** Enter your nick: **") to io.stdOutLines
    nick <- io.stdInLines.take(1)

    // Connect to the server
    c <- Netty connect Server.address
    Exchange(src, snk) = c
    _ <- emit(s"** Connected as $nick **") to io.stdOutLines

    // Reads UTF8 bytes from the connection, decodes them, and sends to stdout
    in = src pipe text.utf8Decode to io.stdOutLines

    // Reads from stdin, prepends the nick, encodes as UTF8, and sends to the server
    out = io.stdInLines.map(msg => s"$nick: $msg") pipe text.utf8Encode to snk

    // The main process nondeterministically merges the in and out processes
    _ <- (in wye out)(wye.mergeHaltBoth)
  } yield ()

  val mainTask = mainProcess.run

  def main(args: Array[String]): Unit = mainTask.run
}

