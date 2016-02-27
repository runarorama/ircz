import scalaz.stream._
import scalaz.stream.Process._
import scalaz.netty._

object Client {
  val mainProcess = for {
    _ <- emit("** Enter your nick: **") to io.stdOutLines
    nick <- io.stdInLines.take(1)
    Exchange(src, snk) <- Netty connect Server.address
    _ <- emit(s"** Connected as $nick **") to io.stdOutLines
    in = src pipe text.utf8Decode to io.stdOutLines
    out = io.stdInLines.map(msg => s"$nick: $msg") pipe text.utf8Encode to snk
    _ <- in merge out
  } yield ()

  val mainTask = mainProcess.run

  def main(args: Array[String]): Unit = mainTask.run
}

