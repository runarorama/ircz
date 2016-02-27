import scalaz.netty._
import scalaz.concurrent.{Task, Strategy}
import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ThreadFactory}
import scodec.bits.ByteVector
import scalaz.stream._
import scalaz.stream.async.mutable.Signal
import scalaz.stream.Process._

object IO {
  def apply[A](a: => A) = Process.eval(Task.delay(a))
}

object Server {
  def address = new InetSocketAddress("localhost", 9090)

  def daemonThreads(name: String) = new ThreadFactory {
    def newThread(r: Runnable) = {
      val t = Executors.defaultThreadFactory.newThread(r)
      t.setDaemon(true)
      t.setName(name)
      t
    }
  }

  val pool = Executors.newCachedThreadPool(daemonThreads("chat-server"))
  val S = Strategy.Executor(pool)

  type Listener = Sink[Task,ByteVector]

  val clients = async.signalOf[Set[Listener]](Set.empty)(S)

  def addClient(c: Listener) =
    clients.compareAndSet { cs =>
      Some(cs.toSet.flatten + c)
    }

  def removeClient(c: Listener) =
    clients.compareAndSet { cs =>
      Some(cs.toSet.flatten - c)
    }

  // A connection emits an exchange and shuts down.
  // We flatMap (or mergeN) over this to make it do something before it shuts down.
  type Connection = Process[Task, Exchange[ByteVector, ByteVector]]

  def connections: Process[Task, Connection] = Netty server address

  val messageQueue = async.boundedQueue[ByteVector](4096)(S)

  // The process that serves client connections.
  // Establishes client connections, registers them as listeners,
  // and relays all their messages.
  def serve = merge.mergeN(connections map { client =>
    for {
      Exchange(src, snk) <- client
      _ <- eval(addClient(snk))
      _ <- src.attempt().stripW to messageQueue.enqueue
    } yield ()
  })(S)

  // The process that relays messages to clients.
  def relay = for {
    message <- messageQueue.dequeue
    cs <- eval(clients.get)
    client <- emitAll(cs.toSeq)
    _ <- emit(message) to client onFailure { _ =>
      eval(removeClient(client))
    }
  } yield ()

  // The main server process
  val mainProcess = serve merge relay

  val mainTask = mainProcess.run

  def main(args: Array[String]): Unit = mainTask.run
}

