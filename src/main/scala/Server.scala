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

  // The mutable server state:
  // Contains the set of sinks to send messages to.
  val clients = async.signalOf[Set[Listener]](Set.empty)(S)

  // Add a client to the signal
  def addClient(c: Listener) =
    clients.compareAndSet { cs =>
      Some(cs.toSet.flatten + c)
    }

  // Remove a client from the signal
  def removeClient(c: Listener) =
    clients.compareAndSet { cs =>
      Some(cs.toSet.flatten - c)
    }

  // A connection emits an exchange and then shuts down.
  // We flatMap (or mergeN) this to make it do something before it shuts down.
  type Connection = Process[Task, Exchange[ByteVector, ByteVector]]

  // The netty server as a stream of incoming connections
  def connections: Process[Task, Connection] = Netty server address

  // The queue of messages from clients
  val messageQueue = async.boundedQueue[ByteVector](8192)(S)

  // The process that serves clients.
  // Establishes client connections, registers them as listeners,
  // and enqueues all their messages on the relay,
  // stripping any client errors.
  def serve = merge.mergeN(connections map { client =>
    for {
      Exchange(src, snk) <- client
      _ <- eval(addClient(snk))
      _ <- src.attempt().stripW to messageQueue.enqueue
    } yield ()
  })(S)

  // The process that relays messages to clients.
  // For each message on the queue, get all clients.
  // For each client, send the message to the client.
  // If that fails, remove the client.
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

