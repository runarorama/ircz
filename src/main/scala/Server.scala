import scalaz.netty._
import scalaz.concurrent.{Task, Strategy}
import java.net.InetSocketAddress
import java.util.concurrent.{Executors, ThreadFactory}
import scodec.bits.ByteVector
import scalaz.stream._
import scalaz.stream.async.mutable.Signal
import scalaz.stream.Process._

object Server {
  val maxThreads = 16

  def address = new InetSocketAddress("localhost", 9090)

  val pool = Executors.newFixedThreadPool(maxThreads)
  implicit val S = Strategy.Executor(pool)

  type Listener = Sink[Task,ByteVector]

  // The mutable server state:
  // Contains the set of sinks to send messages to.
  val clients = async.signalOf[Set[Listener]](Set.empty)

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
  val messageQueue = async.boundedQueue[ByteVector](8192)

  // The process that serves clients.
  // Establishes client connections, registers them as listeners,
  // and enqueues all their messages on the relay,
  // stripping any client errors.
  def serve = merge.mergeN(maxThreads)(connections map { client =>
    for {
      c <- client
      Exchange(src, snk) = c
      _ <- eval(addClient(snk))
      _ <- src.attempt().stripW to messageQueue.enqueue
    } yield ()
  })

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
  val mainProcess = (serve wye relay)(wye.mergeHaltBoth)

  val mainTask = mainProcess.run

  def main(args: Array[String]): Unit = mainTask.run
}

