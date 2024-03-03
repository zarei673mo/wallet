package fr.acinq.eclair.blockchain.electrum

import java.io.InputStream
import java.net.InetSocketAddress
import java.util.concurrent.Executors

import fr.acinq.bitcoin.{Block, BlockHeader, ByteVector32}
import fr.acinq.eclair.blockchain.electrum.ElectrumClient.SSL
import fr.acinq.eclair.blockchain.electrum.ElectrumClientPool._
import fr.acinq.eclair.blockchain.electrum.ElectrumWallet.{DISCONNECTED, RUNNING}
import immortan.crypto.{CanBeRepliedTo, GlobalEventStream, StateMachine}
import org.json4s.JsonAST.{JObject, JString}
import org.json4s.native.JsonMethods

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.Random


class ElectrumClientPool(chainHash: ByteVector32) extends StateMachine[Data] with CanBeRepliedTo { me =>
  implicit val channelContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

  val addresses = collection.mutable.Map.empty[CanBeRepliedTo, InetSocketAddress]
  val statusListeners = collection.mutable.HashSet.empty[CanBeRepliedTo]
  val serverAddresses = loadFromChainHash(chainHash)

  data = DisconnectedData
  state = DISCONNECTED

  override def process(reply: Any): Unit =
    Future(me doProcess reply)

  override def doProcess(change: Any): Unit = (change, state) match {
    case (ElectrumClient.ElectrumReady(height, tip, _, sender), DISCONNECTED) if addresses.contains(sender) =>
      sender process ElectrumClient.HeaderSubscription(me)
      handleHeader(sender, height, tip, None)

    case (ElectrumClient.AddStatusListener(listener), DISCONNECTED) =>
      statusListeners += listener

    case (Terminated(actor), DISCONNECTED) =>
      context.system.scheduler.scheduleOnce(5.seconds, self, Connect)
      addresses -= actor
  }

  when(Connected) {
    case Event(ElectrumClient.ElectrumReady(height, tip, _), d: ConnectedData) if addresses.contains(sender) =>
      sender ! ElectrumClient.HeaderSubscription(self)
      handleHeader(sender, height, tip, Some(d))

    case Event(ElectrumClient.HeaderSubscriptionResponse(height, tip), d: ConnectedData) if addresses.contains(sender) =>
      handleHeader(sender, height, tip, Some(d))

    case Event(request: ElectrumClient.Request, d: ConnectedData) =>
      d.master forward request
      stay

    case Event(ElectrumClient.AddStatusListener(listener), d: ConnectedData) if addresses.contains(d.master) =>
      statusListeners += listener
      val (height, tip) = d.tips(d.master)
      listener ! ElectrumClient.ElectrumReady(height, tip, addresses(d.master))
      stay

    case Event(Terminated(actor), d: ConnectedData) =>
      val address = addresses(actor)
      val tips1 = d.tips - actor

      context.system.scheduler.scheduleOnce(5.seconds, self, Connect)
      addresses -= actor

      if (tips1.isEmpty) {
        log.info("lost connection to {}, no active connections left", address)
        goto(Disconnected) using DisconnectedData // no more connections
      } else if (d.master != actor) {
        log.debug("lost connection to {}, we still have our master server", address)
        stay using d.copy(tips = tips1) // we don't care, this wasn't our master
      } else {
        log.info("lost connection to our master server {}", address)
        // we choose next best candidate as master
        val tips1 = d.tips - actor
        val (bestClient, bestTip) = tips1.toSeq.maxBy(_._2._1)
        handleHeader(bestClient, bestTip._1, bestTip._2, Some(d.copy(tips = tips1)))
      }
  }

  whenUnhandled {
    case Event(InitConnect, _) =>
      val connections = Math.min(3, serverAddresses.size)
      (0 until connections).foreach(_ => self ! Connect)
      stay

    case Event(Connect, _) =>
      pickAddress(serverAddresses, addresses.values.toSet) foreach { esa =>
        val resolved = new InetSocketAddress(esa.address.getHostName, esa.address.getPort)
        val client = context actorOf Props(classOf[ElectrumClient], resolved, esa.ssl, ec)
        client ! ElectrumClient.AddStatusListener(self)
        addresses += Tuple2(client, esa.address)
        context watch client
      }

      stay

    case Event(ElectrumClient.ElectrumDisconnected, _) =>
      // Ignored, we rely on Terminated messages to detect disconnections
      stay
  }

  onTransition {
    case Connected -> Disconnected =>
      statusListeners.foreach(_ ! ElectrumClient.ElectrumDisconnected)
      context.system.eventStream.publish(ElectrumClient.ElectrumDisconnected)
  }

  private def handleHeader(connection: CanBeRepliedTo, height: Int, tip: BlockHeader, cd: Option[ConnectedData] = None) = {

    val connectionParams = (height, tip)
    val tipMap = Map(connection -> connectionParams)
    val remoteAddress = addresses(connection)

    cd match {
      case None =>
        for (lst <- statusListeners) lst process ElectrumClient.ElectrumReady(height, tip, remoteAddress, me)
        GlobalEventStream publish ElectrumClient.ElectrumReady(height, tip, remoteAddress, me)
        become(ConnectedData(connection, tipMap), RUNNING)

      case Some(data1) if connection != data1.master && height > data1.blockHeight + 2 =>
        for (lst <- statusListeners) lst process ElectrumClient.ElectrumDisconnected
        GlobalEventStream publish ElectrumClient.ElectrumDisconnected

        for (lst <- statusListeners) lst process ElectrumClient.ElectrumReady(height, tip, remoteAddress, me)
        GlobalEventStream publish ElectrumClient.ElectrumReady(height, tip, remoteAddress, me)
        become(data1.copy(master = connection, tips = data1.tips ++ tipMap), RUNNING)

      case Some(data1) =>
        become(data1.copy(tips = data1.tips ++ tipMap), state)
    }
  }
}

object ElectrumClientPool {
  case class ElectrumServerAddress(address: InetSocketAddress, ssl: SSL)

  var loadFromChainHash: ByteVector32 => Set[ElectrumServerAddress] = {
    case Block.LivenetGenesisBlock.hash => readServerAddresses(classOf[ElectrumServerAddress] getResourceAsStream "/electrum/servers_mainnet.json")
    case Block.TestnetGenesisBlock.hash => readServerAddresses(classOf[ElectrumServerAddress] getResourceAsStream "/electrum/servers_testnet.json")
    case _ => throw new RuntimeException
  }

  def readServerAddresses(stream: InputStream): Set[ElectrumServerAddress] = try {
    val JObject(values) = JsonMethods.parse(stream)

    for (Tuple2(name, fields) <- values.toSet) yield {
      val port = (fields \ "s").asInstanceOf[JString].s.toInt
      val address = InetSocketAddress.createUnresolved(name, port)
      ElectrumServerAddress(address, SSL.LOOSE)
    }
  } finally {
    stream.close
  }

  def pickAddress(serverAddresses: Set[ElectrumServerAddress], usedAddresses: Set[InetSocketAddress] = Set.empty): Option[ElectrumServerAddress] =
    Random.shuffle(serverAddresses.filterNot(serverAddress => usedAddresses contains serverAddress.address).toSeq).headOption

  sealed trait Data
  case object DisconnectedData extends Data

  type TipAndHeader = (Int, BlockHeader)
  type ActorTipAndHeader = Map[CanBeRepliedTo, TipAndHeader]
  case class ConnectedData(master: CanBeRepliedTo, tips: ActorTipAndHeader) extends Data {
    def blockHeight: Int = tips.get(master).map(_._1).getOrElse(0)
  }

  case object Connect
  case object InitConnect
}
