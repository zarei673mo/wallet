package fr.acinq.eclair.blockchain.electrum

import java.util.concurrent.Executors
import fr.acinq.bitcoin.{Block, ByteVector32}
import fr.acinq.eclair.blockchain.electrum.db.HeaderDb
import fr.acinq.eclair.blockchain.electrum.ElectrumClient.GetHeaders
import fr.acinq.eclair.blockchain.electrum.Blockchain.RETARGETING_PERIOD
import fr.acinq.eclair.blockchain.electrum.ElectrumWallet.{DISCONNECTED, RUNNING, SYNCING, WAITING_FOR_TIP}
import immortan.crypto.{StateMachine, CanBeRepliedTo, KillYourself, GlobalEventStream}
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Success, Try}


object ElectrumChainSync {
  case class ChainSyncing(initialLocalTip: Int, localTip: Int, remoteTip: Int)
  case class ChainSyncEnded(localTip: Int)
}

class ElectrumChainSync(client: CanBeRepliedTo, headerDb: HeaderDb, chainHash: ByteVector32) extends StateMachine[Blockchain] with CanBeRepliedTo { me =>
  implicit val channelContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

  var initialLocalTip: Int = 0
  var reportedTip: Int = 0

  Future {
    state = DISCONNECTED
    data = if (chainHash != Block.RegtestGenesisBlock.hash) {
      // In case if anything at all goes wrong we just use an initial blockchain and resync it from checkpoint
      val blockchain = Blockchain.fromCheckpoints(checkpoints = CheckPoint.load(chainHash, headerDb), chainhash = chainHash)
      val headers = headerDb.getHeaders(startHeight = blockchain.checkpoints.size * RETARGETING_PERIOD, maxCount = Int.MaxValue)
      Try apply Blockchain.addHeadersChunk(blockchain, blockchain.checkpoints.size * RETARGETING_PERIOD, headers) getOrElse blockchain
    } else Blockchain.fromGenesisBlock(Block.RegtestGenesisBlock.hash, Block.RegtestGenesisBlock.header)
    client process ElectrumClient.AddStatusListener(me)
  }

  override def process(reply: Any): Unit =
    Future(me doProcess reply)

  override def doProcess(change: Any): Unit = (change, state) match {
    case (_: ElectrumClient.ElectrumReady, DISCONNECTED) =>
      client process ElectrumClient.HeaderSubscription(me)
      become(data, WAITING_FOR_TIP)

    // WAITING_FOR_TIP

    case (response: ElectrumClient.HeaderSubscriptionResponse, WAITING_FOR_TIP) if response.height < data.height =>
      client process KillYourself
      become(data, DISCONNECTED)

    case (response: ElectrumClient.HeaderSubscriptionResponse, WAITING_FOR_TIP) if data.bestchain.isEmpty =>
      client process ElectrumClient.GetHeaders(data.checkpoints.size * RETARGETING_PERIOD, RETARGETING_PERIOD, me)
      initialLocalTip = data.checkpoints.size * RETARGETING_PERIOD
      reportedTip = response.height
      become(data, SYNCING)

    case (response: ElectrumClient.HeaderSubscriptionResponse, WAITING_FOR_TIP) if response.header == data.tip.header =>
      GlobalEventStream publish ElectrumChainSync.ChainSyncEnded(data.height)
      GlobalEventStream publish data
      become(data, RUNNING)

    case (response: ElectrumClient.HeaderSubscriptionResponse, WAITING_FOR_TIP) =>
      client process ElectrumClient.GetHeaders(data.tip.height + 1, RETARGETING_PERIOD, me)
      initialLocalTip = data.height
      reportedTip = response.height
      become(data, SYNCING)

    // SYNCING

    case (response: ElectrumClient.GetHeadersResponse, SYNCING) if response.headers.isEmpty =>
      GlobalEventStream publish ElectrumChainSync.ChainSyncEnded(data.height)
      GlobalEventStream publish data
      become(data, RUNNING)

    case (ElectrumClient.GetHeadersResponse(start, headers, _), SYNCING) =>
      val blockchain1Try = Try apply Blockchain.addHeaders(data, start, headers)

      blockchain1Try match {
        case Success(blockchain1) =>
          val (chunks, blockchain2) = Blockchain.optimize(blockchain1)
          headerDb.addHeaders(chunks.map(_.header), chunks.head.height)
          GlobalEventStream publish ElectrumChainSync.ChainSyncing(initialLocalTip, data.height, reportedTip)
          client process ElectrumClient.GetHeaders(blockchain2.tip.height + 1, RETARGETING_PERIOD, me)
          become(blockchain2, SYNCING)

        case _ =>
          client process KillYourself
          become(data, DISCONNECTED)
      }

    // RUNNING

    case (ElectrumClient.HeaderSubscriptionResponse(height, header), RUNNING) if data.tip.header != header =>
      val difficultyOk = Blockchain.getDifficulty(data, height, headerDb).forall(header.bits.==)
      val blockchain1Try = Try apply Blockchain.addHeader(data, height, header)

      blockchain1Try match {
        case Success(blockchain1) if difficultyOk =>
          val (chunks, blockchain2) = Blockchain.optimize(blockchain1)
          headerDb.addHeaders(chunks.map(_.header), chunks.head.height)
          GlobalEventStream publish blockchain2
          become(blockchain2, RUNNING)

        case _ =>
          // Peer has sent bad headers
          client process KillYourself
      }

    case (ElectrumClient.GetHeadersResponse(start, headers, _), RUNNING) =>
      val blockchain1Try = Try apply Blockchain.addHeaders(data, start, headers)

      blockchain1Try match {
        case Success(blockchain1) =>
          headerDb.addHeaders(headers, start)
          GlobalEventStream publish blockchain1
          become(blockchain1, RUNNING)

        case _ =>
          // Peer has sent bad headers
          client process KillYourself
      }

    case (ElectrumClient.ElectrumDisconnected, _) => become(data, DISCONNECTED)
    case (ElectrumWallet.ChainFor(target), RUNNING) => target process data
    case (getHeaders: GetHeaders, _) => client process getHeaders
    case _ =>
  }
}
