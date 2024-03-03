package fr.acinq.eclair.blockchain.electrum

import java.net.{InetSocketAddress, SocketAddress}
import java.util
import java.util.concurrent.Executors

import fr.acinq.bitcoin._
import fr.acinq.eclair.blockchain.bitcoind.rpc.{Error, JsonRPCRequest, JsonRPCResponse}
import fr.acinq.eclair.blockchain.electrum.ElectrumClient._
import fr.acinq.eclair.blockchain.fee.FeeratePerKw
import immortan.crypto.Tools._
import immortan.crypto.{CanBeRepliedTo, KillYourself, StateMachine}
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel._
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.string.{LineEncoder, StringDecoder}
import io.netty.handler.codec.{LineBasedFrameDecoder, MessageToMessageDecoder, MessageToMessageEncoder}
import io.netty.handler.proxy.Socks5ProxyHandler
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.resolver.NoopAddressResolverGroup
import io.netty.util.CharsetUtil
import org.json4s.JsonAST._
import org.json4s.native.JsonMethods
import org.json4s.{JInt, JLong, JString}
import rx.lang.scala.Observable
import scodec.bits.ByteVector

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Success, Try}


class ElectrumClient(serverAddress: InetSocketAddress, sslMode: SSL) extends StateMachine[ElectrumClientData] with CanBeRepliedTo { me =>
  implicit val channelContext: ExecutionContextExecutor = scala.concurrent.ExecutionContext.fromExecutor(Executors.newSingleThreadExecutor)

  data = ElectrumClientDisconnected
  state = WAITING_VERSION

  val bootStrap = new Bootstrap
  bootStrap channel classOf[NioSocketChannel]
  bootStrap group workerGroup

  bootStrap.option[java.lang.Boolean](ChannelOption.TCP_NODELAY, true)
  bootStrap.option[java.lang.Boolean](ChannelOption.SO_KEEPALIVE, true)
  bootStrap.option[java.lang.Integer](ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
  bootStrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

  bootStrap handler new ChannelInitializer[SocketChannel] {
    override def initChannel(sockChan: SocketChannel): Unit = {
      if (sslMode == SSL.LOOSE || serverAddress.getPort == 50002) {
        val sslCtx = SslContextBuilder.forClient.trustManager(InsecureTrustManagerFactory.INSTANCE).build
        sockChan.pipeline addLast sslCtx.newHandler(sockChan.alloc, serverAddress.getHostName, serverAddress.getPort)
      }

      // Inbound
      sockChan.pipeline addLast new LineBasedFrameDecoder(Int.MaxValue, true, true)
      sockChan.pipeline addLast new StringDecoder(CharsetUtil.UTF_8)
      sockChan.pipeline addLast new ElectrumResponseDecoder
      sockChan.pipeline addLast new ActorHandler

      // Outbound
      sockChan.pipeline.addLast(new LineEncoder)
      sockChan.pipeline.addLast(new JsonRPCRequestEncoder)

      // Error handler
      sockChan.pipeline.addLast(new ExceptionHandler)

      ElectrumWallet.connectionProvider.proxyAddress.foreach { address =>
        // Optional proxy which must be the first handler
        val handler = new Socks5ProxyHandler(address)
        sockChan.pipeline.addFirst(handler)
      }
    }
  }

  val channelOpenFuture: ChannelFuture = bootStrap.connect(serverAddress.getHostName, serverAddress.getPort)
  if (ElectrumWallet.connectionProvider.proxyAddress.isDefined) bootStrap.resolver(NoopAddressResolverGroup.INSTANCE)

  channelOpenFuture addListeners new ChannelFutureListener {
    private val listener = new ChannelFutureListener { override def operationComplete(fut: ChannelFuture): Unit = me process KillYourself }
    override def operationComplete(future: ChannelFuture): Unit = if (future.isSuccess) future.channel.closeFuture addListener listener else me process KillYourself
  }

  class ExceptionHandler extends ChannelDuplexHandler {
    private val listener = new ChannelFutureListener { override def operationComplete(fut: ChannelFuture): Unit = if (!fut.isSuccess) me process KillYourself }

    override def connect(ctx: ChannelHandlerContext, remoteAddress: SocketAddress, localAddress: SocketAddress, promise: ChannelPromise): Unit =
      ctx.connect(remoteAddress, localAddress, promise addListener listener)

    override def write(ctx: ChannelHandlerContext, msg: scala.Any, promise: ChannelPromise): Unit =
      ctx.write(msg, promise addListener listener)

    override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit =
      me process KillYourself
  }

  class ElectrumResponseDecoder extends MessageToMessageDecoder[String] {
    override def decode(ctx: ChannelHandlerContext, msg: String, out: AnyRefList): Unit = {
      val string = msg.asInstanceOf[String]
      val res = parseResponse(string)
      out.add(res)
    }
  }

  class JsonRPCRequestEncoder extends MessageToMessageEncoder[JsonRPCRequest] {
    override def encode(ctx: ChannelHandlerContext, request: JsonRPCRequest, out: AnyRefList): Unit = {
      import org.json4s.JsonDSL._
      import org.json4s._

      val params = request.params.map {
        case b: ByteVector32 => new JString(b.toHex)
        case f: FeeratePerKw => new JLong(f.toLong)
        case t: Double => new JDouble(t)
        case s: String => new JString(s)
        case b: Boolean => new JBool(b)
        case t: Long => new JLong(t)
        case t: Int => new JInt(t)
      }

      val json = ("method" -> request.method) ~ ("params" -> params) ~ ("id" -> request.id) ~ ("jsonrpc" -> request.jsonrpc)
      val serialized = JsonMethods.compact(JsonMethods render json)
      out.add(serialized)
    }
  }

  class ActorHandler extends ChannelInboundHandlerAdapter {
    override def channelActive(ctx: ChannelHandlerContext): Unit = me process ctx
    override def channelRead(ctx: ChannelHandlerContext, msg: Any): Unit = me process msg
  }

  var reqId = 0
  var ctx: ChannelHandlerContext = _
  var scriptHashSubscriptions = Map.empty[ByteVector32, ActorSet].withDefaultValue(Set.empty)
  val headerSubscriptions = collection.mutable.HashSet.empty[CanBeRepliedTo]
  val statusListeners = collection.mutable.HashSet.empty[CanBeRepliedTo]
  val version = ServerVersion(CLIENT_NAME, PROTOCOL_VERSION, me)

  val pingTrigger = Observable.interval(30.seconds).subscribe { _ =>
    // we need to send periodic ping to not get dosconnected
    me send Ping(me)
  }

  override def process(reply: Any): Unit =
    Future(me doProcess reply)

  override def doProcess(change: Any): Unit = (data, change, state) match {
    case (_, channelHandlerContext: ChannelHandlerContext, WAITING_VERSION) =>
      ctx = channelHandlerContext
      me send version

    case (_, Right(json: JsonRPCResponse), WAITING_VERSION) =>
      (parseJsonResponse(version, json): @unchecked) match {
        case _: ServerVersionResponse =>
          me send HeaderSubscription(me)
          headerSubscriptions += me
          become(null, WAITING_TIP)

        case _: ServerError =>
          me process KillYourself
      }

    case (_, Right(json: JsonRPCResponse), WAITING_TIP) =>
      val (height, blockHeader) = parseBlockHeader(json.result)
      val event = ElectrumClient.ElectrumReady(height, blockHeader, serverAddress, me)
      become(ElectrumClientConnected(height, blockHeader, Map.empty), CONNECTED)
      statusListeners.foreach(_ process event)

    case (edc: ElectrumClientConnected, HeaderSubscription(actor), CONNECTED) =>
      actor process HeaderSubscriptionResponse(edc.height, edc.tip)
      headerSubscriptions += actor

    case (ElectrumClientConnected(height, tip, oldRequests), request: ScriptHashSubscription, CONNECTED) =>
      val requests1 = oldRequests + Tuple2(me send request, request)
      val data1 = ElectrumClientConnected(height, tip, requests1)
      scriptHashSubscriptions(request.scriptHash) += request.from
      become(data1, CONNECTED)

    case (ElectrumClientConnected(height, tip, oldRequests), request: Request, CONNECTED) =>
      val requests1 = oldRequests + Tuple2(me send request, request)
      val data1 = ElectrumClientConnected(height, tip, requests1)
      become(data1, CONNECTED)

    case (ElectrumClientConnected(height, tip, oldRequests), Right(json: JsonRPCResponse), CONNECTED) =>
      for (request <- oldRequests get json.id) request.from process parseJsonResponse(request, json)
      become(ElectrumClientConnected(height, tip, oldRequests - json.id), CONNECTED)

    case (_, Left(response: HeaderSubscriptionResponse), CONNECTED) =>
      headerSubscriptions.foreach(_ process response)

    case (_, Left(response: ScriptHashSubscriptionResponse), CONNECTED) =>
      val listeners = scriptHashSubscriptions.getOrElse(response.scriptHash, Set.empty)
      listeners.foreach(_ process response)

    case (edc: ElectrumClientConnected, HeaderSubscriptionResponse(height1, tip1), CONNECTED) =>
      become(ElectrumClientConnected(height1, tip1, edc.requests), CONNECTED)

    case (edc: ElectrumClientConnected, AddStatusListener(actor), CONNECTED) =>
      actor process ElectrumReady(edc.height, edc.tip, serverAddress, me)
      statusListeners += actor

    case (_, AddStatusListener(actor), WAITING_VERSION | WAITING_TIP) =>
      statusListeners += actor

    case (_, KillYourself, WAITING_VERSION | WAITING_TIP | CONNECTED) =>
      statusListeners.foreach(_ process ElectrumDisconnected)
      pingTrigger.unsubscribe

    case _ =>
      // Do nothing
  }

  def send(request: Request): String = {
    val request = makeRequest(request, reqId.toString)
    if (ctx.channel.isWritable) ctx.channel writeAndFlush request
    else me process KillYourself
    reqId = reqId + 1
    reqId.toString
  }
}

object ElectrumClient {
  val CLIENT_NAME = "3.3.6"
  val PROTOCOL_VERSION = "1.4"
  val WAITING_VERSION = 0
  val WAITING_TIP = 1
  val CONNECTED = 2

  type ActorSet = Set[CanBeRepliedTo]
  type AnyRefList = util.List[AnyRef]
  type RequestMap = Map[String, Request]

  sealed trait ElectrumClientData
  case object ElectrumClientDisconnected extends ElectrumClientData
  case class ElectrumClientConnected(height: Int, tip: BlockHeader, requests: RequestMap) extends ElectrumClientData

  // expensive, shared with all clients
  val workerGroup = new NioEventLoopGroup

  def computeScriptHash(publicKeyScript: ByteVector): ByteVector32 =
    Crypto.sha256(publicKeyScript).reverse

  case class AddStatusListener(actor: CanBeRepliedTo)
  sealed trait Request { val from: CanBeRepliedTo }
  sealed trait Response

  case class ServerVersion(clientName: String, protocolVersion: String, from: CanBeRepliedTo) extends Request
  case class ServerVersionResponse(clientName: String, protocolVersion: String) extends Response

  case class Ping(from: CanBeRepliedTo) extends Request
  case object PingResponse extends Response

  case class TransactionHistoryItem(height: Int, txHash: ByteVector32)
  case class GetScriptHashHistory(scriptHash: ByteVector32, from: CanBeRepliedTo) extends Request
  case class GetScriptHashHistoryResponse(scriptHash: ByteVector32, history: List[TransactionHistoryItem] = Nil) extends Response

  case class UnspentItem(txHash: ByteVector32, txPos: Int, value: Long, height: Long) {
    lazy val outPoint = OutPoint(txHash.reverse, txPos)
  }

  case class BroadcastTransaction(tx: Transaction, from: CanBeRepliedTo) extends Request
  case class BroadcastTransactionResponse(tx: Transaction, error: Option[Error] = None) extends Response

  case class GetTransaction(txid: ByteVector32, from: CanBeRepliedTo) extends Request
  case class GetTransactionResponse(tx: Transaction) extends Response

  case class GetHeaders(startHeight: Int, count: Int, from: CanBeRepliedTo) extends Request
  case class GetHeadersResponse(startHeight: Int, headers: Seq[BlockHeader], max: Int) extends Response

  case class GetMerkle(txid: ByteVector32, height: Int, from: CanBeRepliedTo) extends Request
  case class GetMerkleResponse(txid: ByteVector32, merkle: List[ByteVector32], blockHeight: Int, pos: Int) extends Response {
    lazy val root: ByteVector32 = loop(txid.reverse +: merkle.map(_.reverse), pos)

    @tailrec
    final def loop(hashes: Seq[ByteVector32], pos: Int): ByteVector32 = hashes match {
      case first +: second +: rest if pos % 2 == 1 => loop(Crypto.hash256(second ++ first) +: rest, pos / 2)
      case first +: second +: rest => loop(Crypto.hash256(first ++ second) +: rest, pos / 2)
      case first +: Nil => first
    }
  }

  case class ScriptHashSubscription(scriptHash: ByteVector32, from: CanBeRepliedTo) extends Request
  case class ScriptHashSubscriptionResponse(scriptHash: ByteVector32, status: String) extends Response

  case class HeaderSubscription(from: CanBeRepliedTo) extends Request
  case class HeaderSubscriptionResponse(height: Int, header: BlockHeader) extends Response

  case class ServerError(request: Request, error: Error) extends Response

  sealed trait ElectrumEvent
  case object ElectrumDisconnected extends ElectrumEvent
  case class ElectrumReady(height: Int, tip: BlockHeader, serverAddress: InetSocketAddress, sender: CanBeRepliedTo) extends ElectrumEvent

  sealed trait SSL

  object SSL {
    case object DECIDE extends SSL
    case object LOOSE extends SSL
  }

  def parseResponse(input: String): Either[Response, JsonRPCResponse] = {
    val json = JsonMethods.parse(input)

    json \ "method" match {
      case JString(method) =>
        val JArray(params) = json \ "params"
        val result = (method -> params: @unchecked) match {
          case ("blockchain.headers.subscribe", header :: Nil) =>
            val (height, header1) = parseBlockHeader(header)
            HeaderSubscriptionResponse(height, header1)

          case ("blockchain.scripthash.subscribe", JString(scriptHashHex) :: JNull :: Nil) =>
            ScriptHashSubscriptionResponse(ByteVector32.fromValidHex(scriptHashHex), "")

          case ("blockchain.scripthash.subscribe", JString(scriptHashHex) :: JString(status) :: Nil) =>
            ScriptHashSubscriptionResponse(ByteVector32.fromValidHex(scriptHashHex), status)
        }

        Left(result)

      case _ =>
        parseJsonRpcResponse(json).asRight
    }
  }

  def parseJsonRpcResponse(json: JValue): JsonRPCResponse = {
    val error = json \ "error" match {
      case JNothing => None
      case JNull => None

      case other =>
        val message = other \ "message" match {
          case JString(value) => value
          case _ => ""
        }

        val code = other \ " code" match {
          case JInt(value) => value.intValue
          case JLong(value) => value.intValue
          case _ => 0
        }

        Error(code, message).asSome
    }

    val id = json \ "id" match {
      case JString(value) => value
      case JInt(value) => value.toString
      case JLong(value) => value.toString
      case _ => ""
    }

    JsonRPCResponse(json \ "result", error, id)
  }

  def longField(jvalue: JValue, field: String): Long = (jvalue \ field: @unchecked) match {
    case JLong(value) => value.longValue case JInt(value) => value.longValue
  }

  def intField(jvalue: JValue, field: String): Int = (jvalue \ field: @unchecked) match {
    case JLong(value) => value.intValue case JInt(value) => value.intValue
  }

  def parseBlockHeader(json: JValue): (Int, BlockHeader) = {
    val height = intField(json, "height")
    val JString(hex) = json \ "hex"
    (height, BlockHeader read hex)
  }

  def makeRequest(request: Request, reqId: String): JsonRPCRequest = request match {
    case ScriptHashSubscription(scriptHash, _) => JsonRPCRequest(id = reqId, method = "blockchain.scripthash.subscribe", params = scriptHash.toString :: Nil)
    case ServerVersion(clientName, protocolVersion, _) => JsonRPCRequest(id = reqId, method = "server.version", params = clientName :: protocolVersion :: Nil)
    case BroadcastTransaction(tx, _) => JsonRPCRequest(id = reqId, method = "blockchain.transaction.broadcast", params = Transaction.write(tx).toHex :: Nil)
    case GetScriptHashHistory(scripthash, _) => JsonRPCRequest(id = reqId, method = "blockchain.scripthash.get_history", params = scripthash.toHex :: Nil)
    case GetHeaders(startHeight, count, _) => JsonRPCRequest(id = reqId, method = "blockchain.block.headers", params = startHeight :: count :: Nil)
    case GetMerkle(txid, height, _) => JsonRPCRequest(id = reqId, method = "blockchain.transaction.get_merkle", params = txid :: height :: Nil)
    case GetTransaction(txid, _) => JsonRPCRequest(id = reqId, method = "blockchain.transaction.get", params = txid :: Nil)
    case _: HeaderSubscription => JsonRPCRequest(id = reqId, method = "blockchain.headers.subscribe", params = Nil)
    case _: Ping => JsonRPCRequest(id = reqId, method = "server.ping", params = Nil)
  }

  private def histItem(jValue: JValue) = {
    val JString(txHash) = jValue \ "tx_hash"
    val blockHeight = intField(jValue, "height")
    val hexHash = ByteVector32.fromValidHex(txHash)
    TransactionHistoryItem(blockHeight, hexHash)
  }

  def parseJsonResponse(request: Request, json: JsonRPCResponse): Response =
    json.error match {
      case err @ Some(errorBody) => (request: @unchecked) match {
        case msg: BroadcastTransaction => BroadcastTransactionResponse(msg.tx, err)
        case _ => ServerError(request, errorBody)
      }

      case None => (request: @unchecked) match {
        case _: ServerVersion =>
          val JArray(jItems) = json.result
          val JString(clientName) = jItems.head
          val JString(protocolVersion) = jItems(1)
          ServerVersionResponse(clientName, protocolVersion)

        case _: Ping => PingResponse

        case msg: GetScriptHashHistory =>
          val JArray(jItems) = json.result
          val historyItems = jItems.map(histItem)
          GetScriptHashHistoryResponse(msg.scriptHash, historyItems)

        case _: GetTransaction =>
          val JString(hex) = json.result
          GetTransactionResponse(Transaction read hex)

        case ScriptHashSubscription(scriptHash, _) => json.result match {
          case JString(status) => ScriptHashSubscriptionResponse(scriptHash, status)
          case _ => ScriptHashSubscriptionResponse(scriptHash, "")
        }

        case msg: BroadcastTransaction =>
          val JString(message) = json.result
          Try(ByteVector32 fromValidHex message) match {
            case Success(txid) if txid == msg.tx.txid => BroadcastTransactionResponse(msg.tx, error = None)
            case Success(txid) => BroadcastTransactionResponse(msg.tx, Error(1, s"response/request $txid mismatch").asSome)
            case _ => BroadcastTransactionResponse(msg.tx, Error(1, message).asSome)
          }

        case msg: GetHeaders =>
          val max = intField(json.result, "max")
          val JString(hex) = json.result \ "hex"
          val bin = ByteVector.fromValidHex(hex).toArray
          val blockHeaders = bin.grouped(80).map(BlockHeader.read).toList
          GetHeadersResponse(msg.startHeight, blockHeaders, max)

        case msg: GetMerkle =>
          val JInt(position) = json.result \ "pos"
          val JArray(hashes) = json.result \ "merkle"
          val blockHeight = intField(json.result, "block_height")
          val leaves = hashes collect { case JString(value) => ByteVector32 fromValidHex value }
          GetMerkleResponse(msg.txid, leaves, blockHeight, position.toInt)
      }
    }
}
