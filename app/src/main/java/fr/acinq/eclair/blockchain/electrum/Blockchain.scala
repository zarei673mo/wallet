package fr.acinq.eclair.blockchain.electrum

import java.math.BigInteger
import fr.acinq.bitcoin.{Block, BlockHeader, ByteVector32, decodeCompact}
import fr.acinq.eclair.blockchain.electrum.db.HeaderDb
import scala.annotation.tailrec

case class Blockchain(chainHash: ByteVector32, checkpoints: Vector[CheckPoint], headersMap: Map[ByteVector32, Blockchain.BlockIndex],
                      bestchain: Vector[Blockchain.BlockIndex], orphans: Map[ByteVector32, BlockHeader] = Map.empty) {

  val tip = bestchain.last

  val height = if (bestchain.isEmpty) 0 else bestchain.last.height

  def getHeader(height: Int): Option[BlockHeader] = {
    val correctHeight = bestchain.nonEmpty && height >= bestchain.head.height && height - bestchain.head.height < bestchain.size
    if (correctHeight) Some(bestchain(height - bestchain.head.height).header) else None
  }
}

object Blockchain {
  val RETARGETING_PERIOD = 2016
  val MAX_REORG = 72

  case class BlockIndex(header: BlockHeader, height: Int, parent: Option[BlockIndex], chainwork: BigInt) {
    lazy val hash = header.hash
  }

  def fromCheckpoints(chainhash: ByteVector32, checkpoints: Vector[CheckPoint] = Vector.empty): Blockchain =
    Blockchain(chainhash, checkpoints, Map.empty, Vector.empty)

  def fromGenesisBlock(chainhash: ByteVector32, genesis: BlockHeader): Blockchain = {
    val blockIndex = BlockIndex(genesis, height = 0, None, decodeCompact(genesis.bits)._1)
    Blockchain(chainhash, Vector.empty, Map(blockIndex.hash -> blockIndex), Vector(blockIndex))
  }

  def validateHeadersChunk(blockchain: Blockchain, height: Int, headers: Seq[BlockHeader] = Nil): Unit = {
    if (headers.isEmpty) return

    require(height % RETARGETING_PERIOD == 0, s"header chunk height $height not a multiple of 2016")
    require(BlockHeader.checkProofOfWork(headers.head), "proof of work check has failed")

    headers.tail.foldLeft(headers.head) {
      case (previous, current) =>
        require(BlockHeader.checkProofOfWork(current), "proof of work check has failed")
        require(current.hashPreviousBlock == previous.hash, "hash chain has been broken")
        // on mainnet all blocks with a re-targeting window have the same difficulty target
        // on testnet it doesn't hold, there can be a drop in difficulty if there are no blocks for 20 minutes
        require(blockchain.chainHash == Block.LivenetGenesisBlock.hash && current.bits == previous.bits)
        current
    }

    val cpindex = (height / RETARGETING_PERIOD) - 1

    if (cpindex < blockchain.checkpoints.length) {
      val checkpoint = blockchain.checkpoints(cpindex)
      require(headers.head.hashPreviousBlock == checkpoint.hash)
      require(blockchain.chainHash == Block.LivenetGenesisBlock.hash && headers.head.bits == checkpoint.nextBits)
    }

    if (cpindex < blockchain.checkpoints.length - 1) {
      // if we have a checkpoint after this chunk, check that also
      val nextCheckpoint = blockchain.checkpoints(cpindex + 1)
      require(headers.last.hash == nextCheckpoint.hash)
      require(headers.length == RETARGETING_PERIOD)

      val diff = BlockHeader.calculateNextWorkRequired(headers.last, headers.head.time)
      require(blockchain.chainHash == Block.LivenetGenesisBlock.hash && diff == nextCheckpoint.nextBits)
    }
  }

  def addHeadersChunk(blockchain: Blockchain, height: Int, headers: Seq[BlockHeader]): Blockchain = {
    if (headers.length > RETARGETING_PERIOD) {
      val blockchain1 = Blockchain.addHeadersChunk(blockchain, height, headers.take(RETARGETING_PERIOD))
      return Blockchain.addHeadersChunk(blockchain1, height + RETARGETING_PERIOD, headers.drop(RETARGETING_PERIOD))
    }

    if (headers.isEmpty) return blockchain
    validateHeadersChunk(blockchain, height, headers)
    val bigIntRetargetPeriod = BigInt(RETARGETING_PERIOD)

    height match {
      case _ if height == blockchain.checkpoints.length * RETARGETING_PERIOD =>
        val chain = blockchain.checkpoints(0) +: blockchain.checkpoints.dropRight(1)
        val chainWork = chain.map(work => Blockchain.chainWork(work.nextBits) * bigIntRetargetPeriod).sum
        val totalChainWork = chainWork + Blockchain.chainWork(headers.head)

        val blockIndex = BlockIndex(headers.head, height, None, totalChainWork)
        val chain1 = (Vector(blockIndex) /: headers.tail) { case (idx, header) =>
          val extendedChainWork = idx.last.chainwork + Blockchain.chainWork(header)
          idx :+ BlockIndex(header, idx.last.height + 1, Some(idx.last), extendedChainWork)
        }

        val headersMap1 = blockchain.headersMap ++ chain1.map(bi => bi.hash -> bi)
        blockchain.copy(bestchain = chain1, headersMap = headersMap1)

      case _ if height < blockchain.checkpoints.length * RETARGETING_PERIOD =>
        blockchain

      case _ if height == blockchain.height + 1 =>
        require(headers.head.hashPreviousBlock == blockchain.bestchain.last.hash)
        val chainWork = blockchain.bestchain.last.chainwork + Blockchain.chainWork(headers.head)
        val blockIndex = BlockIndex(headers.head, height, None, chainWork)

        val chain1 = (Vector(blockIndex) /: headers.tail) { case (idx, header) =>
          val extendedChainWork =idx.last.chainwork + Blockchain.chainWork(header)
          idx :+ BlockIndex(header, idx.last.height + 1, Some(idx.last), extendedChainWork)
        }

        val bestchain1 = blockchain.bestchain ++ chain1
        val headersMap1 = blockchain.headersMap ++ chain1.map(bi => bi.hash -> bi)
        blockchain.copy(bestchain = bestchain1, headersMap = headersMap1)

      case _ =>
        throw new IllegalArgumentException
    }
  }

  def addHeader(blockchain: Blockchain, height: Int, header: BlockHeader): Blockchain = {
    require(BlockHeader.checkProofOfWork(header), s"invalid proof of work for $header")

    blockchain.headersMap.get(header.hashPreviousBlock) match {
      case Some(parent) if parent.height == height - 1 =>
        if (height % RETARGETING_PERIOD != 0 && blockchain.chainHash == Block.LivenetGenesisBlock.hash) {
          // check difficulty target, which should be the same as for the parent block
          // we only check this on mainnet, on testnet rules are much more lax
          require(header.bits == parent.header.bits)
        }

        val chainWork = parent.chainwork + Blockchain.chainWork(header)
        val blockIndex = BlockIndex(header, height, Some(parent), chainWork)
        val headersMap1 = blockchain.headersMap + (blockIndex.hash -> blockIndex)

        val bestChain1 =
          if (parent == blockchain.bestchain.last) blockchain.bestchain :+ blockIndex
          else if (blockIndex.chainwork > blockchain.bestchain.last.chainwork) buildChain(blockIndex)
          else blockchain.bestchain

        blockchain.copy(headersMap = headersMap1, bestchain = bestChain1)

      case None if height < blockchain.height - 1000 => blockchain
      case None => throw new IllegalArgumentException
    }
  }

  def addHeaders(blockchain: Blockchain, height: Int, headers: Seq[BlockHeader] = Nil): Blockchain = {
    def loop(bc: Blockchain, hgt: Int, hs: Seq[BlockHeader] = Nil): Blockchain = if (hs.isEmpty) bc else loop(Blockchain.addHeader(bc, hgt, hs.head), hgt + 1, hs.tail)
    if (headers.isEmpty) blockchain else if (height % RETARGETING_PERIOD == 0) addHeadersChunk(blockchain, height, headers) else loop(blockchain, height, headers)
  }

  @tailrec
  def buildChain(index: BlockIndex, acc: Vector[BlockIndex] = Vector.empty): Vector[BlockIndex] =
    index.parent match { case Some(parent) => buildChain(parent, index +: acc) case None => index +: acc }

  def chainWork(target: BigInt): BigInt = BigInt(2).pow(256) / (target + 1)
  def chainWork(header: BlockHeader): BigInt = chainWork(header.bits)

  def chainWork(bits: Long): BigInt = {
    val (target, negative, overflow) = decodeCompact(bits)
    if (target == BigInteger.ZERO || negative || overflow) BigInt(0)
    else chainWork(target)
  }

  @tailrec
  def optimize(chain: Blockchain, acc: Vector[BlockIndex] = Vector.empty) : (Vector[BlockIndex], Blockchain) =
    if (chain.bestchain.size >= RETARGETING_PERIOD + MAX_REORG) {
      val saveMe = chain.bestchain.take(RETARGETING_PERIOD)
      val headersMap1 = chain.headersMap -- saveMe.map(_.hash)
      val bestchain1 = chain.bestchain.drop(RETARGETING_PERIOD)
      val checkpoints1 = chain.checkpoints :+ CheckPoint(saveMe.last.hash, bestchain1.head.header.bits)
      val chain1 = chain.copy(headersMap = headersMap1, bestchain = bestchain1, checkpoints = checkpoints1)
      optimize(chain1, acc ++ saveMe)
    } else {
      val safeWithoutCheckpoint = chain.bestchain.dropRight(MAX_REORG)
      (acc ++ safeWithoutCheckpoint, chain)
    }

  def getDifficulty(blockchain: Blockchain, height: Int, headerDb: HeaderDb): Option[Long] =
    if (blockchain.chainHash == Block.LivenetGenesisBlock.hash) {
      if (height % RETARGETING_PERIOD == 0) {
        for {
          parent <- blockchain.getHeader(height - 1) orElse headerDb.getHeader(height - 1)
          previous <- blockchain.getHeader(height - 2016) orElse headerDb.getHeader(height - 2016)
          target = BlockHeader.calculateNextWorkRequired(parent, previous.time)
        } yield target
      } else {
        blockchain.getHeader(height - 1) orElse headerDb.getHeader(height - 1) map (_.bits)
      }
    } else {
      None
    }
}
