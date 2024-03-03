/*
 * Copyright 2019 ACINQ SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package fr.acinq.eclair.blockchain.electrum

import java.io.InputStream
import fr.acinq.bitcoin.{Block, ByteVector32, encodeCompact}
import fr.acinq.eclair.blockchain.electrum.db.HeaderDb
import org.json4s.JsonAST.{JArray, JInt, JString}
import org.json4s.native.JsonMethods


case class CheckPoint(hash: ByteVector32, nextBits: Long)

object CheckPoint {
  var loadFromChainHash: ByteVector32 => Vector[CheckPoint] = {
    case Block.LivenetGenesisBlock.hash => load(classOf[CheckPoint] getResourceAsStream "/electrum/checkpoints_mainnet.json")
    case Block.TestnetGenesisBlock.hash => load(classOf[CheckPoint] getResourceAsStream "/electrum/checkpoints_testnet.json")
    case _ => throw new RuntimeException
  }

  def load(stream: InputStream): Vector[CheckPoint] = {
    val JArray(values) = JsonMethods.parse(stream)

    values.collect { case JArray(JString(a) :: JInt(b) :: Nil) =>
      val hash = ByteVector32.fromValidHex(a).reverse
      val nextBits = encodeCompact(b.bigInteger)
      CheckPoint(hash, nextBits)
    }.toVector
  }

  def load(chainHash: ByteVector32, headerDb: HeaderDb): Vector[CheckPoint] = {
    val checkpoints = CheckPoint.loadFromChainHash(chainHash)
    import Blockchain.{ RETARGETING_PERIOD => RP }

    headerDb.getTip.map { case (height, _) =>
      val start = checkpoints.size * RP - 1 + RP
      val end = height - RP

      val newcheckpoints = for {
        height <- start to end by RP
      } yield {
        val cpheader = headerDb.getHeader(height).get
        val nextDiff = headerDb.getHeader(height + 1).get
        CheckPoint(cpheader.hash, nextDiff.bits)
      }

      checkpoints ++ newcheckpoints
    } getOrElse checkpoints
  }
}
