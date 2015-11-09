/*
 * Copyright 2015 Daniel W. H. James
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dwhjames.awswrap.dynamodb

import com.amazonaws.services.dynamodbv2.model._

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

case class GameScore(
    userId:           String,
    gameTitle:        String,
    topScore:         Long,
    topScoreDateTime: DateTime,
    wins:             Long,
    losses:           Long
)

object GameScore {

  val tableName = "GameScores"
  val globalSecondaryIndexName = "GameTitleIndex"

  val tableRequest =
    new CreateTableRequest()
    .withTableName(GameScore.tableName)
    .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
    .withAttributeDefinitions(
      Schema.stringAttribute(Attributes.userId),
      Schema.stringAttribute(Attributes.gameTitle),
      Schema.numberAttribute(Attributes.topScore)
    )
    .withKeySchema(
      Schema.hashKey(Attributes.userId),
      Schema.rangeKey(Attributes.gameTitle)
    )
    .withGlobalSecondaryIndexes(
      new GlobalSecondaryIndex()
      .withIndexName(GameScore.globalSecondaryIndexName)
      .withProvisionedThroughput(Schema.provisionedThroughput(10L, 5L))
      .withKeySchema(
        Schema.hashKey(Attributes.gameTitle),
        Schema.rangeKey(Attributes.topScore)
      )
      .withProjection(
        new Projection()
        .withProjectionType(ProjectionType.KEYS_ONLY)
      )
    )

  object Attributes {
    val userId           = "UserId"
    val gameTitle        = "GameTitle"
    val topScore         = "TopScore"
    val topScoreDateTime = "TopScoreDateTime"
    val wins             = "Wins"
    val losses           = "Losses"
  }

  implicit object sameScoreSerializer extends DynamoDBSerializer[GameScore] {
    private val fmt = ISODateTimeFormat.dateTime

    override val tableName = GameScore.tableName
    override val hashAttributeName = Attributes.userId
    override val rangeAttributeName = Some(Attributes.gameTitle)

    override def primaryKeyOf(score: GameScore) =
      Map(
        Attributes.userId    -> score.userId,
        Attributes.gameTitle -> score.gameTitle
      )

    override def toAttributeMap(score: GameScore) =
      Map(
        Attributes.userId           -> score.userId,
        Attributes.gameTitle        -> score.gameTitle,
        Attributes.topScore         -> score.topScore,
        Attributes.topScoreDateTime -> fmt.print(score.topScoreDateTime),
        Attributes.wins             -> score.wins,
        Attributes.losses           -> score.losses
      )

    override def fromAttributeMap(item: collection.mutable.Map[String, AttributeValue]) =
      GameScore(
        userId           = item(Attributes.userId),
        gameTitle        = item(Attributes.gameTitle),
        topScore         = item(Attributes.topScore),
        topScoreDateTime = fmt.parseDateTime(item(Attributes.topScoreDateTime)),
        wins             = item(Attributes.wins),
        losses           = item(Attributes.losses)
      )
  }
}
