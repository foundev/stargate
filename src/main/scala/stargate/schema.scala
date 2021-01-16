/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package stargate

import stargate.cassandra._
import stargate.model._
import com.datastax.oss.driver.api.core.`type`.DataTypes

import scala.util.Try

object schema {

  val TRANSACION_TABLE_NAME = "transactions"
  val ENTITY_ID_COLUMN_NAME = "entityId"
  val TRANSACTION_ID_COLUMN_NAME = "transactionId"
  val TRANSACTION_DELETED_COLUMN_NAME = "transactionDeleted"
  val TRANSACTION_STATE_COLUMN_NAME = "transactionState"
  val RELATION_FROM_COLUMN_NAME = "from"
  val RELATION_TO_COLUMN_NAME = "to"
  val RELATION_JOIN_STRING = "."
  val RELATION_SPLIT_REGEX = "\\."

  val transactionIdColumn = DefaultCassandraColumn(TRANSACTION_ID_COLUMN_NAME, DataTypes.frozenListOf(DataTypes.BIGINT))
  def baseTableName(entityName: String) = entityName
  val baseTableKey = CassandraKey(List(DefaultCassandraColumn(ENTITY_ID_COLUMN_NAME, DataTypes.UUID)), List.empty)
  def viewTableName(entityName: String, key: CassandraKeyNames) = entityName + "_" + key.partitionKeys.mkString("_") + "_" + key.clusteringKeys.mkString("_")
  def relationTableName(entityName: String, relationName: String) = entityName + "_" + relationName
  val relationTableKey = CassandraKey(List(DefaultCassandraColumn(RELATION_FROM_COLUMN_NAME, DataTypes.UUID)), List(DefaultCassandraColumn(RELATION_TO_COLUMN_NAME, DataTypes.UUID)))
  val relationTableColumns = CassandraColumns(relationTableKey, List.empty)

  type GroupedConditions[T] = Map[List[String], List[ScalarCondition[T]]]
  def MATCH_ALL_CONDITION[T]: GroupedConditions[T] = Map((List.empty, List.empty))

  def appendEntityIdKey(key: CassandraKeyNames): CassandraKeyNames = {
    if(key.combined.contains(ENTITY_ID_COLUMN_NAME)){
      key
    } else {
      CassandraKeyNames(key.partitionKeys, key.clusteringKeys ++ List(ENTITY_ID_COLUMN_NAME))
    }
  }

  def rampTable(table: CassandraTable): CassandraTable = {
    val key = CassandraKey(table.columns.key.partitionKeys, table.columns.key.clusteringKeys ++ List(transactionIdColumn))
    val data = table.columns.data ++ List(DefaultCassandraColumn(TRANSACTION_DELETED_COLUMN_NAME, DataTypes.BOOLEAN))
    CassandraTable(table.keyspace, table.name, CassandraColumns(key, data))
  }

  def conditionsKey(conditions: NamedConditions, cardinality: String=>Option[Long], minPartitionKeys: Long): CassandraKeyNames = {
    val sorted = conditions.sortBy(cond => cardinality(cond.field)).reverse
    val (eq, noteq) = sorted.partition(cond => ScalarComparison.isEquality(cond.comparison))
    val noteqSet = noteq.map(_.field).toSet
    var equalities = eq.map(_.field).filter(!noteqSet.contains(_))
    var partitionKeys: List[String] = List()
    var product = 1L
    while(equalities.nonEmpty && product < minPartitionKeys) {
      val next = equalities.head
      partitionKeys = next :: partitionKeys
      // currently assumming if cardinality is unspecified, it is high enough to use as partition key
      product = product * cardinality(next).getOrElse(minPartitionKeys)
      equalities = equalities.tail
    }
    require(partitionKeys.nonEmpty && product >= minPartitionKeys, s"""condition ${conditions} does not have suitable equality conditions to choose a partition key""")
    partitionKeys = partitionKeys.reverse
    val partitionKeySet = partitionKeys.toSet
    val clusteringKeys = sorted.map(_.field).filter(!partitionKeySet.contains(_)).distinct
    CassandraKeyNames(partitionKeys, clusteringKeys)
  }

  def keySupportsConditions(columns: CassandraColumnNames, conditions: NamedConditions): KeyConditionScore = {
    if(conditions.isEmpty) {
      KeyConditionScore(if(columns.key == baseTableKey.names) 0 else 1, 0, false, 0)
    }
    val allConditions = conditions.map(_.field).toSet
    val missingColumns = allConditions.diff(columns.combined.toSet).size
    val partitionSet = columns.key.partitionKeys.toSet
    val clusteringSet = columns.key.clusteringKeys.toSet
    val allKeysSet = partitionSet ++ clusteringSet
    val eq_noteq = conditions.partition(x => ScalarComparison.isEquality(x.comparison))
    val eq :: noteq :: _ = List(eq_noteq._1, eq_noteq._2).map(_.map(_.field).toSet)
    val missingKeys = eq.diff(allKeysSet).size + noteq.diff(clusteringSet).size
    val missingPartitionKeys = partitionSet.diff(eq).nonEmpty
    val allKeys = columns.key.partitionKeys.map((_, true)) ++ columns.key.clusteringKeys.map((_, false))
    val coveredKeys = allKeys.takeWhile(key => if(key._2) eq.contains(key._1) else allConditions.contains(key._1))
    val skipScore = allConditions.size - coveredKeys.length
    KeyConditionScore(missingColumns, missingKeys, missingPartitionKeys, skipScore)
  }

  def tableScores(conditions: NamedConditions, tables: List[CassandraTable]): Map[KeyConditionScore, List[CassandraTable]] = {
    val scores = tables.map(t => (keySupportsConditions(t.columns.names, conditions), t))
    scores.groupMap(_._1)(_._2)
  }

  def transactionStateTable(keyspace: String): CassandraTable = {
    CassandraTable(keyspace, TRANSACION_TABLE_NAME, CassandraColumns(CassandraKey(List(transactionIdColumn), List.empty), List(DefaultCassandraColumn(TRANSACTION_STATE_COLUMN_NAME, DataTypes.INT))))
  }

  def baseTable(keyspace: String, entity: Entity): CassandraTable = {
    val tableName = baseTableName(entity.name)
    val baseKeyNames = baseTableKey.names.combined.toSet
    val columns = CassandraColumns(baseTableKey, entity.fields.values.filter(f => !baseKeyNames(f.name)).map(_.column).toList)
    CassandraTable(keyspace, tableName, columns)
  }

  def relationTables(keyspace: String, entity: Entity): Map[(String,String), CassandraTable] = {
    def toTable(relation: String) = CassandraTable(keyspace, relationTableName(entity.name, relation), relationTableColumns)
    entity.relations.toList.map((name_rel) => ((entity.name, name_rel._1), toTable(name_rel._1))).toMap
  }

  def groupConditionsByPath[T](conditions: List[T], conditionColumn: T=>String): Map[List[String], List[(String,T)]] = {
    val parsedByEntity = conditions.map(cond => {
      val cols = conditionColumn(cond).split(RELATION_SPLIT_REGEX)
      val (path, scalar) = cols.splitAt(cols.length - 1)
      (path.toList, (scalar(0), cond))
    })
    parsedByEntity.groupMap(_._1)(_._2)
  }

  def groupNamedConditionsByPath(conditions: NamedConditions): Map[List[String], NamedConditions] = {
    val grouped = groupConditionsByPath(conditions, (_:NamedCondition).field)
    grouped.view.mapValues(_.map(x => NamedCondition(x._1, x._2.comparison))).toMap
  }

  def groupConditionsByPath[T](conditions: List[ScalarCondition[T]]): GroupedConditions[T] = {
    val grouped = groupConditionsByPath(conditions, (_:ScalarCondition[T]).field)
    grouped.view.mapValues(_.map(x => ScalarCondition(x._1, x._2.comparison, x._2.argument))).toMap
  }

  def traverseRelationPath(model: Entities, entityName: String, path: List[String]): List[RelationField] = {
    if(path.isEmpty) {
      List.empty
    } else {
      val relation = model(entityName).relations(path.head)
      val nextEntity = relation.targetEntityName
      relation +: traverseRelationPath(model, nextEntity, path.tail)
    }
  }
  // TODO - clean up duplication between these two (to get entity name would have to get it from last O(N) of relation path)
  def traverseEntityPath(model: Entities, entityName: String, path: List[String]): String = {
    if(path.isEmpty) {
      entityName
    } else {
      val nextEntity = model(entityName).relations(path.head).targetEntityName
      traverseEntityPath(model, nextEntity, path.tail)
    }
  }

  def queryTables(model: InputModel, keyspace: String, rootEntity: String, conditions: NamedConditions, minPartitions: Long = 1000): Map[String, List[CassandraTable]] = {
    if(conditions.isEmpty) {
      return Map.empty
    }
    val groupedConditions = groupNamedConditionsByPath(conditions)
    val tables = groupedConditions.toList.map(path_conds => {
      val entityName = traverseEntityPath(model.entities, rootEntity, path_conds._1)
      val keyNames = Try(appendEntityIdKey(conditionsKey(path_conds._2, model.cardinality(entityName, _), minPartitions)))
      require(keyNames.isSuccess, s"failed to create view for condition ${conditions} on entity ${rootEntity}:\n${keyNames.failed.get}")
      val tableName = viewTableName(entityName, keyNames.get)
      def nameToCol(name: String): CassandraColumn = (if(name == ENTITY_ID_COLUMN_NAME) DefaultCassandraColumn.apply _ else OptionalCassandraColumn.apply _)(name, model.fieldColumnType(entityName, name))
      val key = CassandraKey(keyNames.get.partitionKeys.map(nameToCol), keyNames.get.clusteringKeys.map(nameToCol))
      val columns = CassandraColumns(key, List.empty)
      (entityName, CassandraTable(keyspace, tableName, columns))
    })
    tables.groupMap(_._1)(_._2)
  }

  def mergeMultimaps[K,V](maps: List[Map[K, List[V]]]): Map[K, List[V]] = maps.flatMap(_.toList).groupBy(_._1).view.mapValues(_.flatMap(_._2)).toMap
  def mergeMultimaps[K,V](a: Map[K, List[V]], b: Map[K, List[V]]): Map[K, List[V]] = mergeMultimaps(List(a,b))



  def conditionsTables(model: InputModel, keyspace: String, minPartitions: Long = 1000): Map[String, List[CassandraTable]] = {
    mergeMultimaps(model.conditions.toList.map(name_conditionsList => {
      val (entityName, conditionsList) = name_conditionsList
      mergeMultimaps(conditionsList.map(conditions => queryTables(model, keyspace, entityName, conditions, minPartitions)))
    }))
  }

  def modelTables(model: InputModel, keyspace: String, minPartitions: Long = 1000): (Map[String, List[CassandraTable]], Map[(String,String), CassandraTable]) = {
    val baseTables = model.entities.view.mapValues(baseTable(keyspace, _)).mapValues(List(_)).toMap
    val _relationTables = model.entities.values.flatMap(relationTables(keyspace, _)).toMap
    val _queryTables = conditionsTables(model, keyspace, minPartitions)
    // TODO: just doing a minimal dedupe of tables right now; should later determine mininmal set of tables needed for all conditions
    val entityTables = mergeMultimaps(baseTables, _queryTables).view.mapValues(_.toSet.toList).toMap
    (entityTables, _relationTables)
  }

  def defaultOutputModel(model: InputModel, keyspace: String, minPartitions: Long = 1000): OutputModel = {
    val (entityTables, relationTables) = modelTables(model, keyspace, minPartitions)
    OutputModel(model, entityTables, relationTables, null)
  }

  def rampOutputModel(model: InputModel, keyspace: String, minPartitions: Long = 1000): OutputModel = {
    val (entityTables, relationTables) = modelTables(model, keyspace, minPartitions)
    val transactionTable = transactionStateTable(keyspace)
    OutputModel(model, entityTables.view.mapValues(_.map(rampTable)).toMap, relationTables.view.mapValues(rampTable).toMap, transactionTable)
  }

  def outputModel(model: InputModel, keyspace: String, mode: Mode.Value, minPartitions: Long = 1000): OutputModel = {
    mode match {
      case Mode.SINGLE => defaultOutputModel(model, keyspace, minPartitions)
      case Mode.BATCH => defaultOutputModel(model, keyspace, minPartitions)
      case Mode.TRANSACTION => rampOutputModel(model, keyspace, minPartitions)
      case _ => throw new RuntimeException(s"invalid query mode: ${mode}")
    }
  }

}
