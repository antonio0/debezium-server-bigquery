/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.debezium.server.bigquery;

import io.debezium.connector.mysql.antlr.MySqlAntlrDdlParser;
import io.debezium.relational.Tables;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class MysqlDdlParserTest {

  // Cloud SQL writes this to the binlog whenever a user is created or its password changes.
  // The connector stops on any binlog statement its DDL parser rejects.
  @Test
  void parsesCloudSqlUserFlush() {
    assertDoesNotThrow(() -> new MySqlAntlrDdlParser().parse("FLUSH TABLES `mysql`.`user`", new Tables()));
  }
}
