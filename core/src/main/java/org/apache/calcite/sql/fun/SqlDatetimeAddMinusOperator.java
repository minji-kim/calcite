/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.fun;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalLiteral.IntervalValue;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.IntervalSqlType;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlMonotonicity;

/**
 * A special operator for infix datetime plus/minus operator, '<code>DATETIME +/- INTERVAL</code>'.
 */
public class SqlDatetimeAddMinusOperator extends SqlSpecialOperator {
  private final String parseName;

  public SqlDatetimeAddMinusOperator(
      String name,
      String parseName,
      SqlKind kind,
      SqlOperandTypeChecker operandTypeChecker) {
    super(
        name,
        kind,
        40,
        true,
        null,
        InferTypes.FIRST_KNOWN,
        operandTypeChecker);
    this.parseName = parseName;
  }

  //~ Methods ----------------------------------------------------------------

  public SqlSyntax getSyntax() {
    return SqlSyntax.SPECIAL;
  }

  @Override public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
    final RelDataTypeFactory typeFactory = opBinding.getTypeFactory();
    final RelDataType leftType = opBinding.getOperandType(0);
    final IntervalSqlType unitType =
        (IntervalSqlType) opBinding.getOperandType(1);
    switch (unitType.getIntervalQualifier().getStartUnit()) {
    case HOUR:
    case MINUTE:
    case SECOND:
    case MILLISECOND:
    case MICROSECOND:
      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(SqlTypeName.TIMESTAMP),
          leftType.isNullable() || unitType.isNullable());
    default:
      return leftType;
    }
  }

  @Override public SqlMonotonicity getMonotonicity(SqlOperatorBinding call) {
    if (getKind() == SqlKind.PLUS) {
      return SqlStdOperatorTable.PLUS.getMonotonicity(call);
    } else {
      return SqlStdOperatorTable.MINUS.getMonotonicity(call);
    }
  }

  public void unparse(
      SqlWriter writer,
      SqlCall call,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    SqlDialect dialect = writer.getDialect();
    switch(dialect.getDatabaseProduct()) {
    case MSSQL:
      final boolean plus = this.getKind() == SqlKind.PLUS;
      if (unparseDatetimeAddOrMinus(plus, call, writer, leftPrec, rightPrec)) {
        return;
      }
    }
    final SqlWriter.Frame frame = writer.startList("(", ")");
    call.operand(0).unparse(writer, leftPrec, rightPrec);
    writer.sep(parseName);
    call.operand(1).unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
  }

  private static boolean unparseDatetimeAddOrMinus(
      boolean plus,
      SqlCall call,
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    assert call.operandCount() == 2;
    final SqlNode operDateTime = call.operand(0);
    final SqlIntervalLiteral operInterval = call.operand(1);

    if (operDateTime == null || operInterval == null) {
      return false;
    }

    final IntervalValue intervalValue = (IntervalValue) operInterval.getValue();
    final String needToCleanUp = intervalValue.getIntervalLiteral();
    final String finalString;
    assert needToCleanUp != null && !needToCleanUp.isEmpty();
    final boolean positiveValue;
    if (needToCleanUp.startsWith("-")) {
      positiveValue = false;
      finalString = needToCleanUp.substring(1).trim();
    } else if (needToCleanUp.startsWith("+")) {
      positiveValue = true;
      finalString = needToCleanUp.substring(1).trim();
    } else {
      finalString = needToCleanUp.trim();
      positiveValue = true;
    }

    final String sign;
    if ((!plus && positiveValue) || (plus && !positiveValue)) {
      sign = "-";
    } else {
      sign = "";
    }

    final SqlIntervalQualifier intervalQualifier = intervalValue.getIntervalQualifier();
    assert intervalQualifier.getEndUnit() == null
        || intervalQualifier.getStartUnit() == intervalQualifier.getEndUnit();
    final SqlWriter.Frame frame = writer.startFunCall("DATEADD");
    writer.sep(",");
    writer.literal(intervalQualifier.getStartUnit().toString());
    writer.sep(",");
    writer.literal(sign + finalString);
    writer.sep(",");
    operDateTime.unparse(writer, leftPrec, rightPrec);
    writer.endList(frame);
    return true;
  }

}
// End SqlDatetimeAddMinusOperator.java
