/*
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
package io.trino.plugin.pinot.query;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotMetadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Type;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.request.PinotQuery;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.OrderByExpressionContext;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.utils.BrokerRequestToQueryContextConverter;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static io.trino.plugin.pinot.query.PinotSqlFormatter.formatFilter;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public final class DynamicTableBuilder
{
    private static final CalciteSqlCompiler REQUEST_COMPILER = new CalciteSqlCompiler();
    private static final String WILDCARD = "*";
    public static final String OFFLINE_SUFFIX = "_OFFLINE";
    public static final String REALTIME_SUFFIX = "_REALTIME";

    private DynamicTableBuilder()
    {
    }

    public static DynamicTable buildFromPql(PinotMetadata pinotMetadata, SchemaTableName schemaTableName)
    {
        requireNonNull(pinotMetadata, "pinotMetadata is null");
        requireNonNull(schemaTableName, "schemaTableName is null");
        String query = schemaTableName.getTableName();
        BrokerRequest request = REQUEST_COMPILER.compileToBrokerRequest(query);
        PinotQuery pinotQuery = request.getPinotQuery();
        QueryContext queryContext = BrokerRequestToQueryContextConverter.convert(request);
        String pinotTableName = stripSuffix(request.getQuerySource().getTableName());
        Optional<String> suffix = getSuffix(request.getQuerySource().getTableName());

        Map<String, ColumnHandle> columnHandles = pinotMetadata.getPinotColumnHandles(pinotTableName);
        List<String> selectionColumns = ImmutableList.of();
        List<OrderByExpression> orderBy = ImmutableList.of();
        if (queryContext.getSelectExpressions() != null && queryContext.getAggregationFunctions() == null) {
            checkState(queryContext.getSelectExpressions() != null && !queryContext.getSelectExpressions().isEmpty(), "Pinot selections is null or empty");
            checkState(queryContext.getAliasList() == null || queryContext.getAliasList().isEmpty() || !queryContext.getAliasList().stream().anyMatch(alias -> alias != null), "Aliases are not supported");
            selectionColumns = getPinotColumnNames(schemaTableName, queryContext.getSelectExpressions(), columnHandles);
        }

        if (queryContext.getOrderByExpressions() != null) {
            ImmutableList.Builder<OrderByExpression> orderByBuilder = ImmutableList.builder();
            for (OrderByExpressionContext orderByExpressionContext : queryContext.getOrderByExpressions()) {
                ExpressionContext expressionContext = orderByExpressionContext.getExpression();
                checkState(expressionContext.getType() == ExpressionContext.Type.IDENTIFIER, "Unexpected order by expression: '%s'", expressionContext);
                PinotColumnHandle columnHandle = getColumnHandle(schemaTableName, expressionContext.getIdentifier(), columnHandles);
                orderByBuilder.add(new OrderByExpression(columnHandle.getColumnName(), orderByExpressionContext.isAsc()));
            }
            orderBy = orderByBuilder.build();
        }

        List<String> groupByColumns = ImmutableList.of();
        if (queryContext.getGroupByExpressions() != null) {
            groupByColumns = getPinotColumnNames(schemaTableName, queryContext.getGroupByExpressions(), columnHandles);
        }

        Optional<String> filter = Optional.empty();
        if (pinotQuery.getFilterExpression() != null) {
            String formatted = formatFilter(schemaTableName, queryContext.getFilter(), columnHandles);
            filter = Optional.of(formatted);
        }

        ImmutableList.Builder<PinotColumnHandle> aggregateColumnsBuilder = ImmutableList.builder();
        if (queryContext.getAggregationFunctions() != null) {
            checkState(queryContext.getAggregationFunctions().length > 0, "Aggregation Functions is empty");
            for (AggregationFunction aggregationFunction : queryContext.getAggregationFunctions()) {
                aggregateColumnsBuilder.add(new PinotColumnHandle(
                        aggregationFunction.getResultColumnName(),
                        toTrinoType(aggregationFunction.getFinalResultColumnType())));
            }
        }

        return new DynamicTable(pinotTableName, suffix, selectionColumns, filter, groupByColumns, aggregateColumnsBuilder.build(), orderBy, OptionalLong.of(queryContext.getLimit()), getOffset(queryContext), query);
    }

    private static PinotColumnHandle getColumnHandle(SchemaTableName schemaTableName, String name, Map<String, ColumnHandle> columnHandles)
    {
        PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(name);
        if (columnHandle == null) {
            throw new ColumnNotFoundException(schemaTableName, name);
        }
        return columnHandle;
    }

    private static Type toTrinoType(DataSchema.ColumnDataType columnDataType)
    {
        switch (columnDataType) {
            case INT:
                return INTEGER;
            case LONG:
                return BIGINT;
            case FLOAT:
                return REAL;
            case DOUBLE:
                return DOUBLE;
            case STRING:
                return VARCHAR;
            case BYTES:
                return VARBINARY;
            case INT_ARRAY:
                return new ArrayType(INTEGER);
            case LONG_ARRAY:
                return new ArrayType(BIGINT);
            case DOUBLE_ARRAY:
                return new ArrayType(DOUBLE);
            case STRING_ARRAY:
                return new ArrayType(VARCHAR);
            default:
                break;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported column data type: " + columnDataType);
    }

    private static List<String> getPinotColumnNames(SchemaTableName schemaTableName, List<ExpressionContext> expressions, Map<String, ColumnHandle> columnHandles)
    {
        ImmutableList.Builder<String> pinotColumnNamesBuilder = ImmutableList.builder();
        for (ExpressionContext expressionContext : expressions) {
            switch (expressionContext.getType()) {
                case LITERAL:
                    pinotColumnNamesBuilder.add(expressionContext.getLiteral());
                    break;
                case IDENTIFIER:
                    if (expressionContext.getIdentifier().equals(WILDCARD)) {
                        pinotColumnNamesBuilder.addAll(columnHandles.values().stream().map(handle -> ((PinotColumnHandle) handle).getColumnName()).collect(toImmutableList()));
                    }
                    else {
                        pinotColumnNamesBuilder.add(getColumnHandle(schemaTableName, expressionContext.getIdentifier(), columnHandles).getColumnName());
                    }
                    break;
                case FUNCTION:
                    ExpressionContext rewrittenContext = rewriteExpressionContext(schemaTableName, expressionContext, columnHandles);
                    pinotColumnNamesBuilder.add(rewrittenContext.getFunction().toString());
                    break;
                default:
                    throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported pinot expression type: " + expressionContext.getType());
            }
        }
        return pinotColumnNamesBuilder.build();
    }

    private static ExpressionContext rewriteExpressionContext(SchemaTableName schemaTableName, ExpressionContext expressionContext, Map<String, ColumnHandle> columnHandles)
    {
        switch (expressionContext.getType()) {
            case LITERAL:
                return expressionContext;
            case IDENTIFIER:
                String pinotColumnName = getColumnHandle(schemaTableName, expressionContext.getIdentifier(), columnHandles).getColumnName();
                return ExpressionContext.forIdentifier(pinotColumnName);
            case FUNCTION:
                List<ExpressionContext> arguments = expressionContext.getFunction().getArguments().stream()
                        .map(context -> rewriteExpressionContext(schemaTableName, context, columnHandles))
                        .collect(toImmutableList());
                return ExpressionContext.forFunction(new FunctionContext(expressionContext.getFunction().getType(), expressionContext.getFunction().getFunctionName(), arguments));
            default:
                throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported pinot expression type: " + expressionContext.getType());
        }
    }

    private static OptionalLong getOffset(QueryContext queryContext)
    {
        if (queryContext.getOffset() > 0) {
            return OptionalLong.of(queryContext.getOffset());
        }
        else {
            return OptionalLong.empty();
        }
    }

    private static String stripSuffix(String tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.toUpperCase(ENGLISH).endsWith(OFFLINE_SUFFIX)) {
            return tableName.substring(0, tableName.length() - OFFLINE_SUFFIX.length());
        }
        else if (tableName.toUpperCase(ENGLISH).endsWith(REALTIME_SUFFIX)) {
            return tableName.substring(0, tableName.length() - REALTIME_SUFFIX.length());
        }
        else {
            return tableName;
        }
    }

    private static Optional<String> getSuffix(String tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.toUpperCase(ENGLISH).endsWith(OFFLINE_SUFFIX)) {
            return Optional.of(OFFLINE_SUFFIX);
        }
        else if (tableName.toUpperCase(ENGLISH).endsWith(REALTIME_SUFFIX)) {
            return Optional.of(REALTIME_SUFFIX);
        }
        else {
            return Optional.empty();
        }
    }
}
