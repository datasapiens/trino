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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.plugin.pinot.PinotException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.geospatial.transform.function.StAreaFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsBinaryFunction;
import org.apache.pinot.core.geospatial.transform.function.StAsTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StContainsFunction;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.geospatial.transform.function.StEqualsFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeogFromWKBFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromTextFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeomFromWKBFunction;
import org.apache.pinot.core.geospatial.transform.function.StGeometryTypeFunction;
import org.apache.pinot.core.geospatial.transform.function.StPointFunction;
import org.apache.pinot.core.geospatial.transform.function.StPolygonFunction;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.AdditionTransformFunction;
import org.apache.pinot.core.operator.transform.function.AndOperatorTransformFunction;
import org.apache.pinot.core.operator.transform.function.ArrayAverageTransformFunction;
import org.apache.pinot.core.operator.transform.function.ArrayLengthTransformFunction;
import org.apache.pinot.core.operator.transform.function.ArrayMaxTransformFunction;
import org.apache.pinot.core.operator.transform.function.ArrayMinTransformFunction;
import org.apache.pinot.core.operator.transform.function.ArraySumTransformFunction;
import org.apache.pinot.core.operator.transform.function.CaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.CastTransformFunction;
import org.apache.pinot.core.operator.transform.function.DateTimeConversionTransformFunction;
import org.apache.pinot.core.operator.transform.function.DateTruncTransformFunction;
import org.apache.pinot.core.operator.transform.function.DivisionTransformFunction;
import org.apache.pinot.core.operator.transform.function.EqualsTransformFunction;
import org.apache.pinot.core.operator.transform.function.GreaterThanOrEqualTransformFunction;
import org.apache.pinot.core.operator.transform.function.GreaterThanTransformFunction;
import org.apache.pinot.core.operator.transform.function.GroovyTransformFunction;
import org.apache.pinot.core.operator.transform.function.InIdSetTransformFunction;
import org.apache.pinot.core.operator.transform.function.JsonExtractKeyTransformFunction;
import org.apache.pinot.core.operator.transform.function.JsonExtractScalarTransformFunction;
import org.apache.pinot.core.operator.transform.function.LessThanOrEqualTransformFunction;
import org.apache.pinot.core.operator.transform.function.LessThanTransformFunction;
import org.apache.pinot.core.operator.transform.function.LiteralTransformFunction;
import org.apache.pinot.core.operator.transform.function.LookupTransformFunction;
import org.apache.pinot.core.operator.transform.function.MapValueTransformFunction;
import org.apache.pinot.core.operator.transform.function.ModuloTransformFunction;
import org.apache.pinot.core.operator.transform.function.MultiplicationTransformFunction;
import org.apache.pinot.core.operator.transform.function.NotEqualsTransformFunction;
import org.apache.pinot.core.operator.transform.function.OrOperatorTransformFunction;
import org.apache.pinot.core.operator.transform.function.ScalarTransformFunctionWrapper;
import org.apache.pinot.core.operator.transform.function.SingleParamMathTransformFunction;
import org.apache.pinot.core.operator.transform.function.SubtractionTransformFunction;
import org.apache.pinot.core.operator.transform.function.TimeConversionTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.operator.transform.function.ValueInTransformFunction;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.utils.JsonUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_INVALID_PQL_GENERATED;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_UNSUPPORTED_COLUMN_TYPE;
import static io.trino.plugin.pinot.query.PinotSqlFormatter.stripQuotes;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class PinotTypeResolver
{
    // Extracted from org.apache.pinot.core.operator.transform.function.GroovyTransformFunction
    private static final String RETURN_TYPE_KEY = "returnType";
    private static final String IS_SINGLE_VALUE_KEY = "isSingleValue";

    // Extracted from org.apache.pinot.core.operator.transform.function.TransformFunctionFactory
    private static final Map<String, Class<? extends TransformFunction>> TRANSFORM_FUNCTION_MAP =
            new HashMap<String, Class<? extends TransformFunction>>() {
                {
                    // NOTE: add all built-in transform functions here
                    put(canonicalize(TransformFunctionType.ADD.getName()), AdditionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.SUB.getName()), SubtractionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.MULT.getName()), MultiplicationTransformFunction.class);
                    put(canonicalize(TransformFunctionType.DIV.getName()), DivisionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.MOD.getName()), ModuloTransformFunction.class);

                    put(canonicalize(TransformFunctionType.PLUS.getName()), AdditionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.MINUS.getName()), SubtractionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.TIMES.getName()), MultiplicationTransformFunction.class);
                    put(canonicalize(TransformFunctionType.DIVIDE.getName()), DivisionTransformFunction.class);

                    put(canonicalize(TransformFunctionType.ABS.getName()), SingleParamMathTransformFunction.AbsTransformFunction.class);
                    put(canonicalize(TransformFunctionType.CEIL.getName()), SingleParamMathTransformFunction.CeilTransformFunction.class);
                    put(canonicalize(TransformFunctionType.EXP.getName()), SingleParamMathTransformFunction.ExpTransformFunction.class);
                    put(canonicalize(TransformFunctionType.FLOOR.getName()), SingleParamMathTransformFunction.FloorTransformFunction.class);
                    put(canonicalize(TransformFunctionType.LN.getName()), SingleParamMathTransformFunction.LnTransformFunction.class);
                    put(canonicalize(TransformFunctionType.SQRT.getName()), SingleParamMathTransformFunction.SqrtTransformFunction.class);

                    put(canonicalize(TransformFunctionType.CAST.getName()), CastTransformFunction.class);
                    put(canonicalize(TransformFunctionType.JSONEXTRACTSCALAR.getName()),
                            JsonExtractScalarTransformFunction.class);
                    put(canonicalize(TransformFunctionType.JSONEXTRACTKEY.getName()), JsonExtractKeyTransformFunction.class);
                    put(canonicalize(TransformFunctionType.TIMECONVERT.getName()), TimeConversionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.DATETIMECONVERT.getName()), DateTimeConversionTransformFunction.class);
                    put(canonicalize(TransformFunctionType.DATETRUNC.getName()), DateTruncTransformFunction.class);
                    put(canonicalize(TransformFunctionType.ARRAYLENGTH.getName()), ArrayLengthTransformFunction.class);
                    put(canonicalize(TransformFunctionType.VALUEIN.getName()), ValueInTransformFunction.class);
                    put(canonicalize(TransformFunctionType.MAPVALUE.getName()), MapValueTransformFunction.class);
                    put(canonicalize(TransformFunctionType.INIDSET.getName()), InIdSetTransformFunction.class);
                    put(canonicalize(TransformFunctionType.LOOKUP.getName()), LookupTransformFunction.class);

                    // Array functions
                    put(canonicalize(TransformFunctionType.ARRAYAVERAGE.getName()), ArrayAverageTransformFunction.class);
                    put(canonicalize(TransformFunctionType.ARRAYMAX.getName()), ArrayMaxTransformFunction.class);
                    put(canonicalize(TransformFunctionType.ARRAYMIN.getName()), ArrayMinTransformFunction.class);
                    put(canonicalize(TransformFunctionType.ARRAYSUM.getName()), ArraySumTransformFunction.class);

                    put(canonicalize(TransformFunctionType.GROOVY.getName()), GroovyTransformFunction.class);
                    put(canonicalize(TransformFunctionType.CASE.getName()), CaseTransformFunction.class);

                    put(canonicalize(TransformFunctionType.EQUALS.getName()), EqualsTransformFunction.class);
                    put(canonicalize(TransformFunctionType.NOT_EQUALS.getName()), NotEqualsTransformFunction.class);
                    put(canonicalize(TransformFunctionType.GREATER_THAN.getName()), GreaterThanTransformFunction.class);
                    put(canonicalize(TransformFunctionType.GREATER_THAN_OR_EQUAL.getName()),
                            GreaterThanOrEqualTransformFunction.class);
                    put(canonicalize(TransformFunctionType.LESS_THAN.getName()), LessThanTransformFunction.class);
                    put(canonicalize(TransformFunctionType.LESS_THAN_OR_EQUAL.getName()), LessThanOrEqualTransformFunction.class);

                    // logical functions
                    put(canonicalize(TransformFunctionType.AND.getName()), AndOperatorTransformFunction.class);
                    put(canonicalize(TransformFunctionType.OR.getName()), OrOperatorTransformFunction.class);

                    // geo functions
                    // geo constructors
                    put(canonicalize(TransformFunctionType.ST_GEOG_FROM_TEXT.getName()), StGeogFromTextFunction.class);
                    put(canonicalize(TransformFunctionType.ST_GEOG_FROM_WKB.getName()), StGeogFromWKBFunction.class);
                    put(canonicalize(TransformFunctionType.ST_GEOM_FROM_TEXT.getName()), StGeomFromTextFunction.class);
                    put(canonicalize(TransformFunctionType.ST_GEOM_FROM_WKB.getName()), StGeomFromWKBFunction.class);
                    put(canonicalize(TransformFunctionType.ST_POINT.getName()), StPointFunction.class);
                    put(canonicalize(TransformFunctionType.ST_POLYGON.getName()), StPolygonFunction.class);

                    // geo measurements
                    put(canonicalize(TransformFunctionType.ST_AREA.getName()), StAreaFunction.class);
                    put(canonicalize(TransformFunctionType.ST_DISTANCE.getName()), StDistanceFunction.class);
                    put(canonicalize(TransformFunctionType.ST_GEOMETRY_TYPE.getName()), StGeometryTypeFunction.class);

                    // geo outputs
                    put(canonicalize(TransformFunctionType.ST_AS_BINARY.getName()), StAsBinaryFunction.class);
                    put(canonicalize(TransformFunctionType.ST_AS_TEXT.getName()), StAsTextFunction.class);

                    // geo relationship
                    put(canonicalize(TransformFunctionType.ST_CONTAINS.getName()), StContainsFunction.class);
                    put(canonicalize(TransformFunctionType.ST_EQUALS.getName()), StEqualsFunction.class);
                }
            };

    private static String canonicalize(String functionName)
    {
        return StringUtils.remove(functionName, '_').toLowerCase(ENGLISH);
    }

    private Class<? extends TransformFunction> get(String functionName)
    {
        return TRANSFORM_FUNCTION_MAP.get(canonicalize(functionName));
    }

    private final Map<String, TransformResultMetadata> typeMap;

    public PinotTypeResolver()
    {
        ImmutableMap.Builder<String, TransformResultMetadata> builder = ImmutableMap.builder();
        for (Map.Entry<String, Class<? extends TransformFunction>> entry : TRANSFORM_FUNCTION_MAP.entrySet()) {
            try {
                Class<? extends TransformFunction> transformFunctionClass = entry.getValue();
                // Extracted from org.apache.pinot.core.operator.transform.function.TransformFunctionFactory
                // This is how Pinot will resolve the return type
                TransformFunction transformFunction = transformFunctionClass.getConstructor().newInstance();
                TransformResultMetadata metadata = transformFunction.getResultMetadata();
                if (metadata != null && metadata.getDataType() != null) {
                    builder.put(entry.getKey(), metadata);
                }
            }
            catch (Exception e) {
                // ignored
            }
        }
        this.typeMap = builder.build();
    }

    public TransformResultMetadata getReturnType(String functionName, List<ExpressionContext> parameters, SchemaTableName schemaTableName, Map<String, ColumnHandle> columnHandles)
    {
        // Extracted from org.apache.pinot.core.operator.transform.function.TransformFunctionFactory
        // This is how Pinot will resolve the return type
        functionName = canonicalize(functionName);
        FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName, parameters.size());
        try {
            TransformFunction transformFunction = new ScalarTransformFunctionWrapper(functionInfo);
            TransformResultMetadata metadata = transformFunction.getResultMetadata();
            if (metadata != null) {
                return metadata;
            }
        }
        catch (Exception e) {
            // ignored
        }
        TransformResultMetadata metadata = typeMap.get(functionName);
        if (metadata != null) {
            return metadata;
        }
        TransformFunctionType transformFunctionType = TransformFunctionType.getTransformFunctionType(functionName);
        if (transformFunctionType != null) {
            switch (transformFunctionType) {
                case CAST:
                    checkState(parameters.size() > 1 && parameters.get(1).getType() == ExpressionContext.Type.LITERAL, "Unexpected parameter for CAST: '%s'", parameters.get(1));
                    FieldSpec.DataType dataType = FieldSpec.DataType.valueOf(stripQuotes(parameters.get(1).getLiteral()).toUpperCase(ENGLISH));
                    if (dataType == null) {
                        throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unsupported expression: '%s'", parameters.get(1)));
                    }
                    return new TransformResultMetadata(dataType, false, false);
                case CASE:
                    int firstThenStatement = parameters.size() / 2 + 1;
                    checkState(parameters.size() > firstThenStatement, "Unexpected parameters for case statement: '%s'", parameters);
                    return resolveExpressionType(parameters.get(firstThenStatement), schemaTableName, columnHandles);
                case ARRAYMIN:
                case ARRAYMAX:
                case VALUEIN:
                    return resolveExpressionType(parameters.get(0), schemaTableName, columnHandles);
                case DATETIMECONVERT:
                    TransformResultMetadata firstParameterMetadata = resolveExpressionType(parameters.get(0), schemaTableName, columnHandles);
                    if (firstParameterMetadata.getDataType() == FieldSpec.DataType.STRING) {
                        return new TransformResultMetadata(FieldSpec.DataType.STRING, false, false);
                    }
                    else {
                        return new TransformResultMetadata(FieldSpec.DataType.LONG, false, false);
                    }
                case DATETRUNC:
                    // Extracted from org.apache.pinot.core.operator.transform.function.DateTruncTransformFunction
                    return new TransformResultMetadata(FieldSpec.DataType.LONG, false, false);
                case MAPVALUE:
                    return resolveExpressionType(parameters.get(2), schemaTableName, columnHandles);
                case JSONEXTRACTSCALAR:
                    String resultsType = parameters.get(2).getIdentifier().toUpperCase(ENGLISH);
                    boolean isSingleValue = !resultsType.endsWith("_ARRAY");
                    try {
                        FieldSpec.DataType jsonExtractDataType =
                                FieldSpec.DataType.valueOf(isSingleValue ? resultsType : resultsType.substring(0, resultsType.length() - 6));
                        return new TransformResultMetadata(jsonExtractDataType, isSingleValue, false);
                    }
                    catch (Exception e) {
                        throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unsupported expression: '%s'", parameters.get(2)));
                    }
                case GROOVY:
                    checkState(parameters.get(0).getType() == ExpressionContext.Type.LITERAL, "Unexpected type '%s' for expression '%s'", parameters.get(0).getType(), parameters.get(0));
                    String returnValueMetadataStr = parameters.get(0).getLiteral();
                    try {
                        JsonNode returnValueMetadataJson = JsonUtils.stringToJsonNode(returnValueMetadataStr);
                        checkState(returnValueMetadataJson.hasNonNull(RETURN_TYPE_KEY),
                                "The json string in the first argument of GROOVY transform function must have non-null 'returnType'");
                        checkState(returnValueMetadataJson.hasNonNull(IS_SINGLE_VALUE_KEY),
                                "The json string in the first argument of GROOVY transform function must have non-null 'isSingleValue'");
                        String returnTypeStr = returnValueMetadataJson.get(RETURN_TYPE_KEY).asText().toUpperCase(ENGLISH);
                        //Preconditions.checkState(EnumUtils.isValidEnum(FieldSpec.DataType.class, returnTypeStr),
                        //        "The 'returnType' in the json string which is the first argument of GROOVY transform function must be a valid FieldSpec.DataType enum value");
                        return new TransformResultMetadata(FieldSpec.DataType.valueOf(returnTypeStr),
                                returnValueMetadataJson.get(IS_SINGLE_VALUE_KEY).asBoolean(true), false);
                    }
                    catch (IOException e) {
                        throw new IllegalStateException(
                                "Caught exception when converting json string '" + returnValueMetadataStr + "' to JsonNode", e);
                    }
                case LOOKUP:
                    // Unsupported
                default:
                    throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unsupported expression: '%s'", parameters.get(1)));
            }
        }
        throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unsupported function '%s' with parameters '%s'", functionName, parameters));
    }

    public TransformResultMetadata resolveExpressionType(ExpressionContext expression, SchemaTableName schemaTableName, Map<String, ColumnHandle> columnHandles)
    {
        switch (expression.getType()) {
            case IDENTIFIER:
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(expression.getIdentifier().toLowerCase(ENGLISH));
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(schemaTableName, expression.getIdentifier());
                }
                return fromTrinoType(columnHandle.getDataType());
            case FUNCTION:
                return getReturnType(expression.getFunction().getFunctionName(), expression.getFunction().getArguments(), schemaTableName, columnHandles);
            case LITERAL:
                FieldSpec.DataType literalDataType = LiteralTransformFunction.inferLiteralDataType(new LiteralTransformFunction(expression.getLiteral()));
                return new TransformResultMetadata(literalDataType, false, false);
            default:
                throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.empty(), format("Unsupported expression: '%s'", expression));
        }
    }

    public static TransformResultMetadata fromTrinoType(Type type)
    {
        if (type instanceof ArrayType) {
            ArrayType arrayType = (ArrayType) type;
            Type elementType = arrayType.getElementType();
            return new TransformResultMetadata(fromPrimitiveTrinoType(elementType), false, false);
        }
        else {
            return new TransformResultMetadata(fromPrimitiveTrinoType(type), true, false);
        }
    }

    private static FieldSpec.DataType fromPrimitiveTrinoType(Type type)
    {
        if (type instanceof VarcharType) {
            return FieldSpec.DataType.STRING;
        }
        if (type instanceof BigintType) {
            return FieldSpec.DataType.LONG;
        }
        if (type instanceof IntegerType) {
            return FieldSpec.DataType.INT;
        }
        if (type instanceof DoubleType) {
            return FieldSpec.DataType.DOUBLE;
        }
        if (type instanceof RealType) {
            return FieldSpec.DataType.FLOAT;
        }
        if (type instanceof BooleanType) {
            return FieldSpec.DataType.BOOLEAN;
        }
        if (type instanceof VarbinaryType) {
            return FieldSpec.DataType.BYTES;
        }
        throw new PinotException(PINOT_UNSUPPORTED_COLUMN_TYPE, Optional.empty(), "Unsupported column data type: " + type);
    }
}
