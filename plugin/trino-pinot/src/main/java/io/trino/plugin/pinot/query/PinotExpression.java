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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.pinot.PinotColumnHandle;
import io.trino.spi.type.Type;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotExpression
{
    private final String fieldName;
    private final String expression;
    private final Type type;
    private final Optional<String> alias;
    private final boolean aggregate;

    public static PinotExpression fromNonAggregateColumnHandle(PinotColumnHandle columnHandle)
    {
        return new PinotExpression(columnHandle.getColumnName(), columnHandle.getColumnName(), columnHandle.getDataType(), Optional.empty(), false);
    }

    public static PinotExpression fromAggregateColumnHandle(PinotColumnHandle columnHandle)
    {
        return new PinotExpression(columnHandle.getColumnName(), columnHandle.getColumnName(), columnHandle.getDataType(), Optional.empty(), true);
    }

    @JsonCreator
    public PinotExpression(@JsonProperty String fieldName,
            @JsonProperty String expression,
            @JsonProperty Type type,
            @JsonProperty Optional<String> alias,
            @JsonProperty boolean aggregate)
    {
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.expression = requireNonNull(expression, "expression is null");
        this.type = requireNonNull(type, "type is null");
        this.alias = requireNonNull(alias, "alias is null");
        this.aggregate = aggregate;
    }

    @JsonProperty
    public String getFieldName()
    {
        return fieldName;
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Optional<String> getAlias()
    {
        return alias;
    }

    @JsonProperty
    public boolean isAggregate()
    {
        return aggregate;
    }

    public String getColumnName()
    {
        return alias.orElse(fieldName);
    }

    public PinotColumnHandle toColumnHandle()
    {
        return new PinotColumnHandle(getColumnName(), type);
    }

    public PinotColumnHandle toAggregateColumnHandle()
    {
        return new PinotColumnHandle(fieldName, type);
    }
}
