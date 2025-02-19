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
package io.trino.json.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static java.util.Objects.requireNonNull;

public class IrConjunctionPredicate
        extends IrPredicate
{
    private final IrPredicate left;
    private final IrPredicate right;

    @JsonCreator
    public IrConjunctionPredicate(@JsonProperty("left") IrPredicate left, @JsonProperty("right") IrPredicate right)
    {
        super();
        this.left = requireNonNull(left, "left is null");
        this.right = requireNonNull(right, "right is null");
    }

    @Override
    protected <R, C> R accept(IrJsonPathVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrConjunctionPredicate(this, context);
    }

    @JsonProperty
    public IrPathNode getLeft()
    {
        return left;
    }

    @JsonProperty
    public IrPathNode getRight()
    {
        return right;
    }
}
