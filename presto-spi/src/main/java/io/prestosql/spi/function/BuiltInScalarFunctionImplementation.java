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
package io.prestosql.spi.function;

import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

public final class BuiltInScalarFunctionImplementation
        implements ScalarFunctionImplementation
{
    private final List<ScalarImplementationChoice> choices;

    public BuiltInScalarFunctionImplementation(
            boolean nullable,
            List<ScalarImplementationChoice.ArgumentProperty> argumentProperties,
            MethodHandle methodHandle)
    {
        this(
                nullable,
                argumentProperties,
                methodHandle,
                Optional.empty());
    }

    public BuiltInScalarFunctionImplementation(
            boolean nullable,
            List<ScalarImplementationChoice.ArgumentProperty> argumentProperties,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(
                ImmutableList.of(new ScalarImplementationChoice(
                        nullable,
                        argumentProperties,
                        ScalarImplementationChoice.ReturnPlaceConvention.STACK,
                        methodHandle,
                        instanceFactory)));
    }

    /**
     * Creates a ScalarFunctionImplementation consisting of one or more choices.
     * <p>
     * All choices must have the same SQL signature, and are equivalent in what they do.
     * The first choice is the default choice, which is the one used for legacy access methods.
     * The default choice must be usable under any context. (e.g. it must not use BLOCK_POSITION convention.)
     *
     * @param choices the list of choices, ordered from generic to specific
     */
    public BuiltInScalarFunctionImplementation(List<ScalarImplementationChoice> choices)
    {
        checkArgument(!choices.isEmpty(), "choices is an empty list");
        this.choices = ImmutableList.copyOf(choices);
    }

    public boolean isNullable()
    {
        return choices.get(0).isNullable();
    }

    public ScalarImplementationChoice.ArgumentProperty getArgumentProperty(int argumentIndex)
    {
        return choices.get(0).getArgumentProperties().get(argumentIndex);
    }

    @Override
    public MethodHandle getMethodHandle()
    {
        return choices.get(0).getMethodHandle();
    }

    public Optional<MethodHandle> getInstanceFactory()
    {
        return choices.get(0).getInstanceFactory();
    }

    public List<ScalarImplementationChoice> getAllChoices()
    {
        return choices;
    }
}
