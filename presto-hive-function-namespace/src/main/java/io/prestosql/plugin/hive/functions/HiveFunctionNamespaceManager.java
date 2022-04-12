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

package io.prestosql.plugin.hive.functions;

import io.prestosql.plugin.hive.functions.scalar.HiveScalarFunction;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceTransactionHandle;
import io.prestosql.spi.function.ScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.function.SqlInvokedFunction;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import static io.prestosql.plugin.hive.functions.FunctionRegistry.getCurrentFunctionNames;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.functionNotFound;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.unsupportedFunctionType;
import static io.prestosql.plugin.hive.functions.scalar.HiveScalarFunction.createHiveScalarFunction;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManager
        implements FunctionNamespaceManager<HiveFunction>
{
    public static final String ID = "hive-functions";

    private final String catalogName;
    private final ClassLoader classLoader;
    private final LoadingCache<FunctionKey, HiveFunction> functions;
    private final HiveFunctionRegistry hiveFunctionRegistry;
    private final TypeManager typeManager;

    @Inject
    public HiveFunctionNamespaceManager(
            @ForHiveFunction String catalogName,
            @ForHiveFunction ClassLoader classLoader,
            HiveFunctionRegistry hiveFunctionRegistry,
            TypeManager typeManager)
    {
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
        this.hiveFunctionRegistry = requireNonNull(hiveFunctionRegistry, "hiveFunctionRegistry is null");
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.functions = CacheBuilder.newBuilder().maximumSize(1000)
                .build(CacheLoader.from(this::initializeFunction));
    }

    /**
     * this function namespace manager does not support transaction.
     */
    public FunctionNamespaceTransactionHandle beginTransaction()
    {
        return new EmptyTransactionHandle();
    }

    public void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    public void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new IllegalStateException(format("Cannot create function in hive function namespace: %s", function.getSignature().getName()));
    }

    @Override
    public List<HiveFunction> listFunctions()
    {
        return getCurrentFunctionNames().stream().map(functionName -> createDummyHiveScalarFunction(functionName)).collect(toImmutableList());
    }

    public List<HiveFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName, List<TypeSignature> parameterTypes)
    {
        List<HiveFunction> hiveFunctions = new ArrayList<>();
        hiveFunctions.add(initializeFunction(FunctionKey.of(functionName, parameterTypes)));
        return hiveFunctions;
    }

    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        return new HiveFunctionHandle(signature);
    }

    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof HiveFunctionHandle);
        HiveFunction function = functions.getUnchecked(FunctionKey.from((HiveFunctionHandle) functionHandle));
        return function.getFunctionMetadata();
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof HiveFunctionHandle);
        HiveFunction function = functions.getUnchecked(FunctionKey.from((HiveFunctionHandle) functionHandle));
        checkState(function instanceof HiveScalarFunction);
        return ((HiveScalarFunction) function).getJavaScalarFunctionImplementation();
    }

    @Override
    public CompletableFuture<Block> executeFunction(FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        throw new IllegalStateException("Execute function is not supported");
    }

    private HiveFunction initializeFunction(FunctionKey key)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            QualifiedObjectName name = key.getName();
            try {
                Class<?> functionClass = hiveFunctionRegistry.getClass(name);
                if (anyAssignableFrom(functionClass, GenericUDF.class, UDF.class)) {
                    return createHiveScalarFunction(functionClass, name, key.getArgumentTypes(), typeManager);
                }
                else {
                    throw unsupportedFunctionType(functionClass);
                }
            }
            catch (ClassNotFoundException e) {
                throw functionNotFound(name.toString());
            }
        }
    }

    private HiveFunction createDummyHiveScalarFunction(String functionName)
    {
        QualifiedObjectName hiveFunctionName = QualifiedObjectName.valueOf(catalogName, "default", functionName);
        Signature signature = new Signature(hiveFunctionName, SCALAR, TypeSignature.parseTypeSignature("T"));
        return new DummyHiveScalarFunction(signature);
    }

    private static boolean anyAssignableFrom(Class<?> cls, Class<?>... supers)
    {
        return Stream.of(supers).anyMatch(s -> s.isAssignableFrom(cls));
    }

    private static class DummyHiveScalarFunction
            extends HiveFunction
    {
        public DummyHiveScalarFunction(Signature signature)
        {
            super(signature.getName(), signature, false, true, true, "");
        }

        @Override
        public FunctionMetadata getFunctionMetadata()
        {
            throw new IllegalStateException("Get function metadata is not supported");
        }
    }

    private static class EmptyTransactionHandle
            implements FunctionNamespaceTransactionHandle
    {
    }

    private static class FunctionKey
    {
        private final QualifiedObjectName name;
        private final List<TypeSignature> argumentTypes;

        public static FunctionKey from(HiveFunctionHandle handle)
        {
            Signature signature = handle.getSignature();
            return FunctionKey.of(signature.getName(), signature.getArgumentTypes());
        }

        private static FunctionKey of(QualifiedObjectName name, List<TypeSignature> argumentTypes)
        {
            return new FunctionKey(name, argumentTypes);
        }

        private FunctionKey(QualifiedObjectName name, List<TypeSignature> argumentTypes)
        {
            this.name = requireNonNull(name, "name is null");
            this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        }

        public QualifiedObjectName getName()
        {
            return name;
        }

        public List<TypeSignature> getArgumentTypes()
        {
            return argumentTypes;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FunctionKey that = (FunctionKey) o;
            return Objects.equals(name, that.name) &&
                    Objects.equals(argumentTypes, that.argumentTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, argumentTypes);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("name", name)
                    .add("arguments", argumentTypes)
                    .toString();
        }
    }
}
