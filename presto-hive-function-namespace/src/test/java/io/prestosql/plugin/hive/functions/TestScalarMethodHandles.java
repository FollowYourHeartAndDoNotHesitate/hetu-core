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

import com.google.inject.Key;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.functions.scalar.ScalarFunctionInvoker;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ByteArrayBlock;
import io.prestosql.spi.block.MapBlock;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.TestRowType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.prestosql.plugin.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static io.prestosql.plugin.hive.functions.gen.ScalarMethodHandles.generateUnbound;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestScalarMethodHandles
{
    private AtomicInteger number;
    private TestingPrestoServer server;
    private TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.number = new AtomicInteger(0);
        this.server = createTestingPrestoServer();
        this.typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    @Test
    public void generateUnboundTest()
            throws Throwable
    {
        runCase("boolean", tokens("boolean"), true, false);
        runCase("bigint", tokens("bigint,bigint"), 3L, 2L, 1L);
        runCase("varchar", tokens("varchar,double"), Slices.utf8Slice("output"), Slices.utf8Slice("input"), 2.1);
        runCase("array(bigint)", tokens("bigint,bigint"), createArrayBlock(), 2L, 1L);
//        runCase("map(varchar,varchar)", tokens("varchar,varchar"), createMapBlock(), Slices.utf8Slice("a"), Slices.utf8Slice("b"));
    }

    private void runCase(String resultType, String[] argumentTypes, Object result, Object... arguments)
            throws Throwable
    {
        Signature signature = createScalarSignature(resultType, argumentTypes);
        MethodHandle methodHandle = generateUnbound(signature, typeManager);
        TestingScalarFunctionInvoker invoker = new TestingScalarFunctionInvoker(signature, result);
        final Object output;
        if (arguments.length == 1) {
            output = methodHandle.invoke(invoker, arguments[0]);
        }
        else if (arguments.length == 2) {
            output = methodHandle.invoke(invoker, arguments[0], arguments[1]);
        }
        else if (arguments.length == 3) {
            output = methodHandle.invoke(invoker, arguments[0], arguments[1], arguments[2]);
        }
        else if (arguments.length == 4) {
            output = methodHandle.invoke(invoker, arguments[0], arguments[1], arguments[2], arguments[3]);
        }
        else {
            throw new RuntimeException("Not supported yet");
        }
        Object[] inputs = invoker.getInputs();

        assertEquals(output, result);
        assertEquals(inputs, arguments);
    }

    private String[] tokens(String s)
    {
        return s.split(",");
    }

    private Signature createScalarSignature(String returnType, String... argumentTypes)
    {
        return new Signature(QualifiedObjectName.valueOf("hive.default.testing_" + number.incrementAndGet()),
                FunctionKind.SCALAR,
                parseTypeSignature(returnType),
                Stream.of(argumentTypes)
                        .map(TypeSignature::parseTypeSignature)
                        .toArray(TypeSignature[]::new));
    }

    private static class TestingScalarFunctionInvoker
            implements ScalarFunctionInvoker
    {
        private final Signature signature;
        private final Object result;
        private Object[] inputs;

        public TestingScalarFunctionInvoker(Signature signature, Object result)
        {
            this.signature = signature;
            this.result = result;
        }

        @Override
        public Signature getSignature()
        {
            return signature;
        }

        @Override
        public Object evaluate(Object... inputs)
        {
            this.inputs = inputs;
            return result;
        }

        public Object[] getInputs()
        {
            return inputs;
        }
    }

    private Block createEmptyBlock()
    {
        return new ByteArrayBlock(0, Optional.empty(), new byte[0]);
    }

    private Block createArrayBlock()
    {
        Block emptyValueBlock = createEmptyBlock();
        return ArrayBlock.fromElementBlock(1, Optional.empty(), IntStream.range(0, 2).toArray(), emptyValueBlock);
    }

    private Block createMapBlock()
    {
        Block emptyKeyBlock = createEmptyBlock();
        Block emptyValueBlock = createEmptyBlock();
        MapType mapType = new MapType(
                VARCHAR,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"));
        return MapBlock.fromKeyValueBlock(Optional.empty(), IntStream.range(0, 2).toArray(), emptyKeyBlock, emptyValueBlock, mapType);
    }
}
