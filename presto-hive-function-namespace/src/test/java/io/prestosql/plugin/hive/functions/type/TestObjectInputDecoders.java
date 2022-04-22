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

package io.prestosql.plugin.hive.functions.type;

import com.google.inject.Key;
import io.airlift.slice.Slices;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.TestRowType;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Date;
import java.util.*;

import static io.prestosql.plugin.hive.functions.HiveFunctionsTestUtils.createTestingPrestoServer;
import static io.prestosql.plugin.hive.functions.type.ObjectInputDecoders.createDecoder;
import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.Decimals.parseIncludeLeadingZerosInPrecision;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestObjectInputDecoders
{
    private TestingPrestoServer server;
    private TypeManager typeManager;

    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = createTestingPrestoServer();
        this.typeManager = server.getInstance(Key.get(TypeManager.class));
    }

    @Test
    public void testToDate()
    {
        Date date = DateTimeUtils.createDate(18380L);
        assertEquals(date.getYear(), 2020 - 1900);
        assertEquals(date.getMonth(), 4 - 1);
        assertEquals(date.getDate(), 28);
    }

    @Test
    public void testPrimitiveObjectDecoders()
    {
        ObjectInputDecoder decoder;

        decoder = createDecoder(BIGINT, typeManager);
        assertTrue(decoder.decode(123456L) instanceof Long);

        decoder = createDecoder(INTEGER, typeManager);
        assertTrue(decoder.decode(12345L) instanceof Integer);

        decoder = createDecoder(SMALLINT, typeManager);
        assertTrue(decoder.decode(1234L) instanceof Short);

        decoder = createDecoder(TINYINT, typeManager);
        assertTrue(decoder.decode(123L) instanceof Byte);

        decoder = createDecoder(BOOLEAN, typeManager);
        assertTrue(decoder.decode(true) instanceof Boolean);

        decoder = createDecoder(REAL, typeManager);
        assertTrue(decoder.decode(((float) 0.2)) instanceof Float);

        decoder = createDecoder(DOUBLE, typeManager);
        assertTrue(decoder.decode(0.1) instanceof Double);
    }

    @Test
    public void testDecimalObjectDecoders()
    {
        ObjectInputDecoder decoder;

        // short decimal
        decoder = createDecoder(createDecimalType(11, 10), typeManager);
        assertTrue(decoder.decode(decimal("1.2345678910")) instanceof HiveDecimal);

        // long decimal
        decoder = createDecoder(createDecimalType(34, 33), typeManager);
        assertTrue(decoder.decode(decimal("1.281734081274028174012432412423134")) instanceof HiveDecimal);
    }

    @Test
    public void testSliceObjectDecoders()
    {
        ObjectInputDecoder decoder;

        decoder = createDecoder(VARBINARY, typeManager);
        assertTrue(decoder.decode(Slices.wrappedBuffer(new byte[] {12, 34, 56})) instanceof byte[]);

        decoder = createDecoder(VARCHAR, typeManager);
        assertTrue(decoder.decode(Slices.utf8Slice("test_varchar")) instanceof String);

        decoder = createDecoder(createCharType(10), typeManager);
        assertTrue(decoder.decode(Slices.utf8Slice("test_char")) instanceof String);
    }

    @Test
    public void testBlockObjectDecoders()
    {
        ObjectInputDecoder decoder;

        decoder = createDecoder(new ArrayType(BIGINT), typeManager);
        assertTrue(decoder instanceof ObjectInputDecoders.ArrayObjectInputDecoder);
        assertEquals(((ArrayList) decoder.decode(createLongArrayBlock())).get(0), 2L);

        decoder = createDecoder(new MapType(
                BIGINT,
                BIGINT,
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation"),
                methodHandle(TestRowType.class, "throwUnsupportedOperation")), typeManager);
        assertTrue(decoder instanceof ObjectInputDecoders.MapObjectInputDecoder);
        HashMap map = (HashMap) decoder.decode(createLongArrayBlock());
        assertEquals(map.get(2L), 1L);
    }

    private Block createLongArrayBlock()
    {
        return new LongArrayBlock(2, Optional.empty(), new long[] {2L, 1L});
    }

    private Object decimal(String decimalString)
    {
        return parseIncludeLeadingZerosInPrecision(decimalString).getObject();
    }
}
