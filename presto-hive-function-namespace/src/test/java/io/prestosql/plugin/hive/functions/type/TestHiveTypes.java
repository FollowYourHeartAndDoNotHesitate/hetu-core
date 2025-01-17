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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.*;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static io.prestosql.spi.type.StandardTypes.*;
import static org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestHiveTypes {
    @Test
    public void testCreateHiveVarCharString() {
        String s = "test";

        Assert.assertEquals(new HiveVarchar(s, s.length()), HiveTypes.createHiveVarChar(s));
    }

    @Test
    public void testCreateHiveChar() {
        String s = "test";

        Assert.assertEquals(new HiveChar(s, s.length()), HiveTypes.createHiveChar(s));
    }

    @Test
    public void testToTypeInfo() {
        Type mockType = mock(Type.class);
        TypeSignature mockTypeSignature = mock(TypeSignature.class);
        when(mockType.getTypeSignature()).thenReturn(mockTypeSignature);

        // case BIGINT
        when(mockTypeSignature.getBase()).thenReturn(BIGINT);
        Assert.assertEquals(longTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case INTEGER
        when(mockTypeSignature.getBase()).thenReturn(INTEGER);
        Assert.assertEquals(intTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case SMALLINT
        when(mockTypeSignature.getBase()).thenReturn(SMALLINT);
        Assert.assertEquals(shortTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case TINYINT
        when(mockTypeSignature.getBase()).thenReturn(TINYINT);
        Assert.assertEquals(byteTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case BOOLEAN
        when(mockTypeSignature.getBase()).thenReturn(BOOLEAN);
        Assert.assertEquals(booleanTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case DATE
        when(mockTypeSignature.getBase()).thenReturn(DATE);
        Assert.assertEquals(dateTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case DECIMAL
        when(mockTypeSignature.getBase()).thenReturn(DECIMAL);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
        // case REAL
        when(mockTypeSignature.getBase()).thenReturn(REAL);
        Assert.assertEquals(floatTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case DOUBLE
        when(mockTypeSignature.getBase()).thenReturn(DOUBLE);
        Assert.assertEquals(doubleTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case TIMESTAMP
        when(mockTypeSignature.getBase()).thenReturn(TIMESTAMP);
        Assert.assertEquals(timestampTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case VARBINARY
        when(mockTypeSignature.getBase()).thenReturn(VARBINARY);
        Assert.assertEquals(binaryTypeInfo, HiveTypes.toTypeInfo(mockType));
        // case DECIMAL
        when(mockTypeSignature.getBase()).thenReturn(VARCHAR);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
        // case CHAR
        when(mockTypeSignature.getBase()).thenReturn(CHAR);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
        // case ROW
        when(mockTypeSignature.getBase()).thenReturn(ROW);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
        // case ARRAY
        when(mockTypeSignature.getBase()).thenReturn(ARRAY);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
        // case MAP
        when(mockTypeSignature.getBase()).thenReturn(MAP);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
        // throw unsupported type
        when(mockTypeSignature.getBase()).thenReturn(GEOMETRY);
        try {
            HiveTypes.toTypeInfo(mockType);
        } catch (PrestoException e) {
            org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", e.getMessage()));
        }
    }

    @Test
    public void testToDecimalTypeInfo() throws IllegalAccessException, NoSuchMethodException, InstantiationException {
        Constructor<HiveTypes> hiveTypesConstructor = HiveTypes.class.getDeclaredConstructor();
        hiveTypesConstructor.setAccessible(true);
        Object instance = null;
        try {
            instance = hiveTypesConstructor.newInstance();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        Class<HiveTypes> hiveTypesClass = HiveTypes.class;
        Method toDecimalTypeInfo = hiveTypesClass.getDeclaredMethod("toDecimalTypeInfo", Type.class);
        toDecimalTypeInfo.setAccessible(true);
        Type mockType = mock(Type.class);

        // type instanceof DecimalType
        DecimalType mockDecimalType = mock(DecimalType.class);
        try {
            toDecimalTypeInfo.invoke(instance, mockDecimalType);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IllegalArgumentException) {
                IllegalArgumentException ex = (IllegalArgumentException) cause;
                org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Decimal precision out of allowed range .*", ex.getMessage()));
            }
        }

        // throw unsupported type
        try {
            toDecimalTypeInfo.invoke(instance, mockType);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PrestoException) {
                PrestoException ex = (PrestoException) cause;
                org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", ex.getMessage()));
            }
        }
    }

    @Test
    public void testToVarcharTypeInfo() throws IllegalAccessException, NoSuchMethodException, InstantiationException, InvocationTargetException {
        Constructor<HiveTypes> hiveTypesConstructor = HiveTypes.class.getDeclaredConstructor();
        hiveTypesConstructor.setAccessible(true);
        Object instance = hiveTypesConstructor.newInstance();

        Class<HiveTypes> hiveTypesClass = HiveTypes.class;
        Method toVarcharTypeInfo = hiveTypesClass.getDeclaredMethod("toVarcharTypeInfo", Type.class);
        toVarcharTypeInfo.setAccessible(true);
        Type mockType = mock(Type.class);

        // throw unsupported type
        try {
            toVarcharTypeInfo.invoke(instance, mockType);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PrestoException) {
                PrestoException ex = (PrestoException) cause;
                org.locationtech.jts.util.Assert.isTrue(Pattern.matches("Unsupported Presto type .*", ex.getMessage()));
            }
        }
    }

    @Test
    public void testToStructTypeInfo() throws IllegalAccessException, NoSuchMethodException, InstantiationException, InvocationTargetException {
        Constructor<HiveTypes> hiveTypesConstructor = HiveTypes.class.getDeclaredConstructor();
        hiveTypesConstructor.setAccessible(true);
        Object instance = hiveTypesConstructor.newInstance();

        Class<HiveTypes> hiveTypesClass = HiveTypes.class;
        Method toStructTypeInfo = hiveTypesClass.getDeclaredMethod("toStructTypeInfo", Type.class);
        toStructTypeInfo.setAccessible(true);

        // throw unsupported RowType
        RowType mockRowType = mock(RowType.class);
        List<RowType.Field> fields = mockRowType.getFields();
        List<String> fieldNames = new ArrayList<>(fields.size());
        List<TypeInfo> fieldTypes = new ArrayList<>(fields.size());

        Assert.assertEquals(getStructTypeInfo(fieldNames, fieldTypes), toStructTypeInfo.invoke(instance, mockRowType));
    }

    @Test
    public void testToListTypeInfo() throws IllegalAccessException, NoSuchMethodException, InstantiationException, InvocationTargetException {
        Constructor<HiveTypes> hiveTypesConstructor = HiveTypes.class.getDeclaredConstructor();
        hiveTypesConstructor.setAccessible(true);
        Object instance = hiveTypesConstructor.newInstance();

        Class<HiveTypes> hiveTypesClass = HiveTypes.class;
        Method toListTypeInfo = hiveTypesClass.getDeclaredMethod("toListTypeInfo", Type.class);
        toListTypeInfo.setAccessible(true);

        // type instanceof ArrayType
        ArrayType mockArrayType = mock(ArrayType.class);
        try {
            toListTypeInfo.invoke(instance, mockArrayType);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            Assert.assertEquals(true, cause instanceof NullPointerException);
        }
    }

    @Test
    public void testToMapTypeInfo() throws IllegalAccessException, NoSuchMethodException, InstantiationException, InvocationTargetException {
        Constructor<HiveTypes> hiveTypesConstructor = HiveTypes.class.getDeclaredConstructor();
        hiveTypesConstructor.setAccessible(true);
        Object instance = hiveTypesConstructor.newInstance();

        Class<HiveTypes> hiveTypesClass = HiveTypes.class;
        Method toMapTypeInfo = hiveTypesClass.getDeclaredMethod("toMapTypeInfo", Type.class);
        toMapTypeInfo.setAccessible(true);

        // type instanceof MapType
        MapType mockMapType = mock(MapType.class);
        try {
            toMapTypeInfo.invoke(instance, mockMapType);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            Assert.assertEquals(true, cause instanceof NullPointerException);
        }
    }
}
