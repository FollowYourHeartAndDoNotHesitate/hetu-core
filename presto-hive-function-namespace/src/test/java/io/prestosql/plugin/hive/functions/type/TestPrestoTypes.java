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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.UNION;
import static org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPrestoTypes {

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type.*")
    public void testFromObjectInspector() {
        TypeManager typeManager = mock(TypeManager.class);
        ObjectInspector mcokObjectInspector = mock(ObjectInspector.class);
        when(mcokObjectInspector.getCategory()).thenReturn(UNION);

        PrestoTypes.fromObjectInspector(mcokObjectInspector, typeManager);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unsupported Hive type.*")
    public void testFromPrimitive() throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Constructor<PrestoTypes> prestoTypesConstructor = PrestoTypes.class.getDeclaredConstructor();
        prestoTypesConstructor.setAccessible(true);
        Object instance = prestoTypesConstructor.newInstance();
        Class<PrestoTypes> prestoTypesClass = PrestoTypes.class;
        Method method = prestoTypesClass.getDeclaredMethod("fromPrimitive", PrimitiveObjectInspector.class);
        method.setAccessible(true);
        PrimitiveObjectInspector mockPrimitiveObjectInspector = mock(PrimitiveObjectInspector.class);

        // case BYTE
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(BYTE);
        Assert.assertEquals(TinyintType.TINYINT, method.invoke(instance, mockPrimitiveObjectInspector));
        // case SHORT
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(SHORT);
        Assert.assertEquals(SmallintType.SMALLINT, method.invoke(instance, mockPrimitiveObjectInspector));
        // case FLOAT
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(FLOAT);
        Assert.assertEquals(RealType.REAL, method.invoke(instance, mockPrimitiveObjectInspector));
        // case DATE
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(DATE);
        Assert.assertEquals(DateType.DATE, method.invoke(instance, mockPrimitiveObjectInspector));
        // case TIMESTAMP
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(TIMESTAMP);
        Assert.assertEquals(TimestampType.TIMESTAMP, method.invoke(instance, mockPrimitiveObjectInspector));
        // case BINARY
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(BINARY);
        Assert.assertEquals(VarbinaryType.VARBINARY, method.invoke(instance, mockPrimitiveObjectInspector));
        // throw unsupported type
        when(mockPrimitiveObjectInspector.getPrimitiveCategory()).thenReturn(UNKNOWN);
        try {
            method.invoke(instance, mockPrimitiveObjectInspector);
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (cause instanceof PrestoException) {
                PrestoException ex = (PrestoException) cause;
                throw new PrestoException(HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE, ex.getMessage());
            }
        }
    }
}
