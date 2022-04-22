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
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static io.prestosql.client.ClientStandardTypes.ROW;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.VARBINARY;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestObjectInspectors {
    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Unsupported Presto type.*")
    public void testCreate() {
        TypeManager mockTypeManager = mock(TypeManager.class);
        Type mockType = mock(Type.class);
        TypeSignature mockTypeSignature = mock(TypeSignature.class);
        when(mockType.getTypeSignature()).thenReturn(mockTypeSignature);

        // case BOOLEAN
        when(mockTypeSignature.getBase()).thenReturn(BOOLEAN);
        Assert.assertEquals(javaBooleanObjectInspector, ObjectInspectors.create(mockType, mockTypeManager));
        // case BOOLEAN
        when(mockTypeSignature.getBase()).thenReturn(VARBINARY);
        Assert.assertEquals(javaByteArrayObjectInspector, ObjectInspectors.create(mockType, mockTypeManager));
        // throw unsupported type
        when(mockTypeSignature.getBase()).thenReturn(ROW);
        Assert.assertEquals(javaByteArrayObjectInspector, ObjectInspectors.create(mockType, mockTypeManager));
    }

    @Test()
    public void testCreateForRow() throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Constructor<ObjectInspectors> objectInspectorsConstructor = ObjectInspectors.class.getDeclaredConstructor();
        objectInspectorsConstructor.setAccessible(true);
        Object instance = objectInspectorsConstructor.newInstance();
        Class<ObjectInspectors> objectInspectorsClass = ObjectInspectors.class;
        Method method = objectInspectorsClass.getDeclaredMethod("createForRow", RowType.class, TypeManager.class);
        method.setAccessible(true);

        RowType mockRowType = mock(RowType.class);
        TypeManager mockTypeManager = mock(TypeManager.class);

        Object result = method.invoke(instance, mockRowType, mockTypeManager).toString();

        Assert.assertEquals("org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector<>", result);
    }
}
