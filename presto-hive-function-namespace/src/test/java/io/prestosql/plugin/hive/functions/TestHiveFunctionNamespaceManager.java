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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.*;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignature;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestHiveFunctionNamespaceManager {
    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Cannot create function in hive function namespace: test.test.test")
    public void testCreateFunction() {
        String catalogName = "hive";
        HiveFunctionNamespacePlugin mockHiveFunctionNamespaceManager = mock(HiveFunctionNamespacePlugin.class);
        ClassLoader mockClassLoader = mockHiveFunctionNamespaceManager.getClass().getClassLoader();
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        TypeManager mockTypeManager = mock(TypeManager.class);
        HiveFunctionNamespaceManager hiveFunctionNamespaceManager = new HiveFunctionNamespaceManager(catalogName, mockClassLoader, mockHiveFunctionRegistry, mockTypeManager);
        SqlInvokedFunction mockSqlInvokedFunction = mock(SqlInvokedFunction.class);
        Signature signature = new Signature(
                QualifiedObjectName.valueOf("test.test.test"),
                FunctionKind.AGGREGATE,
                DoubleType.DOUBLE.getTypeSignature(),
                ImmutableList.of(DoubleType.DOUBLE.getTypeSignature()));
        when(mockSqlInvokedFunction.getSignature()).thenReturn(signature);

        hiveFunctionNamespaceManager.createFunction(mockSqlInvokedFunction, false);
    }

    @Test()
    public void testListFunctions() {
        String catalogName = "hive";
        HiveFunctionNamespacePlugin mockHiveFunctionNamespaceManager = mock(HiveFunctionNamespacePlugin.class);
        ClassLoader mockClassLoader = mockHiveFunctionNamespaceManager.getClass().getClassLoader();
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        TypeManager mockTypeManager = mock(TypeManager.class);
        HiveFunctionNamespaceManager hiveFunctionNamespaceManager = new HiveFunctionNamespaceManager(catalogName, mockClassLoader, mockHiveFunctionRegistry, mockTypeManager);

        Collection<HiveFunction> result = hiveFunctionNamespaceManager.listFunctions();

        assertEquals(0, result.size());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "argumentTypes is null")
    public void testGetFunctions() {
        String catalogName = "hive";
        HiveFunctionNamespacePlugin mockHiveFunctionNamespaceManager = mock(HiveFunctionNamespacePlugin.class);
        ClassLoader mockClassLoader = mockHiveFunctionNamespaceManager.getClass().getClassLoader();
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        TypeManager mockTypeManager = mock(TypeManager.class);
        HiveFunctionNamespaceManager hiveFunctionNamespaceManager = new HiveFunctionNamespaceManager(catalogName, mockClassLoader, mockHiveFunctionRegistry, mockTypeManager);
        QualifiedObjectName mockFunctionName = mock(QualifiedObjectName.class);

        hiveFunctionNamespaceManager.getFunctions(Optional.empty(), mockFunctionName, null);
    }

    @Test
    public void testGetFunctionHandle() {
        String catalogName = "hive";
        HiveFunctionNamespacePlugin mockHiveFunctionNamespaceManager = mock(HiveFunctionNamespacePlugin.class);
        ClassLoader mockClassLoader = mockHiveFunctionNamespaceManager.getClass().getClassLoader();
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        TypeManager mockTypeManager = mock(TypeManager.class);
        HiveFunctionNamespaceManager hiveFunctionNamespaceManager = new HiveFunctionNamespaceManager(catalogName, mockClassLoader, mockHiveFunctionRegistry, mockTypeManager);
        QualifiedObjectName mockName = mock(QualifiedObjectName.class);
        TypeSignature mockReturnType = mock(TypeSignature.class);
        List<TypeSignature> argumentTypes = new ArrayList<>();
        Signature signature = new Signature(mockName, SCALAR, emptyList(), emptyList(), mockReturnType, argumentTypes, false);

        Assert.assertEquals(hiveFunctionNamespaceManager.getFunctionHandle(Optional.empty(), signature), new BuiltInFunctionHandle(signature));
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "Execute function is not supported")
    public void testExecuteFunction() {
        String catalogName = "hive";
        HiveFunctionNamespacePlugin mockHiveFunctionNamespaceManager = mock(HiveFunctionNamespacePlugin.class);
        ClassLoader mockClassLoader = mockHiveFunctionNamespaceManager.getClass().getClassLoader();
        HiveFunctionRegistry mockHiveFunctionRegistry = mock(HiveFunctionRegistry.class);
        TypeManager mockTypeManager = mock(TypeManager.class);
        HiveFunctionNamespaceManager hiveFunctionNamespaceManager = new HiveFunctionNamespaceManager(catalogName, mockClassLoader, mockHiveFunctionRegistry, mockTypeManager);
        FunctionHandle mockFunctionHandle = mock(FunctionHandle.class);
        Page input = new Page(1);
        List<Integer> channels = new ArrayList<>();

        hiveFunctionNamespaceManager.executeFunction(mockFunctionHandle, input, channels, mockTypeManager);
    }
}