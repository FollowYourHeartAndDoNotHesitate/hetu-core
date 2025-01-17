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

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.TypeSignature;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;

class TmpHiveFunction extends HiveFunction {

    public TmpHiveFunction(QualifiedObjectName name, Signature signature, boolean hidden, boolean deterministic, boolean calledOnNullInput, String description) {
        super(name, signature, hidden, deterministic, calledOnNullInput, description);
    }

    @Override
    public FunctionMetadata getFunctionMetadata() {
        return null;
    }
}

public class TestHiveFunction {
    @Test()
    public void testGetName() {
        QualifiedObjectName mockName = mock(QualifiedObjectName.class);
        TypeSignature mockReturnType = mock(TypeSignature.class);
        List<TypeSignature> argumentTypes = new ArrayList<>();
        Signature signature = new Signature(mockName, SCALAR, emptyList(), emptyList(), mockReturnType, argumentTypes, false);
        TmpHiveFunction tmpHiveFunction = new TmpHiveFunction(mockName, signature, false, false, false, "test");

        Assert.assertEquals(mockName, tmpHiveFunction.getName());
        Assert.assertEquals(signature, tmpHiveFunction.getSignature());
        Assert.assertEquals(false, tmpHiveFunction.isDeterministic());
        Assert.assertEquals(false, tmpHiveFunction.isCalledOnNullInput());
        Assert.assertEquals("test", tmpHiveFunction.getDescription());
    }
}
