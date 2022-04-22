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

import io.prestosql.spi.PrestoException;
import org.testng.Assert;
import org.testng.annotations.Test;

import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_EXECUTION_ERROR;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.HIVE_FUNCTION_INITIALIZATION_ERROR;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;
import static org.mockito.Mockito.mock;

public class TestHiveFunctionErrorCode {
    @Test
    public void testFunctionNotFound() {
        String name = "test";

        PrestoException result = HiveFunctionErrorCode.functionNotFound(name);

        Assert.assertEquals(result.getErrorCode().toString().startsWith(FUNCTION_NOT_FOUND.toString()), true);
        Assert.assertEquals(result.getMessage(), format("Function %s not registered", name));
    }

    @Test
    public void testInitializationError() {
        Throwable mockThrowable = mock(Throwable.class);

        PrestoException result = HiveFunctionErrorCode.initializationError(mockThrowable);

        Assert.assertEquals(result.getMessage(), HIVE_FUNCTION_INITIALIZATION_ERROR.toString());
    }

    @Test
    public void testExecutionError() {
        Throwable mockThrowable = mock(Throwable.class);

        PrestoException result = HiveFunctionErrorCode.executionError(mockThrowable);

        Assert.assertEquals(result.getMessage(), HIVE_FUNCTION_EXECUTION_ERROR.toString());
    }
}
