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

import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFMd5;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFAbs;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFLag;

import java.util.Set;

public final class FunctionRegistry
{
    private FunctionRegistry() {}

    // registry for hive functions
    private static final Registry system = new Registry(true);

    static {
        // register genericUDF
        system.registerGenericUDF("abs", GenericUDFAbs.class);
        system.registerGenericUDF("lag", GenericUDFLag.class);
        system.registerUDF("Md5", UDFMd5.class, false);
    }

    public static FunctionInfo getFunctionInfo(String functionName) throws SemanticException
    {
        return system.getFunctionInfo(functionName);
    }

    public static Set<String> getCurrentFunctionNames()
    {
        return system.getCurrentFunctionNames();
    }
}
