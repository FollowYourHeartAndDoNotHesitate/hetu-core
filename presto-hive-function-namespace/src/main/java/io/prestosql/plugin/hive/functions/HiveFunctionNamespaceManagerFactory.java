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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.function.FunctionHandleResolver;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;

import java.util.Map;

import static io.prestosql.plugin.hive.functions.FunctionRegistry.addFunction;
import static io.prestosql.plugin.hive.functions.FunctionRegistry.getCurrentFunctionNames;
import static io.prestosql.plugin.hive.functions.HiveFunctionErrorCode.unsupportedNamespace;
import static io.prestosql.plugin.hive.functions.HiveFunctionNamespacePlugin.EXTERNAL_FUNCTIONS_DIR;
import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
    private static final Logger log = Logger.get(HiveFunctionNamespaceManagerFactory.class);

    private final ClassLoader classLoader;
    private final FunctionHandleResolver functionHandleResolver;

    public static final String NAME = "hive-functions";

    public HiveFunctionNamespaceManagerFactory(ClassLoader classLoader)
    {
        this.classLoader = requireNonNull(classLoader, "classLoader is null");
        this.functionHandleResolver = new HiveFunctionHandleResolver();
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public FunctionHandleResolver getHandleResolver()
    {
        return functionHandleResolver;
    }

    @Override
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config, FunctionNamespaceManagerContext functionNamespaceManagerContext)
    {
        requireNonNull(config, "config is null");

        if (!catalogName.equals("hive")) {
            throw unsupportedNamespace(catalogName);
        }

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            for (String key : config.keySet()) {
                try {
                    if (!getCurrentFunctionNames().contains(key)) {
                        addFunction(key, classLoader.loadClass(config.get(key)));
                    }
                    else {
                        log.warn("Function %s alredy exists.", config.get(key));
                    }
                }
                catch (ClassNotFoundException e) {
                    if (!EXTERNAL_FUNCTIONS_DIR.equals(key)) {
                        log.warn("Invalid parameter %s or class %s not Found.", key, config.get(key));
                    }
                }
            }

            Bootstrap app = new Bootstrap(
                    new HiveFunctionModule(catalogName, classLoader, functionNamespaceManagerContext.getTypeManager().get()));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(FunctionNamespaceManager.class);
        }
        catch (PrestoException e) {
            throw e;
        }
    }
}
