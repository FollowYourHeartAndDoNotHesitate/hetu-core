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
import io.prestosql.spi.classloader.ThreadContextClassLoader;
import io.prestosql.spi.function.FunctionHandleResolver;
import io.prestosql.spi.function.FunctionNamespaceManager;
import io.prestosql.spi.function.FunctionNamespaceManagerContext;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class HiveFunctionNamespaceManagerFactory
        implements FunctionNamespaceManagerFactory
{
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
    public FunctionNamespaceManager<?> create(String catalogName, Map<String, String> config, FunctionNamespaceManagerContext functionNamespaceManagerContext, TypeManager typeManager)
    {
        requireNonNull(config, "config is null");

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(classLoader)) {
            Bootstrap app = new Bootstrap(
                    new HiveFunctionModule(catalogName, classLoader, typeManager));

            Injector injector = app
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(config)
                    .quiet()
                    .initialize();
            return injector.getInstance(FunctionNamespaceManager.class);
        }
    }
}
