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
import io.airlift.log.Logger;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.function.FunctionNamespaceManagerFactory;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.lang.String.format;

public class HiveFunctionNamespacePlugin
        implements Plugin
{
    private static final Logger log = Logger.get(HiveFunctionNamespacePlugin.class);
    private static final String FUNCTION_PROPERTIES_FILE_PATH = format("etc%sfunction-namespace%shive.properties",
            File.separatorChar, File.separatorChar);
    public static final String EXTERNAL_FUNCTIONS_DIR = "external-functions.dir";

    @Override
    public Iterable<FunctionNamespaceManagerFactory> getFunctionNamespaceManagerFactories()
    {
        return ImmutableList.of(new HiveFunctionNamespaceManagerFactory(getClassLoader()));
    }

    private ClassLoader getClassLoader() {
        File file = new File(FUNCTION_PROPERTIES_FILE_PATH);
        if (!file.exists() || file.length() == 0) {
            log.error("The configuration %s does not exist or is empty. Please check.", FUNCTION_PROPERTIES_FILE_PATH);
        }

        List<URL> urls = new ArrayList<>();
        try {
            Map<String, String> config = loadPropertiesFrom(FUNCTION_PROPERTIES_FILE_PATH);
            urls = getURLs(config.get(EXTERNAL_FUNCTIONS_DIR));
        } catch (IOException e) {
            log.error("Read configuration %s fail.", FUNCTION_PROPERTIES_FILE_PATH);
        }
        return new URLClassLoader(urls.toArray(new URL[urls.size()]), HiveFunctionNamespacePlugin.class.getClassLoader());
    }

    private List<URL> getURLs(String externalFunctionsDir)
    {
        File dir = new File(externalFunctionsDir);
        List<URL> urls = new ArrayList<>();
        String dirName = dir.getName();
        if (!dir.exists() || !dir.isDirectory()) {
            log.debug("%s doesn't exist or is not a directory.", dirName);
            return urls;
        }
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            log.debug("%s is empty.", dirName);
            return urls;
        }
        for (File file : files) {
            try {
                urls.add(file.toURI().toURL());
            }
            catch (MalformedURLException e) {
                log.error("Failed to add %s to URLs of HiveFunctionsClassLoader.", file);
            }
        }
        return urls;
    }
}
