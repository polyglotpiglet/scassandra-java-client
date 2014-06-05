/*
 * Copyright (C) 2014 Christopher Batey
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.scassandra;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;

class ScassandraRunner implements Scassandra {

    private final ServerStubRunner serverStubRunner;
    private final PrimingClient primingClient;
    private final ActivityClient activityClient;
    private final int binaryPort;
    private final int adminPort;
    private final String versionurl;
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final Gson gson = new Gson();

    ScassandraRunner(String binaryListenAddress, int binaryPort, String adminListenAddress, int adminPort) {
        this.binaryPort = binaryPort;
        this.adminPort = adminPort;
        this.serverStubRunner = new ServerStubRunner(binaryListenAddress, binaryPort, adminListenAddress, adminPort);
        this.primingClient = PrimingClient.builder().withPort(adminPort).build();
        this.activityClient = ActivityClient.builder().withPort(adminPort).build();
        this.versionurl = "http://" + binaryListenAddress + ":" + adminPort + "/version";

    }

    @Override
    public PrimingClient primingClient() {
        return this.primingClient;
    }

    @Override
    public ActivityClient activityClient() {
        return this.activityClient;
    }

    @Override
    public void start() {
        serverStubRunner.start();
        serverStubRunner.awaitStartup();
    }

    @Override
    public void stop() {
        serverStubRunner.shutdown();
    }

    @Override
    public int getAdminPort() {
        return adminPort;
    }

    @Override
    public int getBinaryPort() {
        return binaryPort;
    }

    @Override
    public String serverVersion() {
        HttpGet get = new HttpGet(versionurl);
        try {
            Type type = new TypeToken<Map<String, String>>(){}.getType();
            Map<String, String> version = gson.fromJson(EntityUtils.toString(httpClient.execute(get).getEntity()), type);
            return version.get("version");
        } catch (IOException e) {
            throw new RuntimeException("Unable to get version", e);
        }
    }

}


