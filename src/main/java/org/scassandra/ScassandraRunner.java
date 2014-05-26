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

import org.scassandra.http.client.ActivityClient;
import org.scassandra.http.client.PrimingClient;

class ScassandraRunner implements Scassandra {

    private final ServerStubRunner serverStubRunner;
    private final PrimingClient primingClient;
    private final ActivityClient activityClient;
    private final int binaryPort;
    private final int adminPort;

    ScassandraRunner(int binaryPort, int adminPort) {
        this.binaryPort = binaryPort;
        this.adminPort = adminPort;
        this.serverStubRunner = new ServerStubRunner(binaryPort, adminPort);
        this.primingClient = PrimingClient.builder().withPort(adminPort).build();
        this.activityClient = ActivityClient.builder().withPort(adminPort).build();

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
        // The above start is async. Once scassandra offers a way to block until it is ready
        // we can remove this sleep.
        // See https://github.com/scassandra/scassandra-server/issues/10
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
        }
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

}


