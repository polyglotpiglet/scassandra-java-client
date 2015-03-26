package org.scassandra.http.client;

import java.util.Map;

abstract class Config {
   abstract Map<String, ?> getProperties();
}
