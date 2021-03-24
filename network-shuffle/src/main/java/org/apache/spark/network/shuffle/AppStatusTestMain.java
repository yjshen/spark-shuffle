package org.apache.spark.network.shuffle;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class AppStatusTestMain {

    private static final Logger logger = LoggerFactory.getLogger(AppStatusTestMain.class);

    public static boolean appFinished(String appId) {
        ObjectMapper om = new ObjectMapper();
        int count = 0;

        while (true) {
            try {
                URL appStatusUrl = new URL("http://0.0.0.0:8042/ws/v1/node/apps/" + appId + "?state");
                HttpURLConnection conn = (HttpURLConnection) appStatusUrl.openConnection();
                conn.setRequestMethod("GET");
                if (conn.getResponseCode() == 200) {
                    JsonNode node = om.readTree(conn.getInputStream());
                    String state = node.at("/app/state").asText();
                    logger.info("Get app status {} periodically, this time the state is {}", appId, state);
                    if ("FINISHED".equals(state) || "FAILED".equals(state) || "KILLED".equals(state)) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    throw new IOException("Get failed");
                }
            } catch (Exception e) {
                if (++count == 3 /* maxTries */) return true;
            }
        }
    }

    public static void main(String[] args) {
        String appId = args[0];
        System.out.println(appId + " is finished " + appFinished(appId));
    }
}
