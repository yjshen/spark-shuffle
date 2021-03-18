package org.apache.spark.network.shuffle;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class AppStatusTestMain {

    private static final Logger logger = LoggerFactory.getLogger(AppStatusTestMain.class);

    public static boolean appFinished(String appId) {
        ObjectMapper om = new ObjectMapper();
        int count = 0;

        while (true) {
            try {
                URL appStatusUrl = new URL("http://10.48.56.8:8088" + "/ws/v1/cluster/apps/" + appId + "/state");
                HttpURLConnection conn = (HttpURLConnection) appStatusUrl.openConnection();
                conn.setRequestMethod("GET");
                if (conn.getResponseCode() == 200) {
                    Map<String, Object> stateMap = om.readValue(conn.getInputStream(), Map.class);
                    String state = (String) stateMap.get("state");
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
                if (++count == 3 /* maxTries */) return false;
            }
        }
    }

    public static void main(String[] args) {
        String appId = args[0];
        System.out.println(appId + " is running " + appFinished(appId));
    }
}
