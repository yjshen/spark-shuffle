package org.apache.spark.network.shuffle;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;

/**
 * Simply encodes an executor's full ID, which is appId + execId.
 */
public class AppExecId {
    public final String appId;
    public final String execId;

    @JsonCreator
    public AppExecId(@JsonProperty("appId") String appId, @JsonProperty("execId") String execId) {
        this.appId = appId;
        this.execId = execId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AppExecId appExecId = (AppExecId) o;
        return Objects.equal(appId, appExecId.appId) && Objects.equal(execId, appExecId.execId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(appId, execId);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
            .add("appId", appId)
            .add("execId", execId)
            .toString();
    }
}