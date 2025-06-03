import { config } from "@/config";
import { newTracker } from "@snowplow/node-tracker";

if (!config.tracking.SNOWPLOW_COLLECTOR_URL) {
  throw "No Snowplow collector URL configured.";
}

export const snowplowTracker = newTracker(
  { appId: config.tracking.SNOWPLOW_APP_ID, namespace: "sp" },
  {
    bufferSize: 1,
    endpoint: config.tracking.SNOWPLOW_COLLECTOR_URL,
  }
);

export * from "./trackSnowplowTransaction";
