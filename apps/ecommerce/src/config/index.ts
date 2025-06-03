export const config = {
  tracking: {
    SNOWPLOW_APP_ID: "next-js-ecommerce-example",
    SNOWPLOW_COLLECTOR_URL: process.env.NEXT_PUBLIC_SNOWPLOW_COLLECTOR_URL,
    /** The ID cookie prefix will be the same for different environments/hosts */
    SNOWPLOW_ID_COOKIE_PREFIX: "_sp_id",
  },
  store: {
    DEFAULT_CURRENCY: "USD",
  },
};
