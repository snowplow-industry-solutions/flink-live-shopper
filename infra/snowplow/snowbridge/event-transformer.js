function main(input) {
  const sp_data = input.Data;

  // Decide the event type
  const event_type = determineEventType(sp_data);

  // Build the structured output
  const output = {
    id: sp_data.event_id,
    type: event_type,
    timestamp: sp_data.derived_tstamp || sp_data.collector_tstamp,
    payload: extractPayload(sp_data, event_type),
    enrich: {
      client: {
        useragent: sp_data.useragent,
        browser: extractBrowserContext(sp_data),
        device: extractDeviceInfo(sp_data),
        page: extractPageInfo(sp_data)
      },
      performance: extractPerformanceMetrics(sp_data),
      user: extractUserInfo(sp_data),
      marketing: extractMarketingInfo(sp_data),
      platform: sp_data.platform
    },
    session: extractSessionInfo(sp_data),
    metadata: extractMetadata(sp_data)
  };

  // Return as Snowbridge expects
  return { Data: JSON.stringify(output) };
}

/**
 * Your original logic:
 *  - page_view => "page_view"
 *  - if (unstruct + event_name = snowplow_ecommerce_action) => type from unstruct context
 *  - else fallback to sp_data.event or "unknown"
 */
function determineEventType(sp_data) {
  if (sp_data.event === "page_view") {
    return "page_view";
  } else if (
    sp_data.event === "unstruct" &&
    sp_data.event_name === "snowplow_ecommerce_action"
  ) {
    return (
      sp_data.unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1?.type ||
      "unknown_ecommerce"
    );
  }
  return sp_data.event || "unknown";
}

/**
 * Extract payload fields for each event type, focusing on list_view’s product array.
 */
function extractPayload(sp_data, event_type) {
  // Gather structured-event fields only if they exist
  const structuredEventData = {};
  if (sp_data.se_category) structuredEventData.category = sp_data.se_category;
  if (sp_data.se_action) structuredEventData.action = sp_data.se_action;
  if (sp_data.se_label) structuredEventData.label = sp_data.se_label;
  if (sp_data.se_property) structuredEventData.property = sp_data.se_property;
  if (sp_data.se_value !== undefined) structuredEventData.value = sp_data.se_value;

  const payload = {};
  // Only add se_data if there's something in it
  if (Object.keys(structuredEventData).length > 0) {
    payload.se_data = structuredEventData;
  }

  // Typical product context array
  const productContextArray =
    sp_data.contexts_com_snowplowanalytics_snowplow_ecommerce_product_1 || [];

  // Possibly an array in the unstructured ecommerce action
  const ecommAction = getEcommerceActionContext(sp_data);
  let actionProducts = [];
  if (ecommAction && Array.isArray(ecommAction.products)) {
    actionProducts = ecommAction.products;
  }

  // Merge them to handle both contexts
  const allProducts = productContextArray.concat(actionProducts);

  switch (event_type) {
    case "page_view":
      payload.page_title = sp_data.page_title;
      payload.page_url = sp_data.page_url;
      payload.referrer = sp_data.page_referrer;
      break;

    case "product_view":
      payload.product = allProducts[0] || {};
      break;

    case "add_to_cart":
    case "remove_from_cart":
      payload.product = allProducts[0] || {};
      payload.cart =
        sp_data.contexts_com_snowplowanalytics_snowplow_ecommerce_cart_1?.[0] ||
        {};
      break;

    case "checkout_step":
      payload.checkout_step =
        sp_data.contexts_com_snowplowanalytics_snowplow_ecommerce_checkout_step_1?.[0] ||
        {};
      payload.cart =
        sp_data.contexts_com_snowplowanalytics_snowplow_ecommerce_cart_1?.[0] ||
        {};
      break;

    case "list_view":
    case "list_click":
      // Put the entire merged products array
      payload.products = allProducts;
      // Possibly get list_name from the unstructured action
      payload.list_name = ecommAction?.name ?? null;
      break;

    case "promo_view":
    case "promo_click":
      payload.promotion =
        sp_data.contexts_com_snowplowanalytics_snowplow_ecommerce_promotion_1?.[0] ||
        {};
      break;

    // Extend if you have "transaction", "refund", etc.
    default:
      break;
  }

  // If event specification context is present
  if (
    sp_data.contexts_com_snowplowanalytics_snowplow_event_specification_1 &&
    sp_data.contexts_com_snowplowanalytics_snowplow_event_specification_1.length > 0
  ) {
    payload.specification =
      sp_data.contexts_com_snowplowanalytics_snowplow_event_specification_1[0];
  }

  return payload;
}

// For unstructured ecommerce action: might be object or array
function getEcommerceActionContext(sp_data) {
  const c =
    sp_data.unstruct_event_com_snowplowanalytics_snowplow_ecommerce_snowplow_ecommerce_action_1;
  if (!c) return null;
  if (Array.isArray(c)) return c[0] || null;
  return c;
}

function extractBrowserContext(sp_data) {
  if (sp_data.contexts_com_snowplowanalytics_snowplow_browser_context_2?.length > 0) {
    return sp_data.contexts_com_snowplowanalytics_snowplow_browser_context_2[0];
  }
  return {
    viewport:
      sp_data.br_viewwidth && sp_data.br_viewheight
        ? `${sp_data.br_viewwidth}x${sp_data.br_viewheight}`
        : undefined,
    resolution:
      sp_data.dvce_screenwidth && sp_data.dvce_screenheight
        ? `${sp_data.dvce_screenwidth}x${sp_data.dvce_screenheight}`
        : undefined,
    color_depth: sp_data.br_colordepth,
    cookies_enabled: sp_data.br_cookies,
    language: sp_data.br_lang
  };
}

function extractPerformanceMetrics(sp_data) {
  if (sp_data.contexts_org_w3_performance_navigation_timing_1?.length > 0) {
    return sp_data.contexts_org_w3_performance_navigation_timing_1[0];
  }
  return {};
}

function extractUserInfo(sp_data) {
  return {
    user_id: sp_data.user_id,
    domain_userid: sp_data.domain_userid,
    network_userid: sp_data.network_userid,
    ip_address: sp_data.user_ipaddress
  };
}

function extractPageInfo(sp_data) {
  return {
    page_url: sp_data.page_url,
    page_title: sp_data.page_title,
    page_referrer: sp_data.page_referrer
  };
}

function extractDeviceInfo(sp_data) {
  return {
    created_tstamp: sp_data.dvce_created_tstamp,
    sent_tstamp: sp_data.dvce_sent_tstamp,
    screen_width: sp_data.dvce_screenwidth,
    screen_height: sp_data.dvce_screenheight,
    os_timezone: sp_data.os_timezone
  };
}

function extractMarketingInfo(sp_data) {
  const marketing_info = {};
  for (const key in sp_data) {
    if (key.startsWith("mkt_")) {
      marketing_info[key] = sp_data[key];
    }
  }
  return Object.keys(marketing_info).length > 0 ? marketing_info : null;
}

function extractSessionInfo(sp_data) {
  if (sp_data.contexts_com_snowplowanalytics_snowplow_client_session_1?.length > 0) {
    return sp_data.contexts_com_snowplowanalytics_snowplow_client_session_1[0];
  }
  return {
    domain_sessionid: sp_data.domain_sessionid,
    domain_sessionidx: sp_data.domain_sessionidx
  };
}

function extractMetadata(sp_data) {
  return {
    event_version: sp_data.event_version,
    event_format: sp_data.event_format,
    event_fingerprint: sp_data.event_fingerprint,
    event_vendor: sp_data.event_vendor,
    app_id: sp_data.app_id,
    etl_tstamp: sp_data.etl_tstamp,
    collector_tstamp: sp_data.collector_tstamp,
    v_tracker: sp_data.v_tracker,
    v_collector: sp_data.v_collector,
    v_etl: sp_data.v_etl,
    name_tracker: sp_data.name_tracker
  };
}
