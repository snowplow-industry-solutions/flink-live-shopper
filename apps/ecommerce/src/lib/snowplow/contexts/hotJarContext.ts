import { createUser as createHotJarUser } from "@/lib/tracking/snowplow";
import { getCookie } from "@/utils";
import { SelfDescribingJson } from "@snowplow/browser-tracker";

const HOTJAR_SITE_ID = "3650333";
const HOTJAR_USER_ID_COOKIE_NAME = `_hjSessionUser_${HOTJAR_SITE_ID}`;

export function addHotjarUserContext() {
  const hotjarUserIdCookie = getCookie(HOTJAR_USER_ID_COOKIE_NAME);

  if (hotjarUserIdCookie) {
    /* Hotjar uses base-64 to encode cookie values. */
    const decodedHotjarCookieValues = atob(hotjarUserIdCookie);

    let hotjarUserId = "";
    let hotjarUserIdValueCreatedAt = "";
    try {
      const { id, created } = JSON.parse(decodedHotjarCookieValues);
      hotjarUserId = id;
      hotjarUserIdValueCreatedAt = created;
    } catch (e) {}

    // Fix when addGlobalContexts accepts properly created context objects.
    return createHotJarUser({
      user_cookie_value: hotjarUserIdCookie,
      user_id: hotjarUserId,
      created_at: Number(hotjarUserIdValueCreatedAt),
    }) as SelfDescribingJson<Record<string, unknown>>;
  }
}
