import { useRouter } from "next/router";
import { tracker } from "@/lib/snowplow";
import { useEffect } from "react";
import { useUserStore } from "@/store/userStore";
import { setUserId } from "@snowplow/browser-tracker";

export function PageTracker() {
  const router = useRouter();
  const userId = useUserStore((state) => state.userId);

  useEffect(() => {
    setUserId(userId);
  }, [userId]);

  useEffect(() => {
    tracker?.trackPageView();

    router.events.on("routeChangeComplete", () => {
      tracker?.trackPageView();
    });

    return () => {
      router.events.off("routeChangeComplete", () => {
        tracker?.trackPageView();
      });
    };
  }, [router.events]);

  return null;
}
