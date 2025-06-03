import { NextApiRequest, NextApiResponse } from "next";
import { getSnowplowCookieValue } from "@/server/lib/snowplow/utils";
import { trackSnowplowTransaction } from "@/server/lib/snowplow";
import { CheckoutSessionRequestBody } from "@/server/types";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse<any>
) {
  if (req.method === "POST") {
    try {
      const { cartProducts, cartId, userId, totalAmount } =
        req.body as CheckoutSessionRequestBody;
      const snowplowIdCookie = getSnowplowCookieValue(req.cookies);

      if (snowplowIdCookie) {
        trackSnowplowTransaction({
          cartProducts,
          snowplowIdCookie,
          userId,
          cartId,
          totalAmount,
        });
        res.json({ id: cartId, ok: true });
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      res.status(500).json(message);
    }
  } else {
    res.setHeader("Allow", "POST");
    res.status(405).end("Method Not Allowed");
  }
}
