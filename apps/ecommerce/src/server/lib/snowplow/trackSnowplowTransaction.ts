import { snowplowTracker } from ".";
import {
  SelfDescribingJson,
  buildSelfDescribingEvent,
} from "@snowplow/node-tracker";
import {
  createProduct,
  createSnowplowEcommerceAction,
  createTransaction,
} from "@/lib/tracking/snowplow";
import { CheckoutSessionRequestBody } from "@/server/types";

export function trackSnowplowTransaction({
  cartProducts,
  snowplowIdCookie,
  userId,
  cartId,
  totalAmount,
}: CheckoutSessionRequestBody & { snowplowIdCookie: string }) {
  const snowplowIdCookieValues = snowplowIdCookie.split(".");
  const domainUserId = snowplowIdCookieValues[0];
  const sessionId = snowplowIdCookieValues[5];
  snowplowTracker.setDomainUserId(domainUserId);
  snowplowTracker.setSessionId(sessionId);
  if (userId) {
    snowplowTracker.setUserId(userId);
  }

  const currency = "USD";
  const totalQuantity =
    cartProducts.reduce(
      (accum, lineItem) => (lineItem.quantityInCart || 1) + accum,
      0
    ) || 0;

  const productContexts = cartProducts.map((product) =>
    createProduct({
      id: product.id,
      name: product.name,
      category: product.category,
      price: product.price,
      quantity: product.quantityInCart,
      size: product.size,
      variant: product.variant,
      brand: product.brand,
      currency,
      creative_id: product.imgSrc,
    })
  ) as unknown as SelfDescribingJson<Record<string, unknown>>[];

  snowplowTracker.track(
    // Fix to node.js compatible tracking function when snowtype allows multiple configuration file generation in the same codebase
    // After that, create an event spec that is the backend transaction.
    buildSelfDescribingEvent({
      event: createSnowplowEcommerceAction({
        type: "transaction",
      }) as unknown as SelfDescribingJson<Record<string, unknown>>,
    }),
    [
      createTransaction({
        transaction_id: cartId,
        revenue: totalAmount,
        currency,
        payment_method: "card",
        total_quantity: totalQuantity,
        tax: 0,
        shipping: 0,
      }) as unknown as SelfDescribingJson<Record<string, unknown>>,
      /* Product contexts */
      ...productContexts,
    ]
  );
}
