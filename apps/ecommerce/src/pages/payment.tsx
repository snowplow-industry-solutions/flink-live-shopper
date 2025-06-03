import { Payment } from "@/components/payment";
import Head from "next/head";

export default function PaymentPage() {
  return (
    <>
      <Head>
        <title>Payment page</title>
        <meta name="description" content="Snowplow shoes" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.webp" />
      </Head>
      <Payment />
    </>
  );
}
