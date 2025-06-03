import { SignIn } from "@/components/sign-in";
import Head from "next/head";

export default function SearchPage() {
  return (
    <>
      <Head>
        <title>Snowplow store sign in</title>
        <meta name="description" content="Snowplow shoes search" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <link rel="icon" href="/favicon.webp" />
      </Head>
      <SignIn />
    </>
  );
}
