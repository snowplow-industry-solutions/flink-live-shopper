import { createRelease } from "@/lib/tracking/snowplow";
import { SelfDescribingJson } from "@snowplow/browser-tracker";

export function addGitScmReleaseContext() {
  /* Vercel exposes these variables by default as part of System variables https://vercel.com/docs/projects/environment-variables/system-environment-variables#system-environment-variables */

  const commitSha = process.env.NEXT_PUBLIC_VERCEL_GIT_COMMIT_SHA;
  const repositorySlug = process.env.NEXT_PUBLIC_VERCEL_GIT_REPO_SLUG;

  if (commitSha || repositorySlug) {
    // Fix when addGlobalContexts accepts properly created context objects.
    return createRelease({
      commit_sha: commitSha,
      repository: repositorySlug
    }) as SelfDescribingJson<Record<string, unknown>>;
  }
}
