import { FormEvent, useState } from "react";
import styles from "./index.module.scss";
import { mockUsers } from "@/mocks/users";
import { tracker } from "@/lib/snowplow";
import { useRouter } from "next/router";
import { config } from "@/config";
import { useUserStore } from "@/store/userStore";

export function SignIn() {
  const [email, setEmail] = useState("");
  const router = useRouter();
  const setUser = useUserStore((state) => state.setUser);

  const handleSignIn = (e: FormEvent) => {
    e.preventDefault();
    const isValidUser = mockUsers.find((user) => user.email === email);
    if (isValidUser) {
      setUser({ email: isValidUser.email, userId: isValidUser.userId });
      tracker?.setUserId(isValidUser.userId);
      router.push(`/`);
    }
  };

  return (
    <div className={styles.signInPage}>
      <div>
        <form onSubmit={handleSignIn}>
          <div className={styles.signInHeader}>
            <h3>Type your email address to sign in.</h3>
          </div>
          <div>
            <input
              required
              value={email}
              name="email"
              id="emailInput"
              placeholder="Email"
              type="text"
              onChange={(e) => setEmail(e.target.value)}
            />
          </div>
          <div className={styles.actions}>
            <button type="submit" className={styles.signInButton}>
              Sign In
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
