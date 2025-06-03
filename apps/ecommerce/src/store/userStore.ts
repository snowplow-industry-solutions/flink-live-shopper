import { create } from "zustand";
import { persist, createJSONStorage } from "zustand/middleware";
import { User } from "@/types/user";

type UserStore = {
  email?: string;
  userId?: string;
  setUser: (user: User) => void;
  clear: () => void;
};

const useUserStore = create<UserStore>()(
  persist(
    (set) => ({
      email: undefined,
      userId: undefined,
      setUser: (user: User) => {
        set((state) => {
          return {
            ...state,
            email: user.email,
            userId: user.userId,
          };
        });
      },
      clear: () =>
        set((state) => ({
          ...state,
          email: undefined,
          userId: undefined,
        })),
    }),
    { name: "user-storage", storage: createJSONStorage(() => sessionStorage) }
  )
);

export { useUserStore };
