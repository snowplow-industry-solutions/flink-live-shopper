import { Product } from "@/types/product";

export type CheckoutSessionRequestBody = {
  cartProducts: Product[];
  cartId: string;
  userId: string | undefined;
  totalAmount: number;
};
