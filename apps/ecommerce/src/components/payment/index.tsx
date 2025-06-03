import type React from "react";

import styles from "./index.module.scss";
import { useState } from "react";
import { CreditCard } from "lucide-react";
import { useRouter } from "next/router";
import { useCartStore } from "@/store";
import { useUserStore } from "@/store/userStore";

export function Payment() {
  const [cardNumber, setCardNumber] = useState("");
  const [cardName, setCardName] = useState("");
  const [expiryDate, setExpiryDate] = useState("");
  const [cvc, setCvc] = useState("");
  const router = useRouter();
  const cartProducts = useCartStore((state) => state.cartProducts);
  const totalAmount = useCartStore((state) => state.totalAmount);
  const clearCart = useCartStore((state) => state.clearCart);
  const cartId = useCartStore((state) => state.cartId);
  const userId = useUserStore((state) => state.userId);

  // Format card number with spaces after every 4 digits
  const handleCardNumberChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value.replace(/\s/g, "");
    if (/^\d*$/.test(value) && value.length <= 16) {
      const formatted = value.replace(/(\d{4})(?=\d)/g, "$1 ");
      setCardNumber(formatted);
    }
  };

  // Format expiry date as MM/YY
  const handleExpiryDateChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value.replace(/\D/g, "");
    if (value.length <= 4) {
      let formatted = value;
      if (value.length > 2) {
        formatted = value.slice(0, 2) + "/" + value.slice(2);
      }
      setExpiryDate(formatted);
    }
  };

  // Only allow numbers for CVC
  const handleCvcChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    if (/^\d*$/.test(value) && value.length <= 3) {
      setCvc(value);
    }
  };

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    const res = await fetch("/api/checkout_session", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        cartProducts,
        totalAmount,
        cartId,
        userId,
      }),
    });
    const jsonRes = await res.json();
    clearCart();
    router.replace("/?success=true&id=" + jsonRes.id);
  };

  return (
    <div className={styles.container}>
      <div className={styles.card}>
        <div className={styles.cardHeader}>
          <div className={styles.titleContainer}>
            <h2 className={styles.cardTitle}>Payment Details</h2>
            <p className={styles.cardDescription}>
              Enter your credit card information
            </p>
          </div>
          <CreditCard className={styles.cardIcon} />
        </div>
        <form onSubmit={handleSubmit}>
          <div className={styles.cardContent}>
            <div className={styles.formGroup}>
              <label htmlFor="cardNumber" className={styles.label}>
                Card Number
              </label>
              <input
                id="cardNumber"
                placeholder="1234 5678 9012 3456"
                value={cardNumber}
                onChange={handleCardNumberChange}
                className={styles.input}
                required
              />
            </div>
            <div className={styles.formGroup}>
              <label htmlFor="cardName" className={styles.label}>
                Cardholder Name
              </label>
              <input
                id="cardName"
                placeholder="John Doe"
                value={cardName}
                onChange={(e) => setCardName(e.target.value)}
                className={styles.input}
                required
              />
            </div>
            <div className={styles.formRow}>
              <div className={styles.formGroup}>
                <label htmlFor="expiryDate" className={styles.label}>
                  Expiry Date
                </label>
                <input
                  id="expiryDate"
                  placeholder="MM/YY"
                  value={expiryDate}
                  onChange={handleExpiryDateChange}
                  className={styles.input}
                  required
                />
              </div>
              <div className={styles.formGroup}>
                <label htmlFor="cvc" className={styles.label}>
                  CVC
                </label>
                <input
                  id="cvc"
                  placeholder="123"
                  value={cvc}
                  onChange={handleCvcChange}
                  className={styles.input}
                  required
                />
              </div>
            </div>
          </div>
          <div className={styles.cardFooter}>
            <button type="submit" className={styles.submitButton}>
              Complete transaction
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
