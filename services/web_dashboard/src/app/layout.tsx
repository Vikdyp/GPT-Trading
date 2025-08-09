import React from "react";
import StatusBar from "./components/StatusBar";

export const metadata = {
  title: "Pro Trading Bot",
  description: "Binance Spot EUR Bot"
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body style={{ fontFamily: "system-ui, sans-serif", margin: 0 }}>
        <div style={{ padding: 8, background:"#111" }}>
          <StatusBar />
        </div>

        <div style={{ padding: 16, display: "flex", gap: 16, alignItems: "center", borderBottom: "1px solid #eee" }}>
          <h3 style={{ margin: 0 }}>Pro Trading Bot</h3>
          <a href="/" style={{ textDecoration: "none" }}>Dashboard</a>
          <a href="/strategy" style={{ textDecoration: "none" }}>Stratégie</a>
          <a href="/risk" style={{ textDecoration: "none" }}>Risque</a>
          <a href="/orders" style={{ textDecoration: "none" }}>Ordres</a>
          <a href="/positions" style={{ textDecoration: "none" }}>Positions</a>
          <a href="/fills" style={{ textDecoration: "none" }}>Fills</a>
        </div>

        <div style={{ padding: 16 }}>{children}</div>
      </body>
    </html>
  );
}
