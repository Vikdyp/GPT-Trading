"use client";
import React, { useEffect, useMemo, useState } from "react";

const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

type EquityPoint = { ts: string; equity_eur: number };

function Sparkline({ data, width=260, height=64 }: { data: EquityPoint[]; width?: number; height?: number }) {
  const path = useMemo(() => {
    if (!data || data.length < 2) return "";
    const values = data.map(d => d.equity_eur);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = Math.max(1e-9, max - min);
    const n = data.length;
    const stepX = width / (n - 1);
    const pts = data.map((d, i) => {
      const x = i * stepX;
      const y = height - ((d.equity_eur - min) / span) * height;
      return `${x.toFixed(2)},${y.toFixed(2)}`;
    });
    return "M " + pts.join(" L ");
  }, [data, width, height]);

  const last = data?.[data.length-1]?.equity_eur ?? 0;
  const first = data?.[0]?.equity_eur ?? last;
  const change = last - first;
  const changePct = first ? (change/first*100) : 0;

  return (
    <div style={{display:"flex", gap:16, alignItems:"center"}}>
      <svg width={width} height={height}>
        <path d={path} fill="none" stroke="#1e88e5" strokeWidth={2} />
      </svg>
      <div>
        <div style={{fontSize:14, color:"#666"}}>Équity (24h)</div>
        <div style={{fontWeight:700}}>
          {last.toFixed(2)} €
          <span style={{marginLeft:8, color: change>=0 ? "#0a0" : "#b00"}}>
            {change>=0? "▲" : "▼"} {change.toFixed(2)} € ({changePct.toFixed(2)}%)
          </span>
        </div>
        <a href="/equity" style={{fontSize:12, color:"#1e88e5"}}>Voir la courbe</a>
      </div>
    </div>
  );
}

export default function Home() {
  const [series, setSeries] = useState<EquityPoint[]>([]);
  const [status, setStatus] = useState<any>(null);

  // equity series 1d
  const loadSeries = async () => {
    try {
      const r = await fetch(API + "/equity/series?range=1d", { cache: "no-store" });
      const j = await r.json();
      setSeries(j.points || []);
    } catch {}
  };

  // bot status
  const loadStatus = async () => {
    try {
      const r = await fetch(API + "/bot/status", { cache: "no-store" });
      const j = await r.json();
      setStatus(j);
    } catch {}
  };

  useEffect(() => {
    loadSeries(); loadStatus();
    const t1 = setInterval(loadSeries, 10000);
    const t2 = setInterval(loadStatus, 5000);
    return () => { clearInterval(t1); clearInterval(t2); };
  }, []);

  return (
    <div style={{padding:16}}>
      <h2>Dashboard</h2>

      <div style={{display:"flex", gap:24, alignItems:"center", flexWrap:"wrap"}}>
        <Sparkline data={series} />
        <div style={{padding:12, border:"1px solid #eee", borderRadius:8, minWidth:220}}>
          <div><b>Bot</b>: {status?.enabled ? "✅ ON" : "⛔ OFF"}</div>
          <div>Mode: <b>{status?.mode}</b></div>
          <div>Kill-switch: {status?.kill_active ? "🛑 actif" : "OK"}</div>
          <div>PNL du jour: {typeof status?.daily_pnl_eur === "number" ? status.daily_pnl_eur.toFixed(2) + " €" : "…"}</div>
        </div>
      </div>
    </div>
  );
}
