"use client";
import React, { useEffect, useMemo, useState } from "react";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

type EquityPoint = { ts: string; equity_eur: number };

function LineChart({ data, width=820, height=300 }: { data: EquityPoint[]; width?: number; height?: number }) {
  const path = useMemo(() => {
    if (!data || data.length < 2) return "";
    const values = data.map(d => d.equity_eur);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const span = Math.max(1e-9, max - min);
    const stepX = width / (data.length - 1);
    const pts = data.map((d, i) => {
      const x = i * stepX;
      const y = height - ((d.equity_eur - min) / span) * height;
      return `${x},${y}`;
    });
    return "M " + pts.join(" L ");
  }, [data, width, height]);

  return (
    <svg width={width} height={height} style={{background:"#fff", border:"1px solid #eee", borderRadius:6}}>
      <path d={path} fill="none" stroke="#1e88e5" strokeWidth={2}/>
    </svg>
  );
}

export default function EquityPage() {
  const [range, setRange] = useState<"1d"|"7d"|"30d">("1d");
  const [series, setSeries] = useState<EquityPoint[]>([]);
  const [stats, setStats] = useState<any>(null);

  const load = async (rng: string) => {
    const r = await fetch(`${API}/equity/series?range=${rng}`, { cache: "no-store" });
    const j = await r.json();
    setSeries(j.points || []);
    setStats(j.stats || null);
  };

  useEffect(()=>{ load(range); const t=setInterval(()=>load(range), 15000); return ()=>clearInterval(t); }, [range]);

  return (
    <div style={{padding:16}}>
      <h2>Equity</h2>
      <div style={{display:"flex", gap:8}}>
        {["1d","7d","30d"].map(r => (
          <button key={r} onClick={()=>setRange(r as any)} disabled={range===r as any}>{r}</button>
        ))}
      </div>

      <div style={{marginTop:12}}>
        <LineChart data={series}/>
      </div>

      <div style={{marginTop:12, display:"grid", gridTemplateColumns:"repeat(3, 1fr)", gap:12, maxWidth:820}}>
        <div className="card">
          <b>Début</b><div>{stats ? stats.start.toFixed(2) + " €" : "…"}</div>
        </div>
        <div className="card">
          <b>Fin</b><div>{stats ? stats.end.toFixed(2) + " €" : "…"}</div>
        </div>
        <div className="card">
          <b>Variation</b>
          <div>
            {stats ? `${(stats.change_eur>=0?"▲":"▼")} ${stats.change_eur.toFixed(2)} € (${(stats.change_pct||0).toFixed(2)}%)` : "…"}
          </div>
        </div>
      </div>
    </div>
  );
}
