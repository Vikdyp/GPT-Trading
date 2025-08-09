"use client";
import React, { useEffect, useState } from "react";
const API = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8000";

type RiskValues = {
  base_risk_pct: number;           // déjà présent normalement
  hard_cap_eur: number;            // si tu l'utilises

  // --- caps d'expo ---
  max_portfolio_expo_pct: number;  // ex: 30 (%)
  min_cash_reserve_eur: number;    // ex: 20
  max_positions_total: number;     // ex: 5
  max_per_symbol_notional_eur: number; // ex: 50
  one_position_per_symbol: boolean;
};

export default function RiskPage() {
  const [v, setV] = useState<RiskValues>({
    base_risk_pct: 1.0,
    hard_cap_eur: 15.0,
    max_portfolio_expo_pct: 30,
    min_cash_reserve_eur: 0,
    max_positions_total: 10,
    max_per_symbol_notional_eur: 100,
    one_position_per_symbol: true,
  });
  const [loading, setLoading] = useState(true);
  const set = (k: keyof RiskValues, val: any) => setV((x)=>({...x, [k]: val}));

  const load = async () => {
    setLoading(true);
    const eff = (await (await fetch(API + "/config/effective", {cache:"no-store"})).json()).config;
    const ov  = (await (await fetch(API + "/config/overrides", {cache:"no-store"})).json()).overrides || {};
    const riskEff = (eff?.risk || {});
    const riskOv  = (ov?.risk || {});
    const expoEff = (riskEff.exposure || {});
    const expoOv  = (riskOv.exposure || {});
    setV({
      base_risk_pct: riskOv.base_risk_pct ?? riskEff.base_risk_pct ?? 1.0,
      hard_cap_eur: riskOv.hard_cap_eur ?? riskEff.hard_cap_eur ?? 15.0,
      max_portfolio_expo_pct: expoOv.max_portfolio_expo_pct ?? expoEff.max_portfolio_expo_pct ?? 30,
      min_cash_reserve_eur:   expoOv.min_cash_reserve_eur   ?? expoEff.min_cash_reserve_eur   ?? 0,
      max_positions_total:    expoOv.max_positions_total    ?? expoEff.max_positions_total    ?? 10,
      max_per_symbol_notional_eur: expoOv.max_per_symbol_notional_eur ?? expoEff.max_per_symbol_notional_eur ?? 100,
      one_position_per_symbol: expoOv.one_position_per_symbol ?? expoEff.one_position_per_symbol ?? true,
    });
    setLoading(false);
  };

  useEffect(()=>{ load(); }, []);

  const save = async () => {
    const body = { risk: {
      base_risk_pct: Number(v.base_risk_pct),
      hard_cap_eur: Number(v.hard_cap_eur),
      exposure: {
        max_portfolio_expo_pct: Number(v.max_portfolio_expo_pct),
        min_cash_reserve_eur: Number(v.min_cash_reserve_eur),
        max_positions_total: Number(v.max_positions_total),
        max_per_symbol_notional_eur: Number(v.max_per_symbol_notional_eur),
        one_position_per_symbol: !!v.one_position_per_symbol,
      }
    }};
    await fetch(API + "/config/overrides", {
      method: "PUT", headers: {"Content-Type":"application/json"},
      body: JSON.stringify(body),
    });
    alert("Paramètres risk/exposure sauvegardés (hot-reload).");
  };

  const [status, setStatus] = useState<any>(null);
  const loadStatus = async () => {
    const r = await fetch(API + "/exposure/status", {cache:"no-store"});
    const j = await r.json();
    setStatus(j);
  };
  useEffect(()=>{ loadStatus(); const t=setInterval(loadStatus, 5000); return ()=>clearInterval(t); }, []);

  if (loading) return <div>Chargement…</div>;

  return (
    <div style={{padding:16}}>
      <h2>Risk & Exposure</h2>

      <h3>Risk sizing</h3>
      <div style={{display:"grid",gridTemplateColumns:"280px 240px",gap:8, alignItems:"center",maxWidth:720}}>
        <label>Base risk % (du solde)</label>
        <input type="number" step="0.1" value={v.base_risk_pct} onChange={e=>set("base_risk_pct", Number(e.target.value))}/>
        <label>Hard cap par trade (€)</label>
        <input type="number" step="1" value={v.hard_cap_eur} onChange={e=>set("hard_cap_eur", Number(e.target.value))}/>
      </div>

      <h3 style={{marginTop:16}}>Caps d’exposition</h3>
      <div style={{display:"grid",gridTemplateColumns:"280px 240px",gap:8, alignItems:"center",maxWidth:720}}>
        <label>Exposition max portefeuille (%)</label>
        <input type="number" step="1" value={v.max_portfolio_expo_pct} onChange={e=>set("max_portfolio_expo_pct", Number(e.target.value))}/>
        <label>Réserve cash minimale (€)</label>
        <input type="number" step="1" value={v.min_cash_reserve_eur} onChange={e=>set("min_cash_reserve_eur", Number(e.target.value))}/>
        <label>Nombre max de positions</label>
        <input type="number" step="1" value={v.max_positions_total} onChange={e=>set("max_positions_total", Number(e.target.value))}/>
        <label>Notional max par symbole (€)</label>
        <input type="number" step="1" value={v.max_per_symbol_notional_eur} onChange={e=>set("max_per_symbol_notional_eur", Number(e.target.value))}/>
        <label>Une position par symbole</label>
        <input type="checkbox" checked={!!v.one_position_per_symbol} onChange={e=>set("one_position_per_symbol", e.target.checked)}/>
      </div>

      <div style={{marginTop:16}}>
        <button onClick={save}>Enregistrer</button>
      </div>

      <h3 style={{marginTop:16}}>Statut exposition</h3>
      <pre style={{background:"#f6f6f6", padding:12, borderRadius:6, overflow:"auto"}}>
{status ? JSON.stringify(status, null, 2) : "…"}
      </pre>
    </div>
  );
}
