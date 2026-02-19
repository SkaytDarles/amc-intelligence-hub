import os, re, hashlib, datetime
import feedparser
from typing import List
from pydantic import BaseModel, Field
from google import genai
from google.cloud import firestore

# ---------- Config ----------
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3-flash-preview")
MIN_SCORE = int(os.getenv("MIN_SCORE", "70"))
MAX_PER_SOURCE = int(os.getenv("MAX_PER_SOURCE", "8"))
MAX_TOTAL = int(os.getenv("MAX_TOTAL", "30"))

DEPARTMENTS = [
    "Finanzas y ROI",
    "FoodTech and Supply Chain",
    "Innovación y Tendencias",
    "Tecnología e Innovación",
    "Legal & Regulatory Affairs / Innovation",
]

TOPICS = [
    "LLMs & Agents", "RAG & Search", "MLOps & Observability",
    "Data Platforms", "Security & Governance", "Automation",
    "Regulation", "Productivity Tools", "FoodTech", "Supply Chain"
]

class Analysis(BaseModel):
    titulo_mejorado: str
    resumen: str
    accion: str
    departamento: str
    topics: List[str] = Field(default_factory=list)
    score: int = Field(ge=0, le=100)

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def utcnow():
    return datetime.datetime.utcnow()

def sanitize_doc_id(raw: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "_", raw.lower()).strip("_")

def load_sources(db):
    docs = db.collection("sources").where("enabled", "==", True).stream()
    out = []
    for d in docs:
        s = d.to_dict()
        if s.get("type") == "rss" and s.get("url"):
            out.append(s)
    return out

def fetch_rss(url: str, max_items: int):
    fp = feedparser.parse(url)
    out = []
    for e in (fp.entries or [])[:max_items]:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary = (e.get("summary") or e.get("description") or "").strip()
        if title and link:
            out.append({"title": title, "url": link, "summary": summary})
    return out

def analyze_item(client, source: str, title: str, url: str, summary: str) -> Analysis:
    prompt = f"""
Eres analista de inteligencia competitiva para AMC Global.
Devuelve SOLO JSON válido con el schema.

Departamentos:
{DEPARTMENTS}

Topics (máx 4):
{TOPICS}

Noticia:
Fuente: {source}
Título: {title}
URL: {url}
Texto: {summary[:1500]}

Reglas:
- Si no es relevante para AMC, score < 60.
- Acción debe ser accionable.
"""
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_json_schema": Analysis.model_json_schema(),
        },
    )
    return Analysis.model_validate_json(resp.text)

def upsert_news(db, item, analysis: Analysis, source_name: str) -> bool:
    doc_id = sha256(item["url"])
    ref = db.collection("news_articles").document(doc_id)
    if ref.get().exists:
        return False

    dept = analysis.departamento if analysis.departamento in DEPARTMENTS else "Innovación y Tendencias"
    ref.set({
        "title": analysis.titulo_mejorado,
        "url": item["url"],
        "source": source_name,
        "published_at": utcnow(),
        "analysis": {
            "departamento": dept,
            "resumen_ejecutivo": analysis.resumen,
            "accion_sugerida": analysis.accion,
            "relevancia_score": int(analysis.score),
            "topics": analysis.topics[:4],
            "model": GEMINI_MODEL,
        }
    })
    return True

def build_digest_html(dept: str, items: list, date_label: str) -> str:
    rows = ""
    for n in items:
        a = n.get("analysis", {})
        rows += f"""
        <tr><td style="padding:14px;border-bottom:1px solid #eee;">
          <div style="font-size:10px;color:#888;font-weight:700;">{dept.upper()}</div>
          <div style="font-size:16px;font-weight:800;margin:6px 0;">
            <a href="{n.get('url','')}" style="color:#00c1a9;text-decoration:none;">{n.get('title','')}</a>
          </div>
          <div style="font-size:13px;color:#333;margin:6px 0;">{a.get('resumen_ejecutivo','')}</div>
          <div style="font-size:12px;background:#eafff6;display:inline-block;padding:6px 10px;border-radius:8px;">
            💡 {a.get('accion_sugerida','')}
          </div>
          <div style="font-size:11px;color:#666;margin-top:6px;">
            Score: {a.get('relevancia_score',0)} · Topics: {", ".join(a.get("topics", [])[:4])}
          </div>
        </td></tr>
        """
    if not rows:
        rows = "<tr><td style='padding:14px;'>Sin noticias relevantes.</td></tr>"

    return f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:720px;margin:0 auto;border:1px solid #e5e5e5;border-radius:12px;overflow:hidden;">
      <div style="background:#0d1117;color:#00c1a9;padding:18px 22px;">
        <div style="font-size:18px;font-weight:900;">AMC Intelligence Digest</div>
        <div style="font-size:12px;color:#9aa4ad;">{date_label} · {dept}</div>
      </div>
      <div style="padding:14px 18px;background:#fff;">
        <table style="width:100%;border-collapse:collapse;">{rows}</table>
      </div>
    </div>
    """

def save_digest(db, date_label: str, dept: str, items: list, html: str):
    raw = f"{date_label}__{dept}"
    doc_id = sanitize_doc_id(raw)
    db.collection("newsletters").document(doc_id).set({
        "date": date_label,
        "department": dept,
        "created_at": utcnow(),
        "items": [{"title": i.get("title"), "url": i.get("url")} for i in items],
        "html": html
    }, merge=True)
    return doc_id

# ---------- Cloud Function HTTP ----------
def run_daily(request):
    # Auth simple por token (evita que cualquiera lo dispare)
    expected = os.getenv("RUN_TOKEN", "")
    got = request.headers.get("X-Run-Token", "")
    if expected and got != expected:
        return ("Unauthorized", 401)

    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        return ("Missing GOOGLE_API_KEY env var", 500)

    db = firestore.Client()
    client = genai.Client(api_key=api_key)

    run_id = utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_ref = db.collection("runs").document(run_id)
    run_ref.set({"started_at": utcnow(), "status": "running", "mode": "cloud_function"}, merge=True)

    sources = load_sources(db)
    added = analyzed = errors = total_done = 0

    try:
        for s in sources:
            if total_done >= MAX_TOTAL:
                break
            name = s.get("name", "RSS")
            items = fetch_rss(s["url"], MAX_PER_SOURCE)

            for it in items:
                if total_done >= MAX_TOTAL:
                    break
                total_done += 1
                try:
                    a = analyze_item(client, name, it["title"], it["url"], it["summary"])
                    analyzed += 1
                    if int(a.score) >= MIN_SCORE:
                        if upsert_news(db, it, a, name):
                            added += 1
                except Exception:
                    errors += 1

        # Digest por dept (últimas 24h)
        date_label = utcnow().date().isoformat()
        cutoff = utcnow() - datetime.timedelta(hours=24)

        docs = (
            db.collection("news_articles")
            .where("published_at", ">=", cutoff)
            .stream()
        )
        last_news = [d.to_dict() for d in docs]

        digests = 0
        for dept in DEPARTMENTS:
            dept_news = [
                n for n in last_news
                if n.get("analysis", {}).get("departamento") == dept
                and int(n.get("analysis", {}).get("relevancia_score", 0)) >= MIN_SCORE
            ]
            dept_news.sort(key=lambda x: int(x.get("analysis", {}).get("relevancia_score", 0)), reverse=True)
            dept_news = dept_news[:10]
            html = build_digest_html(dept, dept_news, date_label)
            save_digest(db, date_label, dept, dept_news, html)
            digests += 1

        run_ref.set({
            "finished_at": utcnow(),
            "status": "done",
            "sources": len(sources),
            "analyzed": analyzed,
            "added": added,
            "errors": errors,
            "digests": digests,
            "min_score": MIN_SCORE
        }, merge=True)

        return ({"ok": True, "added": added, "analyzed": analyzed, "errors": errors, "digests":
