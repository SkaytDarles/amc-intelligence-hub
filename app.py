import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import hashlib
import feedparser
from pydantic import BaseModel, Field
from typing import List
from google import genai

# ============================
# UI
# ============================
st.set_page_config(page_title="AMC Intelligence Hub (MVP)", page_icon="🧠", layout="wide")
st.title("🧠 AMC Intelligence Hub — MVP")
st.caption(
    "MVP: lee RSS desde Firestore (`sources`), analiza con Gemini (JSON), guarda noticias curadas en `news_articles` "
    "y genera digests por departamento en `newsletters`."
)

# ============================
# Firestore
# ============================
@st.cache_resource
def get_db():
    if "FIREBASE_KEY" not in st.secrets:
        raise RuntimeError("Falta FIREBASE_KEY en st.secrets")

    key_dict = dict(st.secrets["FIREBASE_KEY"])
    if "private_key" in key_dict and isinstance(key_dict["private_key"], str):
        key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

    if not firebase_admin._apps:
        cred = credentials.Certificate(key_dict)
        firebase_admin.initialize_app(cred)

    return firestore.client()

try:
    db = get_db()
    st.success("✅ Conectado a Firestore")
except Exception as e:
    st.error(f"❌ No se pudo conectar a Firestore: {e}")
    st.stop()

# ============================
# Gemini
# ============================
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("❌ Falta GOOGLE_API_KEY en Secrets")
    st.stop()

client = genai.Client(api_key=st.secrets["GOOGLE_API_KEY"])
GEMINI_MODEL = "gemini-3-flash-preview"

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
    titulo_mejorado: str = Field(description="Título breve en español")
    resumen: str = Field(description="Resumen ejecutivo (max 40 palabras)")
    accion: str = Field(description="Acción sugerida (1 frase)")
    departamento: str = Field(description="Uno de los departamentos permitidos")
    topics: List[str] = Field(default_factory=list, description="máx 4 tags")
    score: int = Field(ge=0, le=100, description="Relevancia 0-100")

# ============================
# Utils
# ============================
def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def utcnow():
    return datetime.datetime.utcnow()

def load_sources():
    """
    Lee fuentes desde Firestore:
      collection: sources
      fields: {name, type:'rss', url, enabled:true}
    """
    docs = db.collection("sources").where("enabled", "==", True).stream()
    sources = []
    for d in docs:
        s = d.to_dict()
        if s.get("type") == "rss" and s.get("url"):
            sources.append(s)
    return sources

def fetch_rss(url: str, max_items: int = 10):
    fp = feedparser.parse(url)
    out = []
    for e in (fp.entries or [])[:max_items]:
        title = (e.get("title") or "").strip()
        link = (e.get("link") or "").strip()
        summary = (e.get("summary") or e.get("description") or "").strip()
        if title and link:
            out.append({"title": title, "url": link, "summary": summary})
    return out

def analyze_item(source: str, title: str, url: str, summary: str) -> Analysis:
    prompt = f"""
Eres analista de inteligencia competitiva para AMC Global (alimentos/ingredientes).
Debes curar noticias de IA, digitalización y tecnología aplicada al negocio.

Devuelve SOLO JSON válido siguiendo el schema.

Departamentos permitidos:
{DEPARTMENTS}

Topics permitidos (elige máx 4):
{TOPICS}

Noticia:
- Fuente: {source}
- Título: {title}
- URL: {url}
- Texto: {summary[:1500]}

Reglas:
- Si no es relevante para AMC, score debe ser < 60.
- 'accion' debe ser accionable para un área de negocio.
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

def upsert_news(item, analysis: Analysis, source_name: str) -> bool:
    """
    Deduplicación fuerte por URL: doc_id = sha256(url)
    """
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

# ============================
# Newsletter (Digest) Builder
# ============================
def load_recent_news(limit: int = 250):
    docs = (
        db.collection("news_articles")
        .order_by("published_at", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )
    return [d.to_dict() for d in docs]

def in_last_hours(ts, hours: int = 24):
    if not ts:
        return False
    now = utcnow()
    try:
        delta = now - ts.replace(tzinfo=None)
        return delta.total_seconds() <= hours * 3600
    except Exception:
        return False

def build_digest_html(dept: str, items: list, date_label: str) -> str:
    rows = ""
    for n in items:
        a = n.get("analysis", {})
        rows += f"""
        <tr>
          <td style="padding:14px;border-bottom:1px solid #eee;">
            <div style="font-size:10px;color:#888;font-weight:700;">{dept.upper()}</div>
            <div style="font-size:16px;font-weight:800;margin:6px 0;">
              <a href="{n.get('url','')}" style="color:#00c1a9;text-decoration:none;">
                {n.get('title','')}
              </a>
            </div>
            <div style="font-size:13px;color:#333;margin:6px 0;">
              {a.get('resumen_ejecutivo','')}
            </div>
            <div style="font-size:12px;background:#eafff6;display:inline-block;padding:6px 10px;border-radius:8px;">
              💡 {a.get('accion_sugerida','')}
            </div>
            <div style="font-size:11px;color:#666;margin-top:6px;">
              Score: {a.get('relevancia_score',0)} · Topics: {", ".join(a.get("topics", [])[:4])}
            </div>
          </td>
        </tr>
        """
    if not rows:
        rows = "<tr><td style='padding:14px;'>Sin noticias relevantes en las últimas 24h.</td></tr>"

    return f"""
    <div style="font-family:Arial,Helvetica,sans-serif;max-width:720px;margin:0 auto;border:1px solid #e5e5e5;border-radius:12px;overflow:hidden;">
      <div style="background:#0d1117;color:#00c1a9;padding:18px 22px;">
        <div style="font-size:18px;font-weight:900;">AMC Intelligence Digest</div>
        <div style="font-size:12px;color:#9aa4ad;">{date_label} · {dept}</div>
      </div>
      <div style="padding:14px 18px;background:#fff;">
        <table style="width:100%;border-collapse:collapse;">
          {rows}
        </table>
      </div>
    </div>
    """

def save_digest(date_label: str, dept: str, items: list, html: str, min_score: int):
    doc_id = f"{date_label}__{dept}".replace(" ", "_").replace("&", "and").lower()
    db.collection("newsletters").document(doc_id).set({
        "date": date_label,
        "department": dept,
        "min_score": min_score,
        "created_at": utcnow(),
        "items": [{"title": i.get("title"), "url": i.get("url")} for i in items],
        "html": html
    }, merge=True)
    return doc_id

# ============================
# Sidebar Controls
# ============================
with st.sidebar:
    st.header("⚙️ Control del pipeline")

    min_score = st.slider("Score mínimo para guardar", 0, 100, 70, 1)
    max_per_source = st.slider("Máx items por fuente (RSS)", 1, 30, 8, 1)
    max_total = st.slider("Máx total por corrida", 1, 100, 25, 1)

    if st.button("🚀 Run Pipeline (RSS → Gemini → Firestore)"):
        sources = load_sources()
        if not sources:
            st.error("No hay sources enabled=true en Firestore.")
            st.stop()

        prog = st.progress(0)
        total_done = 0
        added = 0
        analyzed = 0
        errors = 0

        run_id = utcnow().strftime("%Y%m%dT%H%M%SZ")
        run_ref = db.collection("runs").document(run_id)
        run_ref.set({"started_at": utcnow(), "status": "running"}, merge=True)

        try:
            for s in sources:
                if total_done >= max_total:
                    break

                name = s.get("name", "RSS")
                url = s["url"]
                items = fetch_rss(url, max_items=max_per_source)

                for it in items:
                    if total_done >= max_total:
                        break
                    total_done += 1

                    try:
                        a = analyze_item(name, it["title"], it["url"], it["summary"])
                        analyzed += 1
                        if int(a.score) >= min_score:
                            if upsert_news(it, a, name):
                                added += 1
                    except Exception:
                        errors += 1

                    prog.progress(min(1.0, total_done / max_total))

            run_ref.set({
                "finished_at": utcnow(),
                "status": "done",
                "sources": len(sources),
                "analyzed": analyzed,
                "added": added,
                "errors": errors,
                "min_score": min_score
            }, merge=True)

            st.success(f"✅ Pipeline terminado. analyzed={analyzed} added={added} errors={errors}")
            st.toast("Listo. Revisa el feed y luego genera digests.", icon="✅")

        except Exception as e:
            run_ref.set({"status": "error", "error": str(e)}, merge=True)
            st.error(f"❌ Pipeline falló: {e}")

    st.divider()
    st.subheader("🧾 Newsletter (Digest)")

    if st.button("🧾 Generar digest por departamento (últimas 24h)"):
        all_news = load_recent_news(limit=250)
        last_news = [n for n in all_news if in_last_hours(n.get("published_at"), hours=24)]
        date_label = utcnow().date().isoformat()

        created = 0
        for dept in DEPARTMENTS:
            dept_news = [
                n for n in last_news
                if n.get("analysis", {}).get("departamento") == dept
                and int(n.get("analysis", {}).get("relevancia_score", 0)) >= min_score
            ]
            dept_news.sort(key=lambda x: int(x.get("analysis", {}).get("relevancia_score", 0)), reverse=True)
            dept_news = dept_news[:10]

            html = build_digest_html(dept, dept_news, date_label)
            save_digest(date_label, dept, dept_news, html, min_score)
            created += 1

        st.success(f"✅ Digests generados: {created} (1 por departamento)")
        st.rerun()

# ============================
# Main: Feed
# ============================
st.divider()
st.subheader("📰 Noticias curadas (últimas 50)")

docs = db.collection("news_articles").order_by("published_at", direction=firestore.Query.DESCENDING).limit(50).stream()
news = [d.to_dict() for d in docs]

if not news:
    st.info("Aún no hay noticias. Corre el pipeline desde la barra lateral.")
else:
    for n in news:
        a = n.get("analysis", {})
        st.markdown(f"### [{n.get('title','')}]({n.get('url','')})")
        st.caption(
            f"{n.get('source','')} • {a.get('departamento','')} • "
            f"score={a.get('relevancia_score',0)} • topics={', '.join(a.get('topics',[]))}"
        )
        st.write(a.get("resumen_ejecutivo", ""))
        st.write(f"**Acción:** {a.get('accion_sugerida', '')}")
        st.divider()

# ============================
# Main: Digests
# ============================
st.subheader("🧾 Newsletters generadas (últimas 10)")
dig = db.collection("newsletters").order_by("created_at", direction=firestore.Query.DESCENDING).limit(10).stream()
digests = [d.to_dict() for d in dig]

if not digests:
    st.info("Aún no hay digests. Genera uno desde la barra lateral.")
else:
    for d in digests:
        st.markdown(f"### {d.get('date')} — {d.get('department')}")
        st.components.v1.html(d.get("html", ""), height=420, scrolling=True)
        st.divider()
