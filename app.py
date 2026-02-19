import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import datetime
import hashlib
import feedparser
from pydantic import BaseModel, Field
from typing import List
from google import genai

# ----------------------------
# UI
# ----------------------------
st.set_page_config(page_title="AMC Intelligence Hub (MVP)", page_icon="🧠", layout="wide")
st.title("🧠 AMC Intelligence Hub — MVP")
st.caption("MVP: lee RSS desde Firestore (`sources`), analiza con Gemini (JSON), y guarda noticias curadas en `news_articles`.")

# ----------------------------
# Firestore
# ----------------------------
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

db = get_db()
st.success("✅ Conectado a Firestore")

# ----------------------------
# Gemini
# ----------------------------
if "GOOGLE_API_KEY" not in st.secrets:
    st.error("❌ Falta GOOGLE_API_KEY en Secrets")
    st.stop()

client = genai.Client(api_key=st.secrets["GOOGLE_API_KEY"])

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

def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()

def load_sources():
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

Devuelve SOLO JSON válido siguiendo este schema.

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
        model="gemini-3-flash-preview",
        contents=prompt,
        config={
            "response_mime_type": "application/json",
            "response_json_schema": Analysis.model_json_schema(),
        },
    )
    return Analysis.model_validate_json(resp.text)

def upsert_news(item, analysis: Analysis, source_name: str) -> bool:
    doc_id = sha256(item["url"])
    ref = db.collection("news_articles").document(doc_id)
    if ref.get().exists:
        return False

    dept = analysis.departamento if analysis.departamento in DEPARTMENTS else "Innovación y Tendencias"
    ref.set({
        "title": analysis.titulo_mejorado,
        "url": item["url"],
        "source": source_name,
        "published_at": datetime.datetime.utcnow(),
        "analysis": {
            "departamento": dept,
            "resumen_ejecutivo": analysis.resumen,
            "accion_sugerida": analysis.accion,
            "relevancia_score": int(analysis.score),
            "topics": analysis.topics[:4],
            "model": "gemini-3-flash-preview",
        }
    })
    return True

# ----------------------------
# Sidebar Controls
# ----------------------------
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

        st.write(f"Fuentes activas: {len(sources)}")
        prog = st.progress(0)
        total_done = 0
        added = 0
        analyzed = 0
        errors = 0

        # guarda el run
        run_id = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        run_ref = db.collection("runs").document(run_id)
        run_ref.set({"started_at": datetime.datetime.utcnow(), "status": "running"}, merge=True)

        try:
            for si, s in enumerate(sources):
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
                "finished_at": datetime.datetime.utcnow(),
                "status": "done",
                "sources": len(sources),
                "analyzed": analyzed,
                "added": added,
                "errors": errors,
                "min_score": min_score
            }, merge=True)

            st.success(f"✅ Pipeline terminado. analyzed={analyzed} added={added} errors={errors}")
            st.toast("Listo. Revisa el feed en news_articles.", icon="✅")

        except Exception as e:
            run_ref.set({"status": "error", "error": str(e)}, merge=True)
            st.error(f"❌ Pipeline falló: {e}")

st.divider()

# ----------------------------
# Feed de noticias (hoy)
# ----------------------------
st.subheader("📰 Noticias curadas (últimas 50)")

docs = db.collection("news_articles").order_by("published_at", direction=firestore.Query.DESCENDING).limit(50).stream()
news = [d.to_dict() for d in docs]

if not news:
    st.info("Aún no hay noticias. Corre el pipeline desde la barra lateral.")
else:
    for n in news:
        a = n.get("analysis", {})
        st.markdown(f"### [{n.get('title','')}]({n.get('url','')})")
        st.caption(f"{n.get('source','')} • {a.get('departamento','')} • score={a.get('relevancia_score',0)} • topics={', '.join(a.get('topics',[]))}")
        st.write(a.get("resumen_ejecutivo",""))
        st.write(f"**Acción:** {a.get('accion_sugerida','')}")
        st.divider()
