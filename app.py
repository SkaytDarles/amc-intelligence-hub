import streamlit as st
import firebase_admin
from firebase_admin import credentials, firestore
import datetime

st.set_page_config(page_title="AMC Intelligence Hub (MVP)", page_icon="🧪", layout="wide")

st.title("🧪 AMC Intelligence Hub — MVP (Healthcheck)")
st.caption("Objetivo de este paso: verificar conexión estable a Firestore usando st.secrets (sin UI compleja todavía).")

@st.cache_resource
def get_db():
    if "FIREBASE_KEY" not in st.secrets:
        raise RuntimeError("Falta FIREBASE_KEY en st.secrets")

    # FIREBASE_KEY viene como dict en secrets
    key_dict = dict(st.secrets["FIREBASE_KEY"])

    # Corrige saltos de línea del private_key si vienen escapados
    if "private_key" in key_dict and isinstance(key_dict["private_key"], str):
        key_dict["private_key"] = key_dict["private_key"].replace("\\n", "\n")

    # Inicializa Firebase una sola vez
    if not firebase_admin._apps:
        cred = credentials.Certificate(key_dict)
        firebase_admin.initialize_app(cred)

    return firestore.client()

try:
    db = get_db()
    st.success("✅ Conectado a Firestore")

    # --- DEBUG: confirma que Streamlit apunta al MISMO proyecto que tu Firebase Console ---
    try:
        key_dict = dict(st.secrets["FIREBASE_KEY"])
        st.write("🔎 Project ID (desde secrets):", key_dict.get("project_id"))
    except Exception as e:
        st.warning(f"No pude leer project_id desde FIREBASE_KEY: {e}")

    # Lista IDs reales de la colección sources (si está en este proyecto)
    try:
        docs_debug = list(db.collection("sources").limit(20).stream())
        st.write("📌 sources encontrados:", len(docs_debug))
        st.write("IDs:", [d.id for d in docs_debug])
    except Exception as e:
        st.warning(f"No pude listar sources: {e}")

except Exception as e:
    st.error(f"❌ No se pudo conectar a Firestore: {e}")
    st.stop()

st.divider()

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("Healthcheck")
    if st.button("✅ Probar escritura/lectura"):
        try:
            # Escribe un documento “health” (no rompe nada del futuro)
            now = datetime.datetime.utcnow().isoformat() + "Z"
            ref = db.collection("healthchecks").document("streamlit")
            ref.set({
                "last_check": now,
                "status": "ok",
                "app": "amc-intelligence-hub-mvp"
            }, merge=True)

            # Lee el mismo documento
            data = ref.get().to_dict()
            st.success("✅ Escritura/lectura OK")
            st.json(data)
        except Exception as e:
            st.error(f"❌ Healthcheck falló: {e}")

with col2:
    st.subheader("Vista rápida de configuración")
    st.write("Secrets detectados:")
    st.code(
        "\n".join(sorted([k for k in st.secrets.keys()])),
        language="text"
    )

st.divider()

st.subheader("Preparación para el pipeline (aún no lo ejecutamos)")
st.write("Si existe una colección `sources`, aquí la verás (más adelante la usaremos para que AMC administre fuentes sin tocar código).")

try:
    docs = db.collection("sources").limit(10).stream()
    sources = [d.to_dict() for d in docs]
    if sources:
        st.json(sources)
    else:
        st.info("No hay fuentes aún. Está bien por ahora.")
except Exception as e:
    st.warning(f"No se pudo leer `sources`: {e}")
