import streamlit as st
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
from io import BytesIO
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# =========================
# CONFIGURACIÓN
# =========================
login_url = "http://sigof.distriluz.com.pe/plus/usuario/login"
CAMBIAR_UNIDAD_URL = "http://sigof.distriluz.com.pe/plus/usuario/ajax_cambiar_sesion"
FILE_ID = "1td-2WGFN0FUlas0Vx8yYUSb7EZc7MbGWjHDtJYhEY-0"
PERIODO_PERMITIDO = "0"

headers = {
    "User-Agent": "Mozilla/5.0",
    "Referer": login_url,
}

UNIDADES = {
    "Ayacucho": 76,
    "Huancayo": 77,
    "Huancavelica": 78,
    "Tarma": 79,
    "Selva Central": 80,
    "Pasco": 81,
    "Huánuco": 82,
    "Valle Mantaro": 83,
    "Tingo María": 84,
}

# =========================
# LOGIN
# =========================
def login_sigof(session, usuario, password):
    credentials = {
        "data[Usuario][usuario]": usuario,
        "data[Usuario][pass]": password
    }

    login_page = session.get(login_url, headers=headers)
    soup = BeautifulSoup(login_page.text, "html.parser")
    csrf_token = soup.find("input", {"name": "_csrf_token"})
    if csrf_token:
        credentials["_csrf_token"] = csrf_token["value"]

    response = session.post(login_url, data=credentials, headers=headers)

    match = re.search(r"var DEFECTO_IDUUNN\s*=\s*'(\d+)'", response.text)
    if not match:
        return None, False

    return int(match.group(1)), True


# =========================
# CAMBIO UNIDAD
# =========================
def cambiar_unidad(session, iduunn):
    payload = {"idempresa": 4, "iduunn": iduunn}
    session.post(CAMBIAR_UNIDAD_URL, data=payload, headers=headers)
    return True


# =========================
# DESCARGA DRIVE
# =========================
@st.cache_data(ttl=600)
def cargar_ciclos_drive(file_id):
    url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
    r = requests.get(url)
    return pd.read_excel(BytesIO(r.content))


# =========================
# DESCARGA PARALELA
# =========================
def descargar_archivo_fast(session, codigo, periodo, nombre, intentos=2):
    zona = ZoneInfo("America/Lima")
    hoy = datetime.now(zona).strftime("%Y-%m-%d")

    url = (
        f"http://sigof.distriluz.com.pe/plus/Reportes/"
        f"ajax_ordenes_historico_xls/U/{hoy}/{hoy}/0/"
        f"{codigo}/0/0/0/0/0/0/0/0/9/{periodo}"
    )

    for _ in range(intentos):
        try:
            r = session.get(url, headers=headers, timeout=(10, 60), stream=True)
            if r.status_code == 200 and "spreadsheetml" in r.headers.get("Content-Type", ""):
                return r.content, f"{nombre}_{periodo}.xlsx"
        except:
            continue

    return None, None


# =========================
# APP
# =========================
def main():
    st.set_page_config(page_title="LMC Lecturas", layout="wide")
    st.title("📊 Control de Avance Lecturas")

    if "session" not in st.session_state:
        st.session_state.session = None

    # LOGIN
    if st.session_state.session is None:
        usuario = st.text_input("Usuario SIGOF")
        password = st.text_input("Contraseña SIGOF", type="password")

        if st.button("Iniciar Sesión"):
            session = requests.Session()
            defecto_iduunn, ok = login_sigof(session, usuario, password)

            if not ok:
                st.error("Error de autenticación")
                return

            st.session_state.session = session
            st.session_state.defecto_iduunn = defecto_iduunn
            st.success("Sesión iniciada correctamente")
            st.rerun()

    # UNA VEZ LOGUEADO
    if st.session_state.session:

        df_drive = cargar_ciclos_drive(FILE_ID)
        df_drive["id_unidad"] = pd.to_numeric(df_drive["id_unidad"], errors="coerce")

        df_drive = df_drive[df_drive["id_unidad"] == st.session_state.defecto_iduunn]

        ciclos = {
            f"{r['Id_ciclo']} {r['nombre_ciclo']}": str(r["Id_ciclo"])
            for _, r in df_drive.iterrows()
        }

        seleccionados = st.multiselect("Seleccione ciclos", list(ciclos.keys()))

        if st.button("📥 Descargar y Mostrar Avance"):

            archivos = {}

            with ThreadPoolExecutor(max_workers=4) as executor:
                tareas = [
                    executor.submit(
                        descargar_archivo_fast,
                        st.session_state.session,
                        ciclos[nombre],
                        PERIODO_PERMITIDO,
                        nombre
                    )
                    for nombre in seleccionados
                ]

                for future in as_completed(tareas):
                    contenido, filename = future.result()
                    if contenido:
                        archivos[filename] = contenido

            if not archivos:
                st.error("No se descargó ningún archivo.")
                return

            resumen_total = []

            for filename, contenido in archivos.items():

                df = pd.read_excel(
                    BytesIO(contenido),
                    usecols=["lecturista", "resultado", "foto"],
                    engine="openpyxl"
                )

                resumen = (
                    df.groupby("lecturista")
                    .agg(
                        Asignados=("lecturista", "size"),
                        Avance=("resultado", lambda x: x.notna().sum()),
                        Fotos=("foto", lambda x: x.astype(str).str.lower().str.contains("ver foto").sum())
                    )
                    .reset_index()
                )

                resumen["Pendientes"] = resumen["Asignados"] - resumen["Avance"]
                resumen["% Avance"] = (resumen["Avance"] / resumen["Asignados"] * 100).round(2)
                resumen["% Fotos"] = (resumen["Fotos"] / resumen["Asignados"] * 100).round(2)
                resumen["Ciclo"] = filename

                resumen_total.append(resumen)

            df_final = pd.concat(resumen_total, ignore_index=True)

            st.dataframe(df_final, use_container_width=True)

            # EXPORTAR
            buffer = BytesIO()
            df_final.to_excel(buffer, index=False)
            buffer.seek(0)

            st.download_button(
                "📊 Exportar Excel",
                data=buffer,
                file_name="Resumen_LMC.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

        if st.button("Cerrar Sesión"):
            st.session_state.session = None
            st.rerun()


if __name__ == "__main__":
    main()
