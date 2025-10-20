# Nombre: app_filtro_ventas_streamlit.py
# Fecha: 17 de octubre de 2025
# Utilidad: Aplicación interactiva con Streamlit para filtrar ventas por año, mes, código de producto y clase de cliente,
#           visualizar los resultados y exportarlos a un archivo CSV personalizado.
# API/Dependencias: Streamlit, pandas, pyodbc (o pymssql)
# Descripción: 
#   Esta aplicación se conecta a una base de datos SQL Server, ejecuta una consulta predefinida que obtiene datos de ventas,
#   permite al usuario aplicar filtros interactivos y muestra una tabla con los resultados agrupados mensualmente.
#   Incluye botones para exportar los datos filtrados a un archivo CSV, con opción de elegir nombre y ubicación (en entornos compatibles).
#
# Ejemplo de uso:
#   - Al ejecutar: streamlit run app_filtro_ventas_streamlit.py
#   - El usuario selecciona: Año=2024, Mes=Marzo, Clase_cliente="Minorista"
#   - Se muestra una tabla con columnas: fecha_documento, codigo_producto, ..., precio_total_orig
#   - Al hacer clic en "Exportar CSV", se descarga un archivo como "ventas_2024_03.csv"
import streamlit as st
import pandas as pd
from urllib.parse import quote_plus
import pyodbc
from datetime import datetime
import io
from io import BytesIO
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import calendar
import time
import locale
import requests

# Configurar locale para formato español (opcional)
try:
    locale.setlocale(locale.LC_ALL, 'es_ES.UTF-8')
except:
    pass

# Configuración inicial de la página
st.set_page_config(
    page_title="Filtro de Ventas - Análisis Mensual",
    page_icon="📊",
    layout="wide"
)

def _ensure_download_param(url: str) -> str:
    if "download=1" in url:
        return url
    joiner = "&" if "?" in url else "?"
    return f"{url}{joiner}download=1"

@st.cache_data(ttl=600)
def load_csv_from_onedrive(url: str) -> pd.DataFrame:
    final_url = _ensure_download_param(url)
    resp = requests.get(final_url, timeout=60, allow_redirects=True)
    resp.raise_for_status()
    
    # Lista de encodings para español
    encodings = ['latin1', 'iso-8859-1', 'cp1252', 'utf-8']
    
    for encoding in encodings:
        try:
            raw = io.BytesIO(resp.content)
            df = pd.read_csv(
                raw, 
                sep=None, 
                engine="python", 
                encoding=encoding,
                dayfirst=True,
                parse_dates=False
            )
            st.success(f"✅ Archivo cargado con encoding: {encoding}")
            return df
        except UnicodeDecodeError as e:
            continue
        except Exception as e:
            st.error(f"Error con {encoding}: {e}")
            continue
    
    st.error("❌ No se pudo cargar con ningún encoding")
    return None
        



def set_dataframe_font_size(font_size=12, header_size=14):
    """
    Función para establecer tamaño de fuente en DataFrames
    """
    css = f"""
    <style>
    .dataframe {{
        font-size: {font_size}px !important;
    }}
    .dataframe thead th {{
        font-size: {header_size}px !important;
        font-weight: bold;
    }}
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def colorear_valor(valor):
        color = "red" if valor < 0 else "black"
        return f"<span style='color:{color}; font-weight:bold;'>${valor:,.2f}</span>"


st.title("🔍 Filtro Interactivo de Ventas por Producto y Cliente")
st.markdown("Seleccione los filtros deseados y exporte los resultados agrupados por mes.")

# ==============================
# 1. FUNCIÓN PARA CARGAR DATOS
# ==============================
#@st.cache_data(ttl=3600)  # Cachea por 1 hora


def cargar_datos():
    
    try:
        # 🔒 Reemplaza estos parámetros con tu configuración real
        username = "rdgp"
        password = "P3muGP@386x"  # ¡sin codificar aquí!
        server = r"IBMSQLN1\DynamicsChile"
        database = "GPCPR"

        # Codificamos la contraseña para que sea segura en una URI
        encoded_password = quote_plus(password)

        conn = (
            f"mssql+pyodbc://{username}:{encoded_password}@{server}/{database}"
            "?driver=ODBC+Driver+11+for+SQL+Server")

        # Origen del query, este es el original
        query01 = """WITH 
        CTE_TEMPORAL1 AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                CASE 
                    WHEN CCATEGORIA IN ('CVTAG', 'CVTA') THEN 1
                    ELSE 0
                END AS EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE CTIPODISTRIBUCION = 8 AND CCATEGORIA IN ('CVTAG', 'CVTANG', 'CVTA')
        ),
        CTE_TEMPORAL2 AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                CASE 
                    WHEN CCATEGORIA IN ('VTASG', 'DVLSG', 'VTAS') THEN 1
                    ELSE 0
                END AS EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE CTIPODISTRIBUCION IN (9, 18, 19) AND CCATEGORIA IN ('VTASG', 'VTASNG', 'DVLSG', 'DVLSNG', 'VTAS')
        ),
        CTE_TEMPORAL3 AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                CASE 
                    WHEN CCATEGORIA IN ('DSCG', 'DSC') THEN 1
                    ELSE 0
                END AS EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE CTIPODISTRIBUCION = 10 AND CCATEGORIA IN ('DSCG', 'DSCNG', 'DSC')
        ),
        CTE_TEMPORAL4 AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                0 EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE CTIPODISTRIBUCION = 8 AND CCATEGORIA = 'CVTA'
        ),
        CTE_TEMPORAL5 AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                0 EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE CTIPODISTRIBUCION IN (9, 18, 19) AND CCATEGORIA = 'VTAS'
        ),
        CTE_TEMPORAL6 AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                0 EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE CTIPODISTRIBUCION = 10 AND CCATEGORIA = 'DSC'
        ), CTE_VTDOCUMENTOVTACAB AS (
            SELECT 
                A.*
            FROM 
                MOVGC_VTDOCUMENTOVTACAB (NOLOCK) A
            WHERE 
                A.CTIPODOCUMENTO IN ('FT', 'DV')
                AND NOT A.EDOCUMENTOVTACAB = 'NVO'
                AND A.FEMISION >= CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 31, 0) AS DATE)--BETWEEN '2025-01-01' AND '2025-04-30'
        ),
        CTE_VTDOCUMENTOVTADET AS (
            SELECT 
                A.*
            FROM 
                MOVGC_VTDOCUMENTOVTADET (NOLOCK) A
            INNER JOIN 
                CTE_VTDOCUMENTOVTACAB B
            ON 
                A.CDOCUMENTOVTACAB = B.CDOCUMENTOVTACAB
            AND A.CTIPODOCUMENTO = B.CTIPODOCUMENTO
        )
        -- AHORA PUEDES UTILIZAR LA CTE EN UNA CONSULTA POSTERIOR:
        SELECT
            CASE
                WHEN A.EREBATE = 1 THEN 'SI'
                ELSE 'NO'
            END AS REBATE, 
            --CASE
            --	WHEN ISNULL(A.EREBATEFACTURADO, 0)=1 THEN 'SI'
            --	ELSE 'NO'
            --END AS [REBATE FACT.], 
            B.DESTADO AS ESTADO_DOCUMENTO,
            (CASE A.EDOCUMENTOVTACAB 
                WHEN 'ANL' THEN 'SI'
                ELSE 'NO'
            END) AS ESTADO_ANULADO,
            RTRIM(C.DTIPODOCUMENTO) AS TIPO,
            RTRIM(G.DCONCEPTO) AS CONCEPTO,
            RTRIM(A.CDIRECCION) AS CODIGO,
            RTRIM(A.DDIRECCION) AS DIRECCION,
            RTRIM(A.CSERIE) AS SERIE,
            RTRIM(A.CCORRELATIVO) AS CORRELATIVO,
            RTRIM(O.DMARCA) AS MARCA,
            RTRIM(D.CPRODUCTO) AS CODIGO_PRODUCTO,
            RTRIM(D.DPRODUCTO) AS NOMBRE_PRODUCTO,
            (CASE WHEN ISNULL(N.CCATEGORIA, 'NIN') = 'NIN' THEN '' ELSE N.CCATEGORIA END) AS CATEGORIA,
            RTRIM(A.CCLIENTE) AS NUMERO_CLIENTE,
            A.DRAZONSOCIAL AS NOMBRE_CLIENTE,
            ISNULL(RTRIM(F.CLASDSCR), '') AS CLASE_CLIENTE,
            A.FEMISION AS FECHA_DOCUMENTO,   
            A.FENTREGAORDEN AS FECHA_ENTREGA,
            ISNULL(H.CCUENTAHOMOLOGADA,K.CCUENTAHOMOLOGADA) AS CUENTA_COSTO_VENTAS,
            ISNULL(I.CCUENTAHOMOLOGADA,L.CCUENTAHOMOLOGADA) AS CUENTA_VENTAS,
            ISNULL(J.CCUENTAHOMOLOGADA,M.CCUENTAHOMOLOGADA) AS CUENTA_DESCUENTO,
            (CASE A.EREBATE WHEN 0 THEN (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.NUNIDADES * -1 ELSE D.NUNIDADES END) ELSE 0 END) AS CANTIDAD,			   	
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ISUBTOTAJUORG * -1 ELSE D.ISUBTOTAJUORG END) AS PRECIO_TOTAL_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ISUBTOTAJUFUN * -1 ELSE D.ISUBTOTAJUFUN END) AS PRECIO_TOTAL_FUNC,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTCOSTOFUN * -1 ELSE D.ITOTCOSTOFUN END) AS COSTO_TOTAL,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.IPVPAJUORG * -1 ELSE D.IPVPAJUORG END) AS PVP_UNIT_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.IPVPAJUFUN * -1 ELSE D.IPVPAJUFUN END) AS PVP_UNIT_FUNC,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTPVPAJUORG * -1 ELSE D.ITOTPVPAJUORG END) AS PVP_UNIT_TOTAL_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTPVPAJUFUN * -1 ELSE D.ITOTPVPAJUFUN END) AS PVP_UNIT_TOTAL_FUNC,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ICANALAJUORG * -1 ELSE D.ICANALAJUORG END) AS CANAL_UNIT_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ICANALAJUFUN * -1 ELSE D.ICANALAJUFUN END) AS CANAL_UNIT_FUNC,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTCANALAJUORG * -1 ELSE D.ITOTCANALAJUORG END) AS CANAL_TOTAL_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTCANALAJUFUN * -1 ELSE D.ITOTCANALAJUFUN END) AS CANAL_TOTAL_FUNC,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.IDESCAJUORG * -1 ELSE D.IDESCAJUORG END) AS OTROS_UNIT_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.IDESCAJUFUN * -1 ELSE D.IDESCAJUFUN END) AS OTROS_UNIT_FUNC,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTDESCAJUORG * -1 ELSE D.ITOTDESCAJUORG END) AS OTROS_TOTAL_ORIG,
            (CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ITOTDESCAJUFUN * -1 ELSE D.ITOTDESCAJUFUN END) AS OTROS_TOTAL_FUNC,
            D.ICOSTOPRMFUN AS COSTO_UNITARIO_FUNC,
            ISNULL(A.CDOCUMENTOVTAANULACION,'') AS DOC_ANULADO,
            CASE WHEN RTRIM(LTRIM(A.CTIPODOCUMENTOORG)) = 'OV' THEN LTRIM(RTRIM(ISNULL(A.CDOCUMENTOVTACABORG,''))) ELSE '' END AS DOC_ORIGEN,
            RTRIM(LTRIM(A.[CORDENCOMPRACLIENTE])) AS NUMERO_ORDEN
        FROM CTE_VTDOCUMENTOVTACAB A (NOLOCK)
            INNER JOIN MAEGC_CFESTADO B (NOLOCK)
            ON A.EDOCUMENTOVTACAB = B.CESTADO AND B.CTABLA = 'DV'
            INNER JOIN MAEGC_CFTIPODOCUMENTO C (NOLOCK)
            ON A.CTIPODOCUMENTO = C.CTIPODOCUMENTO
            INNER JOIN CTE_VTDOCUMENTOVTADET D (NOLOCK)
            ON A.CDOCUMENTOVTACAB = D.CDOCUMENTOVTACAB
            AND A.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            INNER JOIN RM00101 E (NOLOCK)
            ON A.CCLIENTE = E.CUSTNMBR
            LEFT JOIN RM00201 F (NOLOCK)
            ON E.CUSTCLAS = F.CLASSID
            INNER JOIN MAEGC_CFCONCEPTO G (NOLOCK)
            ON A.CCONCEPTO = G.CCONCEPTO
            LEFT JOIN CTE_TEMPORAL1 H 
            ON H.CDOCUMENTO = D.CDOCUMENTOVTACAB
            AND H.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            AND H.EGRAVADOVENTAS = D.EGRAVADOVENTAS
            LEFT JOIN CTE_TEMPORAL2 I
            ON I.CDOCUMENTO = D.CDOCUMENTOVTACAB
            AND I.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            AND I.EGRAVADOVENTAS = D.EGRAVADOVENTAS
            LEFT JOIN CTE_TEMPORAL3 J
            ON J.CDOCUMENTO = D.CDOCUMENTOVTACAB
            AND J.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            AND J.EGRAVADOVENTAS = D.EGRAVADOVENTAS
            LEFT JOIN CTE_TEMPORAL4 K
            ON K.CDOCUMENTO = D.CDOCUMENTOVTACAB
            AND K.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            AND K.EGRAVADOVENTAS = D.EGRAVADOVENTAS
            LEFT JOIN CTE_TEMPORAL5 L
            ON L.CDOCUMENTO = D.CDOCUMENTOVTACAB
            AND L.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            AND L.EGRAVADOVENTAS = D.EGRAVADOVENTAS
            LEFT JOIN CTE_TEMPORAL6 M
            ON M.CDOCUMENTO = D.CDOCUMENTOVTACAB
            AND M.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            AND M.EGRAVADOVENTAS = D.EGRAVADOVENTAS
            INNER JOIN MAEGC_PRODUCTO N (NOLOCK)
            ON N.CPRODUCTO = D.CPRODUCTO
            LEFT JOIN MAEGC_MARCA O (NOLOCK)
            ON N.CMARCA = O.CMARCA 
        WHERE A.CTIPODOCUMENTO IN ('FT','DV')
        AND NOT A.EDOCUMENTOVTACAB = 'NVO' 
        AND A.FEMISION >=  CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 31, 0) AS DATE)"""

        # QUERY_FACTURACION  este es el nombre del query origenal mejorado
        query02 = """
        WITH 
        CTE_DISTRIBUCION_CONTABLE AS (
            SELECT 
                CDOCUMENTO, 
                CTIPODOCUMENTO, 
                CCUENTAHOMOLOGADA, 
                CCATEGORIA, 
                CTIPODISTRIBUCION,
                CASE 
                    WHEN CTIPODISTRIBUCION = 8 AND CCATEGORIA IN ('CVTAG', 'CVTA') THEN 1
                    WHEN CTIPODISTRIBUCION IN (9, 18, 19) AND CCATEGORIA IN ('VTASG', 'DVLSG', 'VTAS') THEN 1
                    WHEN CTIPODISTRIBUCION = 10 AND CCATEGORIA IN ('DSCG', 'DSC') THEN 1
                    ELSE 0
                END AS EGRAVADOVENTAS
            FROM MOVGC_DOCUMENTOXDISTRIBUCION (NOLOCK)
            WHERE 
                (CTIPODISTRIBUCION = 8 AND CCATEGORIA IN ('CVTAG', 'CVTANG', 'CVTA'))
                OR 
                (CTIPODISTRIBUCION IN (9, 18, 19) AND CCATEGORIA IN ('VTASG', 'VTASNG', 'DVLSG', 'DVLSNG', 'VTAS'))
                OR 
                (CTIPODISTRIBUCION = 10 AND CCATEGORIA IN ('DSCG', 'DSCNG', 'DSC'))
        ),
        CTE_VTDOCUMENTOVTACAB AS (
            SELECT A.*
            FROM MOVGC_VTDOCUMENTOVTACAB A (NOLOCK)
            WHERE 
                A.CTIPODOCUMENTO IN ('FT', 'DV')
                AND A.EDOCUMENTOVTACAB <> 'NVO'
                AND A.FEMISION >= CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 31, 0) AS DATE)
        ),
        CTE_VTDOCUMENTOVTADET AS (
            SELECT A.*
            FROM MOVGC_VTDOCUMENTOVTADET A (NOLOCK)
            INNER JOIN CTE_VTDOCUMENTOVTACAB B
                ON A.CDOCUMENTOVTACAB = B.CDOCUMENTOVTACAB
                AND A.CTIPODOCUMENTO = B.CTIPODOCUMENTO
        )
        SELECT
            CASE WHEN A.EREBATE = 1 THEN 'SI' ELSE 'NO' END AS REBATE,
            B.DESTADO AS ESTADO_DOCUMENTO,
            CASE WHEN A.EDOCUMENTOVTACAB = 'ANL' THEN 'SI' ELSE 'NO' END AS ESTADO_ANULADO,
            RTRIM(C.DTIPODOCUMENTO) AS TIPO,
            RTRIM(G.DCONCEPTO) AS CONCEPTO,
            RTRIM(A.CDIRECCION) AS CODIGO,
            RTRIM(A.DDIRECCION) AS DIRECCION,
            RTRIM(A.CSERIE) AS SERIE,
            RTRIM(A.CCORRELATIVO) AS CORRELATIVO,
            RTRIM(O.DMARCA) AS MARCA,
            RTRIM(D.CPRODUCTO) AS CODIGO_PRODUCTO,
            RTRIM(D.DPRODUCTO) AS NOMBRE_PRODUCTO,
            CASE WHEN ISNULL(N.CCATEGORIA, 'NIN') = 'NIN' THEN '' ELSE N.CCATEGORIA END AS CATEGORIA,
            RTRIM(A.CCLIENTE) AS NUMERO_CLIENTE,
            A.DRAZONSOCIAL AS NOMBRE_CLIENTE,
            ISNULL(RTRIM(F.CLASDSCR), '') AS CLASE_CLIENTE,
            A.FEMISION AS FECHA_DOCUMENTO,   
            A.FENTREGAORDEN AS FECHA_ENTREGA,

            ISNULL(
                MAX(CASE WHEN H.EGRAVADOVENTAS = 1 AND H.CTIPODISTRIBUCION = 8 THEN H.CCUENTAHOMOLOGADA END),
                MAX(CASE WHEN H.EGRAVADOVENTAS = 0 AND H.CTIPODISTRIBUCION = 8 THEN H.CCUENTAHOMOLOGADA END)
            ) AS CUENTA_COSTO_VENTAS,

            ISNULL(
                MAX(CASE WHEN H.EGRAVADOVENTAS = 1 AND H.CTIPODISTRIBUCION IN (9,18,19) THEN H.CCUENTAHOMOLOGADA END),
                MAX(CASE WHEN H.EGRAVADOVENTAS = 0 AND H.CTIPODISTRIBUCION IN (9,18,19) THEN H.CCUENTAHOMOLOGADA END)
            ) AS CUENTA_VENTAS,

            ISNULL(
                MAX(CASE WHEN H.EGRAVADOVENTAS = 1 AND H.CTIPODISTRIBUCION = 10 THEN H.CCUENTAHOMOLOGADA END),
                MAX(CASE WHEN H.EGRAVADOVENTAS = 0 AND H.CTIPODISTRIBUCION = 10 THEN H.CCUENTAHOMOLOGADA END)
            ) AS CUENTA_DESCUENTO,

            CASE 
                WHEN A.EREBATE = 1 THEN 0 
                ELSE CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.NUNIDADES * -1 ELSE D.NUNIDADES END 
            END AS CANTIDAD,

            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ISUBTOTAJUORG * -1 ELSE D.ISUBTOTAJUORG END AS PRECIO_TOTAL_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ISUBTOTAJUFUN * -1 ELSE D.ISUBTOTAJUFUN END AS PRECIO_TOTAL_FUNC,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTCOSTOFUN * -1 ELSE D.ITOTCOSTOFUN END AS COSTO_TOTAL,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.IPVPAJUORG * -1 ELSE D.IPVPAJUORG END AS PVP_UNIT_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.IPVPAJUFUN * -1 ELSE D.IPVPAJUFUN END AS PVP_UNIT_FUNC,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTPVPAJUORG * -1 ELSE D.ITOTPVPAJUORG END AS PVP_UNIT_TOTAL_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTPVPAJUFUN * -1 ELSE D.ITOTPVPAJUFUN END AS PVP_UNIT_TOTAL_FUNC,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ICANALAJUORG * -1 ELSE D.ICANALAJUORG END AS CANAL_UNIT_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ICANALAJUFUN * -1 ELSE D.ICANALAJUFUN END AS CANAL_UNIT_FUNC,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTCANALAJUORG * -1 ELSE D.ITOTCANALAJUORG END AS CANAL_TOTAL_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTCANALAJUFUN * -1 ELSE D.ITOTCANALAJUFUN END AS CANAL_TOTAL_FUNC,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.IDESCAJUORG * -1 ELSE D.IDESCAJUORG END AS OTROS_UNIT_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.IDESCAJUFUN * -1 ELSE D.IDESCAJUFUN END AS OTROS_UNIT_FUNC,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTDESCAJUORG * -1 ELSE D.ITOTDESCAJUORG END AS OTROS_TOTAL_ORIG,
            CASE WHEN A.CTIPODOCUMENTO = 'DV' THEN D.ITOTDESCAJUFUN * -1 ELSE D.ITOTDESCAJUFUN END AS OTROS_TOTAL_FUNC,

            D.ICOSTOPRMFUN AS COSTO_UNITARIO_FUNC,
            ISNULL(A.CDOCUMENTOVTAANULACION, '') AS DOC_ANULADO,
            CASE 
                WHEN RTRIM(LTRIM(A.CTIPODOCUMENTOORG)) = 'OV' 
                THEN LTRIM(RTRIM(ISNULL(A.CDOCUMENTOVTACABORG, ''))) 
                ELSE '' 
            END AS DOC_ORIGEN,
            RTRIM(LTRIM(A.CORDENCOMPRACLIENTE)) AS NUMERO_ORDEN

        FROM 
            CTE_VTDOCUMENTOVTACAB A (NOLOCK)
            INNER JOIN MAEGC_CFESTADO B (NOLOCK) 
                ON A.EDOCUMENTOVTACAB = B.CESTADO AND B.CTABLA = 'DV'
            INNER JOIN MAEGC_CFTIPODOCUMENTO C (NOLOCK) 
                ON A.CTIPODOCUMENTO = C.CTIPODOCUMENTO
            INNER JOIN CTE_VTDOCUMENTOVTADET D (NOLOCK) 
                ON A.CDOCUMENTOVTACAB = D.CDOCUMENTOVTACAB AND A.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            INNER JOIN RM00101 E (NOLOCK) 
                ON A.CCLIENTE = E.CUSTNMBR
            LEFT JOIN RM00201 F (NOLOCK) 
                ON E.CUSTCLAS = F.CLASSID
            INNER JOIN MAEGC_CFCONCEPTO G (NOLOCK) 
                ON A.CCONCEPTO = G.CCONCEPTO
            LEFT JOIN CTE_DISTRIBUCION_CONTABLE H 
                ON H.CDOCUMENTO = D.CDOCUMENTOVTACAB
                AND H.CTIPODOCUMENTO = D.CTIPODOCUMENTO
            INNER JOIN MAEGC_PRODUCTO N (NOLOCK) 
                ON N.CPRODUCTO = D.CPRODUCTO
            LEFT JOIN MAEGC_MARCA O (NOLOCK) 
                ON N.CMARCA = O.CMARCA 

        WHERE 
            A.CTIPODOCUMENTO IN ('FT','DV')
            AND A.EDOCUMENTOVTACAB <> 'NVO'

        GROUP BY
            A.EREBATE,
            B.DESTADO,
            A.EDOCUMENTOVTACAB,
            C.DTIPODOCUMENTO,
            G.DCONCEPTO,
            A.CDIRECCION,
            A.DDIRECCION,
            A.CSERIE,
            A.CCORRELATIVO,
            O.DMARCA,
            D.CPRODUCTO,
            D.DPRODUCTO,
            N.CCATEGORIA,
            A.CCLIENTE,
            A.DRAZONSOCIAL,
            F.CLASDSCR,
            A.FEMISION,
            A.FENTREGAORDEN,
            A.CDOCUMENTOVTAANULACION,
            A.CTIPODOCUMENTOORG,
            A.CDOCUMENTOVTACABORG,
            A.CORDENCOMPRACLIENTE,
            D.NUNIDADES,
            D.ISUBTOTAJUORG,
            D.ISUBTOTAJUFUN,
            D.ITOTCOSTOFUN,
            D.IPVPAJUORG,
            D.IPVPAJUFUN,
            D.ITOTPVPAJUORG,
            D.ITOTPVPAJUFUN,
            D.ICANALAJUORG,
            D.ICANALAJUFUN,
            D.ITOTCANALAJUORG,
            D.ITOTCANALAJUFUN,
            D.IDESCAJUORG,
            D.IDESCAJUFUN,
            D.ITOTDESCAJUORG,
            D.ITOTDESCAJUFUN,
            D.ICOSTOPRMFUN,
            A.CTIPODOCUMENTO;
        """




        # query es el nombre de origen
        query = """
        WITH CTE_VTDOCUMENTOVTACAB AS (
            SELECT A.*
            FROM MOVGC_VTDOCUMENTOVTACAB A WITH (NOLOCK)
            WHERE A.CTIPODOCUMENTO IN ('FT', 'DV')
              AND A.EDOCUMENTOVTACAB <> 'NVO'
              AND A.FEMISION >= CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 31, 0) AS DATE)
        ),
        CTE_VTDOCUMENTOVTADET AS (
            SELECT A.*
            FROM MOVGC_VTDOCUMENTOVTADET A WITH (NOLOCK)
            INNER JOIN CTE_VTDOCUMENTOVTACAB B 
                ON A.CDOCUMENTOVTACAB = B.CDOCUMENTOVTACAB
               AND A.CTIPODOCUMENTO = B.CTIPODOCUMENTO
        ),
        CTE_CUENTAS AS (
            SELECT 
                CDOCUMENTO,
                CTIPODOCUMENTO,
                MAX(CASE WHEN CTIPODISTRIBUCION = 8  THEN CCUENTAHOMOLOGADA END) AS CUENTA_COSTO,
                MAX(CASE WHEN CTIPODISTRIBUCION IN (9, 18, 19) THEN CCUENTAHOMOLOGADA END) AS CUENTA_VENTAS,
                MAX(CASE WHEN CTIPODISTRIBUCION = 10 THEN CCUENTAHOMOLOGADA END) AS CUENTA_DESCUENTO
            FROM MOVGC_DOCUMENTOXDISTRIBUCION WITH (NOLOCK)
            WHERE CTIPODISTRIBUCION IN (8, 9, 10, 18, 19)
              AND CCATEGORIA IN ('CVTAG', 'CVTANG', 'CVTA', 'VTASG', 'VTASNG', 'DVLSG', 'DVLSNG', 'VTAS', 'DSCG', 'DSCNG', 'DSC')
            GROUP BY CDOCUMENTO, CTIPODOCUMENTO
        )
        SELECT
            RTRIM(D.CPRODUCTO) AS CODIGO_PRODUCTO,
            RTRIM(D.DPRODUCTO) AS NOMBRE_PRODUCTO,
            ISNULL(RTRIM(F.CLASDSCR), '') AS CLASE_CLIENTE,
            A.FEMISION AS FECHA_DOCUMENTO,   
            CASE 
                WHEN A.EREBATE = 0 THEN 
                    CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.NUNIDADES * -1 ELSE D.NUNIDADES END 
                ELSE 0 
            END AS CANTIDAD,
            CASE A.CTIPODOCUMENTO WHEN 'DV' THEN D.ISUBTOTAJUORG * -1 ELSE D.ISUBTOTAJUORG END AS PRECIO_TOTAL_ORIG,
            B.DESTADO AS ESTADO_DOCUMENTO
        FROM CTE_VTDOCUMENTOVTACAB A
        INNER JOIN MAEGC_CFESTADO B WITH (NOLOCK)
            ON A.EDOCUMENTOVTACAB = B.CESTADO AND B.CTABLA = 'DV'
        INNER JOIN CTE_VTDOCUMENTOVTADET D
            ON A.CDOCUMENTOVTACAB = D.CDOCUMENTOVTACAB
           AND A.CTIPODOCUMENTO = D.CTIPODOCUMENTO
        INNER JOIN RM00101 E WITH (NOLOCK)
            ON A.CCLIENTE = E.CUSTNMBR
        LEFT JOIN RM00201 F WITH (NOLOCK)
            ON E.CUSTCLAS = F.CLASSID
        INNER JOIN MAEGC_PRODUCTO N WITH (NOLOCK)
            ON N.CPRODUCTO = D.CPRODUCTO
        WHERE A.CTIPODOCUMENTO IN ('FT','DV')
          AND A.EDOCUMENTOVTACAB <> 'NVO'
          AND A.FEMISION >= CAST(DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 31, 0) AS DATE);
        """
       
        leer = 1
        if leer ==1:
             df = load_csv_from_onedrive("https://1drv.ms/x/c/dc44eeb8f33dc5b8/EcDIdi0xIWNMjI630nuNjQsBgMr_B1R9ojE-Vam5ZSZ_OQ?e=4UvUUr")
           
        else:    
            df = pd.read_sql(query, conn)
        
        
        #conn.close()
        df = df[df['CANTIDAD'] != 0]
        # Convertir fecha y crear columnas de año/mes
        df['FECHA_DOCUMENTO'] = pd.to_datetime(df['FECHA_DOCUMENTO'])
        df['AÑO'] = df['FECHA_DOCUMENTO'].dt.year
        df['MES_NUM'] = df['FECHA_DOCUMENTO'].dt.month
        df['MES'] = df['FECHA_DOCUMENTO'].dt.strftime('%B')  # Nombre del mes en inglés; usa locale si necesitas español

        return df

    except Exception as e:
        st.error(f"❌ Error al conectar con la base de datos: {e}")
        return pd.DataFrame()

# ==============================
# 2. CARGAR DATOS
# ==============================
# Inicio del proceso
inicio = time.perf_counter()
df_original = cargar_datos()

if df_original.empty:
    st.stop()

# ==============================
# 3. CREAR FILTROS
# ==============================
st.sidebar.header("_filtros_")

# Años únicos
años = sorted(df_original['AÑO'].dropna().unique())
año_seleccionado = st.sidebar.multiselect("Año", options=años, default=años)

# Meses únicos (por nombre)
meses = sorted(df_original['MES'].dropna().unique())
mes_seleccionado = st.sidebar.multiselect("Mes", options=meses, default=meses)

# Código de producto
codigos = sorted(df_original['CODIGO_PRODUCTO'].dropna().unique())
codigo_seleccionado = st.sidebar.multiselect(
    "Código de Producto",
    options=codigos,
    default=None,
    placeholder="Escriba o seleccione..."
)

# Clase de cliente
clases = sorted(df_original['CLASE_CLIENTE'].dropna().unique())
clase_seleccionada = st.sidebar.multiselect(
    "Clase de Cliente",
    options=clases,
    default=clases
)

# Aplicar filtros
df_filtrado = df_original.copy()
df_filtrado = df_filtrado[df_filtrado['AÑO'].isin(año_seleccionado)]
df_filtrado = df_filtrado[df_filtrado['MES'].isin(mes_seleccionado)]

if codigo_seleccionado:
    df_filtrado = df_filtrado[df_filtrado['CODIGO_PRODUCTO'].isin(codigo_seleccionado)]

df_filtrado = df_filtrado[df_filtrado['CLASE_CLIENTE'].isin(clase_seleccionada)]

# ==============================
# 4. AGRUPAR RESULTADOS
# ==============================
if not df_filtrado.empty:
    df_agrupado = df_filtrado.groupby([
        'CODIGO_PRODUCTO', 'AÑO', 'MES_NUM', 'MES'
    ]).agg({
        'FECHA_DOCUMENTO': 'first',
        'NOMBRE_PRODUCTO': 'first',
        'CLASE_CLIENTE': 'first',
        'CANTIDAD': 'sum',
        'PRECIO_TOTAL_ORIG': 'sum',
        'ESTADO_DOCUMENTO': 'first'
    }).reset_index().sort_values(['AÑO', 'MES_NUM', 'CODIGO_PRODUCTO'])

    # Seleccionar columnas finales en el orden solicitado
    df_final = df_agrupado[[
        'FECHA_DOCUMENTO',
        'CODIGO_PRODUCTO',
        'NOMBRE_PRODUCTO',
        'CLASE_CLIENTE',
        'CANTIDAD',
        'PRECIO_TOTAL_ORIG',
        'ESTADO_DOCUMENTO'
    ]]

    df_final = df_final[df_final['CANTIDAD'] != 0]
    df_final = df_final[df_final['PRECIO_TOTAL_ORIG'] != 0]
    # 🔢 Calcular total dinámico
    total_ventas = df_final['PRECIO_TOTAL_ORIG'].sum()
    # === COLUMNA IZQUIERDA: Métricas ===
    
    
    # Obtener el valor máximo
    max_valor = df_final['PRECIO_TOTAL_ORIG'].max()
    # Encontrar la fila con el precio_total_orig MÁXIMO
    fila_max = df_final.loc[df_final['PRECIO_TOTAL_ORIG'].idxmax()]

    # Obtener el valor mínimo
    min_valor = df_final['PRECIO_TOTAL_ORIG'].min()
    # Encontrar la fila con el precio_total_orig MÍNIMO
    fila_min = df_final.loc[df_final['PRECIO_TOTAL_ORIG'].idxmin()]

    # Mostrar totalizador destacado
    text_izq, text_der = st.columns([1, 2])
    with text_izq:
        st.markdown(f"##### 💰 Total de Ventas: **${total_ventas:,.2f}**")
        st.markdown(f"📈 Valor maximo de ventas agrupada: {max_valor:,.2f}")
        st.markdown(f"📉 Valor mínimo de ventas agrupada: {colorear_valor(min_valor)}", unsafe_allow_html=True)
    with text_der:
        st.markdown(f"##### 🔢 Cantidad de Items: **{len(df_final)}**")
        st.markdown(f"corresponde a {fila_max['NOMBRE_PRODUCTO']}")
        st.markdown(f"corresponde a **{fila_min['NOMBRE_PRODUCTO']}**")
    #st.markdown(f"📉 Valor minimo de ventas agrupada: {min_valor:,.2f}  corresponde a {fila_min['NOMBRE_PRODUCTO']}")
    
    col_izq, col_der = st.columns([1, 2])
    # === COLUMNA DERECHA: Gráfico ===
    with col_izq:
        # Crear figura
        fig_1 = go.Figure()

        # Añadir puntos y líneas
        fig_1.add_trace(go.Scatter(
            x=df_final['FECHA_DOCUMENTO'],
            y=df_final['PRECIO_TOTAL_ORIG'],
            mode='markers',
            name='Ventas',
            text=df_final['NOMBRE_PRODUCTO'],  # Esto aparece en el tooltip
            hovertemplate=(
                "<b>%{text}</b><br>" +
                "Fecha: %{x|%Y-%m-%d}<br>" +
                "Monto: $%{y:,.2f}<br>" +
               "Código: %{customdata}<extra></extra>"
            ),
            customdata=df_final['CODIGO_PRODUCTO'],
            line=dict(color='steelblue'),
            marker=dict(size=4)
        ))
        fig_1.update_layout(
            title="Ventas Individuales",
            xaxis_title="Fecha",
            yaxis_title="Precio Total",
            height=400
        )
    
        st.plotly_chart(fig_1, use_container_width=True)

    with col_der:
        fig_violin = go.Figure()
        fig_violin.add_trace(go.Violin(
            y=df_final['PRECIO_TOTAL_ORIG'],
            box_visible=True,
            meanline_visible=True,
            points='all',
            pointpos=0,
            jitter=0.5,
            name='Distribución de Ventas',
            hovertemplate="Monto: $%{y:,.2f}<extra></extra>",
            fillcolor='lightblue',
            line_color='steelblue'
        ))

        # Líneas para máximo y mínimo
        fig_violin.add_hline(
            y=max_valor,
            line=dict(color='green', dash='dot', width=1.5),
            annotation_text=f"Máx: ${max_valor:,.2f}",
            annotation_position="top right"
        )
        fig_violin.add_hline(
            y=min_valor,
            line=dict(color='red', dash='dot', width=1.5),
            annotation_text=f"Mín: ${min_valor:,.2f}",
            annotation_position="bottom right"
        )

        fig_violin.update_layout(
            title="Distribución de Ventas (Violín)",
            yaxis_title="Precio Total",
            height=400,
            template="plotly_white"
        )
        
        st.plotly_chart(fig_violin, use_container_width=True)  # ✅ Esta ya estaba

##################
###    Graficos agregados
#####################

    new_izq, new_der = st.columns([1, 2])
    with new_izq:
        #fig = px.histogram(df_final, x=df_final['PRECIO_TOTAL_ORIG'], nbins=20)
        fig_0 = px.funnel(df_final, x=df_final['PRECIO_TOTAL_ORIG'], y=df_final['FECHA_DOCUMENTO'])
        fig_0.update_layout(
            title="Ventas Individuales (Embudo)",
            xaxis_title="Precio Total",
            yaxis_title="Fecha",
            height=400
        )
        st.plotly_chart(fig_0, use_container_width=True)
    with new_der:
        fig_2 = px.box(df_final, y=df_final['FECHA_DOCUMENTO'], x=df_final['PRECIO_TOTAL_ORIG'])
        fig_2.update_layout(
            title="Ventas Individuales (Caja)",
            xaxis_title="Precio Total",
            yaxis_title="Fecha",
            height=400
        )
        st.plotly_chart(fig_2, use_container_width=True)

    # Selector de tamaño de fuente
    ##font_size = st.slider('Tamaño de fuente:', min_value=8, max_value=20, value=12)
    ##header_size = st.slider('Tamaño de encabezados:', min_value=10, max_value=24, value=14)

    # Aplicar CSS dinámico
    ##st.markdown(f"""
    ##<style>
    ##.dataframe {{
    ##    font-size: {font_size}px !important;
    ##}}
    ##.dataframe thead th {{
    ##    font-size: {header_size}px !important;
    ##    background-color: #f0f2f6;
    ##}}
    ##</style>
    ##""", unsafe_allow_html=True)

# Mostrar DataFrame

    df_izq, df_der = st.columns([1, 2])
    with df_izq:
        st.subheader(f"📋Resultados Filtrados {len(df_final)}")
        #set_dataframe_font_size(font_size=6, header_size=8)   
        # st.dataframe(df_final, use_container_width=True)
        st.dataframe(df_final, width="stretch")
        
    with df_der:
        
        df_agrupados = df_final.groupby([
                'CODIGO_PRODUCTO'
            ]).agg({'NOMBRE_PRODUCTO': 'first',
                'CLASE_CLIENTE': 'first',
                'CANTIDAD': 'sum',
                'PRECIO_TOTAL_ORIG': 'sum'
            }).reset_index().sort_values(['CANTIDAD'])
        df_agrupados=df_agrupados[df_agrupados['CANTIDAD'] > 0]
        st.subheader(f"📋Resultados Filtrados Agrupado por Articulo {len(df_agrupados)}")
        #set_dataframe_font_size(font_size=6, header_size=8)
        #st.dataframe(df_agrupados, use_container_width=True)
        st.dataframe(df_agrupados, width="stretch")
    
    fin = time.perf_counter()
    duracion = fin - inicio
    minutos, segundos = divmod(duracion, 60)
    #print(f"Tiempo total del proceso: {int(minutos)} min {segundos:.1f} seg")
    st.markdown(f"##### Tiempo total del proceso: **{int(minutos)} min {segundos:.1f} seg**")
    #print(f"Tiempo total del proceso: {duracion:.2f} segundos")
    # ==============================
    # 5. EXPORTAR A CSV
    # ==============================
    st.subheader("📤 Exportar Resultados")

    # Sugerir nombre de archivo
    años_str = "_".join(map(str, sorted(año_seleccionado))) if len(año_seleccionado) <= 3 else "varios_años"
    meses_str = "_".join([m[:3].lower() for m in sorted(mes_seleccionado)]) if len(mes_seleccionado) <= 3 else "varios_meses"
    nombre_default = f"ventas_{años_str}_{meses_str}.csv"

    nombre_archivo = st.text_input("Nombre del archivo (.csv)", value=nombre_default)

    if st.button("📥 Generar y Descargar CSV"):
        if not nombre_archivo.endswith(".csv"):
            nombre_archivo += ".csv"

        # Crear buffer en memoria
        buffer = io.BytesIO()
        df_final.to_csv(buffer, index=False, encoding='utf-8-sig')
        buffer.seek(0)

        st.download_button(
            label="⬇️ Descargar Archivo",
            data=buffer,
            file_name=nombre_archivo,
            mime="text/csv"
        )
else:
    st.warning("⚠️ No hay datos que coincidan con los filtros seleccionados.")
