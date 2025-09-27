import os
import re
import pandas as pd
from pathlib import Path
from datetime import datetime
import time
import requests
import xml.etree.ElementTree as ET
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# -------------------------
# CONFIGURACIÓN
# -------------------------
RUTA_TXT_CARPETA = r"C:\Users\Acer\OneDrive\Documentos\guardado_datos\txt"
RUTA_OUTPUT = Path(r"C:\Users\Acer\OneDrive\Documentos\guardado_datos")

# Crear carpeta de salida si no existe
RUTA_OUTPUT.mkdir(parents=True, exist_ok=True)

# Configurar logging
log_path = RUTA_OUTPUT / "sri_processing.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# -------------------------
# FUNCIONES UTILITARIAS
# -------------------------
def limpiar_nombre(nombre):
    return re.sub(r'[\\/*?:"<>|\t]', "_", str(nombre).strip())

def guardar_xml(xml_content, ruta_output, emisor, receptor, tipo_comprobante="FACTURA", clave_acceso=""):
    try:
        carpeta_emisor = ruta_output / tipo_comprobante / "EMISOR" / limpiar_nombre(emisor)
        carpeta_receptor = ruta_output / tipo_comprobante / "RECEPTOR" / limpiar_nombre(receptor)

        carpeta_emisor.mkdir(parents=True, exist_ok=True)
        carpeta_receptor.mkdir(parents=True, exist_ok=True)

        archivo_emisor = carpeta_emisor / f"{clave_acceso}.xml"
        archivo_receptor = carpeta_receptor / f"{clave_acceso}.xml"

        with open(archivo_emisor, "w", encoding="utf-8") as f:
            f.write(xml_content)

        with open(archivo_receptor, "w", encoding="utf-8") as f:
            f.write(xml_content)

        logging.info(f"XML guardado en: {archivo_emisor} y {archivo_receptor}")
        return True
    except Exception as e:
        logging.error(f"Error guardando XML: {e}")
        return False

def leer_todos_txt(carpeta):
    txt_files = [f for f in Path(carpeta).glob("*.txt")]
    dataframes = []
    for file in txt_files:
        try:
            df = pd.read_csv(file, sep="\t", encoding="latin-1", dtype=str)
            df['CLAVE_ACCESO'] = df['CLAVE_ACCESO'].str.strip()
            df['RUC_EMISOR'] = df['RUC_EMISOR'].str.strip()
            df['IDENTIFICACION_RECEPTOR'] = df['IDENTIFICACION_RECEPTOR'].str.strip()
            dataframes.append(df)
            logging.info(f"Archivo leído: {file}")
        except Exception as e:
            logging.error(f"Error leyendo {file}: {e}")
    if dataframes:
        return pd.concat(dataframes, ignore_index=True)
    return pd.DataFrame()

# -------------------------
# CLASE SRI CLIENT
# -------------------------
class SRIClient:
    def __init__(self):
        self.url_autorizacion = "https://cel.sri.gob.ec/comprobantes-electronicos-ws/AutorizacionComprobantesOffline"
        self.headers = {
            'Content-Type': 'text/xml; charset=utf-8',
            'SOAPAction': ''
        }

    def crear_soap_envelope(self, clave_acceso):
        return f"""<?xml version="1.0" encoding="UTF-8"?>
<soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" 
                  xmlns:ec="http://ec.gob.sri.ws.autorizacion">
   <soapenv:Header/>
   <soapenv:Body>
      <ec:autorizacionComprobante>
         <claveAccesoComprobante>{clave_acceso}</claveAccesoComprobante>
      </ec:autorizacionComprobante>
   </soapenv:Body>
</soapenv:Envelope>"""

    def consultar_autorizacion(self, clave_acceso):
        soap_body = self.crear_soap_envelope(clave_acceso)
        for intento in range(3):
            try:
                logging.info(f"Consultando clave: {clave_acceso} (Intento {intento + 1})")
                response = requests.post(
                    self.url_autorizacion,
                    data=soap_body,
                    headers=self.headers,
                    timeout=30
                )
                if response.status_code == 200:
                    return self.procesar_respuesta_autorizacion(response.text)
                else:
                    logging.warning(f"HTTP {response.status_code}: {response.text}")
            except requests.exceptions.RequestException as e:
                logging.error(f"Error en petición: {e}")
                time.sleep(2 ** intento)
        return "", "NO_AUTORIZADO"

    def procesar_respuesta_autorizacion(self, xml_response):
        """
        Extrae el estado de la autorización y el XML original del comprobante.
        """
        try:
            root = ET.fromstring(xml_response)
            estado_elem = root.find(".//autorizacion/estado")
            estado = estado_elem.text.upper() if estado_elem is not None else "NO_AUTORIZADO"

            comprobante_elem = root.find(".//autorizacion/comprobante")
            xml_comprobante = ET.tostring(comprobante_elem, encoding="utf-8", method="xml").decode("utf-8") if comprobante_elem is not None else ""

            return xml_comprobante, estado
        except ET.ParseError as e:
            logging.error(f"Error parseando XML: {e}")
            return "", "NO_AUTORIZADO"

# -------------------------
# FUNCIONES PRINCIPALES
# -------------------------
def procesar_fila(fila, sri_client):
    clave_acceso = fila["CLAVE_ACCESO"]
    emisor = fila["RUC_EMISOR"]
    receptor = fila["IDENTIFICACION_RECEPTOR"]
    tipo_comprobante = fila.get("TIPO_COMPROBANTE", "FACTURA")

    xml, estado = sri_client.consultar_autorizacion(clave_acceso)
    if xml:
        guardar_xml(xml, RUTA_OUTPUT, emisor, receptor, tipo_comprobante, clave_acceso)

    return {
        "clave": clave_acceso,
        "tipo": tipo_comprobante,
        "estado": estado
    }

def procesar():
    df = leer_todos_txt(RUTA_TXT_CARPETA)
    if df.empty:
        logging.warning("No se encontraron archivos TXT o están vacíos.")
        return

    sri_client = SRIClient()
    resultados = []

    # Procesamiento concurrente con 5 hilos
    with ThreadPoolExecutor(max_workers=5) as executor:
        futuros = [executor.submit(procesar_fila, fila, sri_client) for _, fila in df.iterrows()]
        for futuro in as_completed(futuros):
            resultados.append(futuro.result())

    df_resumen = pd.DataFrame(resultados)
    resumen = df_resumen.groupby(["tipo", "estado"]).size().reset_index(name="cantidad")
    print("\n=== RESUMEN ===")
    print(resumen)
    resumen.to_excel(RUTA_OUTPUT / "resumen.xlsx", index=False)
    logging.info(f"Resumen guardado en: {RUTA_OUTPUT / 'resumen.xlsx'}")

# -------------------------
# EJECUCIÓN
# -------------------------
if __name__ == "__main__":
    procesar()
