import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
import io
import base64
import PyPDF2
import json
import sqlite3
from datetime import datetime

# Configuration de la page
st.set_page_config(
    page_title="Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Styles CSS pour améliorer l'apparence
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #1E88E5;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .success-message {
        background-color: #D4EDDA;
        color: #155724;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .stButton button {
        background-color: #1E88E5;
        color: white;
    }
    .stButton button:hover {
        background-color: #0D47A1;
    }
</style>
""", unsafe_allow_html=True)

# Base de données SQLite
def init_db():
    conn = sqlite3.connect('dashboard_data.db')
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS uploads (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        filename TEXT,
        upload_date TEXT,
        file_type TEXT,
        data_preview TEXT
    )
    ''')
    conn.commit()
    conn.close()

def save_upload_to_db(filename, file_type, data_preview):
    conn = sqlite3.connect('dashboard_data.db')
    c = conn.cursor()
    c.execute("INSERT INTO uploads (filename, upload_date, file_type, data_preview) VALUES (?, ?, ?, ?)",
              (filename, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), file_type, data_preview))
    conn.commit()
    conn.close()

def get_uploads():
    conn = sqlite3.connect('dashboard_data.db')
    df = pd.read_sql_query("SELECT * FROM uploads ORDER BY upload_date DESC", conn)
    conn.close()
    return df

# Initialiser la base de données
init_db()

# Fonctions d'extraction de données
def extract_from_excel(file):
    try:
        df = pd.read_excel(file)
        return df
    except Exception as e:
        st.error(f"Erreur lors de l'extraction du fichier Excel: {e}")
        return None

def extract_from_csv(file, encoding='utf-8'):
    try:
        # Essayer différents séparateurs
        for sep in [',', ';', '\t']:
            try:
                df = pd.read_csv(file, sep=sep, encoding=encoding)
                if len(df.columns) > 1:  # S'assurer que les données sont correctement séparées
                    return df
            except:
                pass
        
        st.error("Impossible de déterminer le séparateur du fichier CSV")
        return None
    except Exception as e:
        st.error(f"Erreur lors de l'extraction du fichier CSV: {e}")
        return None

def extract_from_pdf(file):
    try:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ""
        for page in pdf_reader.pages:
            text += page.extract_text()
            
        # Tenter de structurer les données extraites (simpliste)
        lines = text.split('\n')
        data = []
        headers = []
        
        # Chercher des structures tabulaires
        for line in lines:
            if not headers and ',' in line:
                headers = [h.strip() for h in line.split(',')]
            elif headers and ',' in line:
                values = [v.strip() for v in line.split(',')]
                if len(values) == len(headers):
                    data.append(values)
        
        if headers and data:
            return pd.DataFrame(data, columns=headers)
        else:
            st.warning("Données tabulaires non trouvées dans le PDF. Affichage du texte brut.")
            return pd.DataFrame({'Texte': [text]})
    except Exception as e:
        st.error(f"Erreur lors de l'extraction du fichier PDF: {e}")
        return None

# Fonctions pour générer des visualisations
def generate_basic_stats(df):
    stats = {}
    # Nombre de lignes et colonnes
    stats["Nombre de lignes"] = df.shape[0]
    stats["Nombre de colonnes"] = df.shape[1]
    
    # Types de données
    stats["Types de données"] = df.dtypes.astype(str).to_dict()
    
    # Valeurs manquantes
    stats["Valeurs manquantes"] = df.isna().sum().to_dict()
    
    return stats

def auto_generate_charts(df):
    charts = []
    
    # Pour chaque colonne numérique, créer un histogramme
    for col in df.select_dtypes(include=['number']).columns:
        fig = px.histogram(df, x=col, title=f"Distribution de {col}")
        charts.append(("histogram", col, fig))
    
    # Pour chaque paire de colonnes numériques, créer un scatter plot
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    if len(numeric_cols) >= 2:
        fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1], 
                         title=f"{numeric_cols[0]} vs {numeric_cols[1]}")
        charts.append(("scatter", f"{numeric_cols[0]}_vs_{numeric_cols[1]}", fig))
    
    # Pour chaque colonne catégorielle, créer un bar chart
    for col in df.select_dtypes(include=['object', 'category']).columns:
        if df[col].nunique() < 15:  # Limiter aux colonnes avec moins de 15 valeurs uniques
            value_counts = df[col].value_counts().head(10)
            fig = px.bar(x=value_counts.index, y=value_counts.values, 
                         title=f"Top 10 valeurs pour {col}")
            fig.update_xaxes(title=col)
            fig.update_yaxes(title="Fréquence")
            charts.append(("bar", col, fig))
    
    return charts

def generate_powerbi_template(df):
    # Cette fonction génère un fichier de configuration pour PowerBI
    # Dans un vrai projet, vous pourriez utiliser l'API PowerBI pour une intégration directe
    template = {
        "version": "1.0",
        "datasets": [
            {
                "name": "Dashboard Data",
                "tables": []
            }
        ]
    }
    
    # Ajouter structure de la table
    table = {
        "name": "MainData",
        "columns": []
    }
    
    for col_name, dtype in zip(df.columns, df.dtypes):
        data_type = "string"
        if np.issubdtype(dtype, np.number):
            data_type = "number"
        elif np.issubdtype(dtype, np.datetime64):
            data_type = "datetime"
        
        table["columns"].append({
            "name": col_name,
            "dataType": data_type
        })
    
    template["datasets"][0]["tables"].append(table)
    
    return json.dumps(template, indent=2)

def get_powerbi_download_link(template):
    b64 = base64.b64encode(template.encode()).decode()
    return f'<a href="data:application/json;base64,{b64}" download="powerbi_template.json">Télécharger le template PowerBI</a>'

def get_csv_download_link(df):
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    return f'<a href="data:file/csv;base64,{b64}" download="data_export.csv">Télécharger les données (CSV)</a>'

# Interface principale
def main():
    st.markdown('<h1 class="main-header">Dashboard Analytics</h1>', unsafe_allow_html=True)
    
    # Barre latérale
    st.sidebar.image("https://via.placeholder.com/150x150.png?text=DA", width=150)
    st.sidebar.markdown("## Menu")
    page = st.sidebar.radio("", ["Importer des données", "Analyser les données", "Historique d'importation"])
    
    if page == "Importer des données":
        render_import_page()
    elif page == "Analyser les données":
        render_analysis_page()
    else:
        render_history_page()

def render_import_page():
    st.markdown('<h2 class="section-header">Importer vos données</h2>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader("Choisissez un fichier", 
                                     type=['csv', 'xlsx', 'xls', 'pdf'],
                                     help="Formats supportés: CSV, Excel, PDF")
    
    if uploaded_file is not None:
        file_details = {"Nom": uploaded_file.name, "Type": uploaded_file.type, "Taille": f"{uploaded_file.size / 1024:.2f} KB"}
        st.write(file_details)
        
        file_extension = uploaded_file.name.split('.')[-1].lower()
        
        # Extraction des données selon le type de fichier
        df = None
        if file_extension in ['xlsx', 'xls']:
            df = extract_from_excel(uploaded_file)
        elif file_extension == 'csv':
            df = extract_from_csv(uploaded_file)
        elif file_extension == 'pdf':
            df = extract_from_pdf(uploaded_file)
        
        if df is not None:
            st.markdown('<div class="success-message">Données importées avec succès!</div>', unsafe_allow_html=True)
            st.markdown('<h3 class="section-header">Aperçu des données</h3>', unsafe_allow_html=True)
            st.dataframe(df.head(10))
            
            # Sauvegarder dans la session
            st.session_state['data'] = df
            st.session_state['filename'] = uploaded_file.name
            
            # Sauvegarder dans la base de données
            save_upload_to_db(uploaded_file.name, file_extension, df.head(5).to_json())
            
            st.markdown('<h3 class="section-header">Actions</h3>', unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Analyser les données", key="analyze_btn"):
                    st.session_state['page'] = "Analyser les données"
                    st.rerun()
            with col2:
                st.markdown(get_csv_download_link(df), unsafe_allow_html=True)

def render_analysis_page():
    st.markdown('<h2 class="section-header">Analyse des données</h2>', unsafe_allow_html=True)
    
    if 'data' not in st.session_state:
        st.warning("Aucune donnée à analyser. Veuillez d'abord importer un fichier.")
        if st.button("Aller à l'importation"):
            st.session_state['page'] = "Importer des données"
            st.rerun()
        return
    
    df = st.session_state['data']
    
    # Onglets pour différentes analyses
    tab1, tab2, tab3, tab4 = st.tabs(["Aperçu", "Statistiques", "Visualisations", "PowerBI"])
    
    with tab1:
        st.markdown(f"### Données de: {st.session_state['filename']}")
        st.dataframe(df)
        
    with tab2:
        st.markdown("### Statistiques descriptives")
        st.write(df.describe())
        
        stats = generate_basic_stats(df)
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Nombre de lignes", stats["Nombre de lignes"])
            st.metric("Nombre de colonnes", stats["Nombre de colonnes"])
        
        st.markdown("### Types de données")
        st.json(stats["Types de données"])
        
        st.markdown("### Valeurs manquantes")
        st.json(stats["Valeurs manquantes"])
    
    with tab3:
        st.markdown("### Visualisations automatiques")
        charts = auto_generate_charts(df)
        
        if not charts:
            st.info("Pas assez de données numériques ou catégorielles pour générer des visualisations automatiques.")
        
        for chart_type, name, fig in charts:
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.markdown("### Exportation PowerBI")
        st.info("Cette section permet de préparer vos données pour PowerBI.")
        
        template = generate_powerbi_template(df)
        st.json(template)
        
        st.markdown(get_powerbi_download_link(template), unsafe_allow_html=True)
        st.markdown("""
        **Pour utiliser ce template dans PowerBI:**
        1. Téléchargez le fichier JSON
        2. Dans PowerBI Desktop, cliquez sur 'Obtenir les données'
        3. Sélectionnez 'JSON' comme source
        4. Importez le fichier téléchargé
        5. Transformez les données selon vos besoins
        """)

def render_history_page():
    st.markdown('<h2 class="section-header">Historique des importations</h2>', unsafe_allow_html=True)
    
    uploads_df = get_uploads()
    
    if uploads_df.empty:
        st.info("Aucun historique d'importation disponible.")
    else:
        st.dataframe(uploads_df[['id', 'filename', 'upload_date', 'file_type']])
        
        # Permettre de recharger une importation précédente
        selected_id = st.selectbox("Sélectionner une importation pour l'analyser", uploads_df['id'].tolist())
        
        if st.button("Recharger cette importation"):
            selected_row = uploads_df[uploads_df['id'] == selected_id].iloc[0]
            # Dans un vrai projet, vous chargeriez les données complètes depuis la base de données
            # Ici on simule avec un dataframe vide pour l'exemple
            st.session_state['data'] = pd.DataFrame(json.loads(selected_row['data_preview']))
            st.session_state['filename'] = selected_row['filename']
            st.session_state['page'] = "Analyser les données"
            st.rerun()

if __name__ == "__main__":
    # Initialiser la navigation par pages si ce n'est pas déjà fait
    if 'page' in st.session_state:
        page = st.session_state['page']
        st.sidebar.radio("", ["Importer des données", "Analyser les données", "Historique d'importation"], 
                        index=["Importer des données", "Analyser les données", "Historique d'importation"].index(page),
                        key="page_radio")
    main()
    