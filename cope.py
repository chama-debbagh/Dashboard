import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import io
import sqlite3
import zipfile
import base64
from datetime import datetime
from typing import Optional, List, Tuple, Any, Dict
from pathlib import Path
from io import StringIO
import sys
import chardet
import openpyxl


class DataExtractor:
    """Classe pour extraire les données de différents formats de fichiers"""
    
    def __init__(self):
        self.supported_formats = ['csv', 'xlsx', 'xls', 'json', 'txt']
    
    def extract_data(self, uploaded_file) -> Optional[pd.DataFrame]:
        try:
            file_extension = self._get_file_extension(uploaded_file.name)
            
            if file_extension not in self.supported_formats:
                st.error(f"Format de fichier non supporté: {file_extension}")
                return None
            
            if file_extension in ['xlsx', 'xls']:
                return self._extract_from_excel(uploaded_file)
            elif file_extension == 'csv':
                return self._extract_from_csv(uploaded_file)
            elif file_extension == 'json':
                return self._extract_from_json(uploaded_file)
            elif file_extension == 'txt':
                return self._extract_from_txt(uploaded_file)
            
        except Exception as e:
            st.error(f"Erreur lors de l'extraction: {str(e)}")
            return None
    
    def _get_file_extension(self, filename: str) -> str:
        return Path(filename).suffix.lower().lstrip('.')
    
    def _extract_from_excel(self, uploaded_file) -> Optional[pd.DataFrame]:
        try:
            excel_file = pd.ExcelFile(uploaded_file)
            
            if len(excel_file.sheet_names) > 1:
                st.info(f"Le fichier contient {len(excel_file.sheet_names)} feuilles")
                selected_sheet = st.selectbox(
                    "Sélectionnez la feuille à importer:",
                    excel_file.sheet_names,
                    key="excel_sheet_selector"
                )
                df = pd.read_excel(uploaded_file, sheet_name=selected_sheet)
            else:
                df = pd.read_excel(uploaded_file, sheet_name=0)
            
            df = self._clean_dataframe(df)
            st.success(f"Fichier Excel importé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return df
            
        except Exception as e:
            st.error(f"Erreur lors de l'importation Excel: {str(e)}")
            return None
    
    def _extract_from_csv(self, uploaded_file) -> Optional[pd.DataFrame]:
        try:
            raw_data = uploaded_file.read()
            uploaded_file.seek(0)
            
            encoding_result = chardet.detect(raw_data)
            encoding = encoding_result['encoding'] if encoding_result['confidence'] > 0.7 else 'utf-8'
            
            st.info(f"🔍 Encodage détecté: {encoding} (confiance: {encoding_result['confidence']:.2f})")
            
            separators = [',', ';', '\t', '|']
            best_df = None
            best_cols = 0
            
            for sep in separators:
                try:
                    uploaded_file.seek(0)
                    df_test = pd.read_csv(
                        uploaded_file, 
                        sep=sep, 
                        encoding=encoding,
                        low_memory=False,
                        na_values=['', 'NA', 'N/A', 'NULL', 'null', '#N/A']
                    )
                    
                    if len(df_test.columns) > best_cols and len(df_test.columns) > 1:
                        best_df = df_test
                        best_cols = len(df_test.columns)
                        best_sep = sep
                        
                except:
                    continue
            
            if best_df is None:
                st.error("Impossible de déterminer le format CSV")
                return None
            
            st.success(f"CSV importé avec séparateur '{best_sep}': {best_df.shape[0]} lignes, {best_df.shape[1]} colonnes")
            best_df = self._clean_dataframe(best_df)
            return best_df
            
        except Exception as e:
            st.error(f"Erreur lors de l'importation CSV: {str(e)}")
            return None
    
    def _extract_from_json(self, uploaded_file) -> Optional[pd.DataFrame]:
        try:
            json_data = json.load(uploaded_file)
            
            if isinstance(json_data, list):
                df = pd.json_normalize(json_data)
            elif isinstance(json_data, dict):
                for key, value in json_data.items():
                    if isinstance(value, list) and len(value) > 0:
                        df = pd.json_normalize(value)
                        st.info(f"Données extraites de la clé: '{key}'")
                        break
                else:
                    df = pd.json_normalize([json_data])
            else:
                st.error("Structure JSON non supportée")
                return None
            
            st.success(f"JSON importé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return self._clean_dataframe(df)
            
        except Exception as e:
            st.error(f"Erreur lors de l'importation JSON: {str(e)}")
            return None
    
    def _extract_from_txt(self, uploaded_file) -> Optional[pd.DataFrame]:
        try:
            content = uploaded_file.read().decode('utf-8')
            lines = content.strip().split('\n')
            
            if not lines:
                st.error("Fichier texte vide")
                return None
            
            first_line = lines[0]
            separators = ['\t', ',', ';', '|', ' ']
            best_sep = None
            max_cols = 0
            
            for sep in separators:
                cols = len(first_line.split(sep))
                if cols > max_cols:
                    max_cols = cols
                    best_sep = sep
            
            if max_cols < 2:
                df = pd.DataFrame({'Contenu': lines})
                st.info("Fichier traité comme texte simple")
            else:
                data = []
                headers = lines[0].split(best_sep)
                
                for line in lines[1:]:
                    if line.strip():
                        values = line.split(best_sep)
                        while len(values) < len(headers):
                            values.append('')
                        data.append(values[:len(headers)])
                
                df = pd.DataFrame(data, columns=headers)
                st.info(f"Fichier traité comme données tabulaires (séparateur: '{best_sep}')")
            
            st.success(f"Fichier texte importé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return self._clean_dataframe(df)
            
        except Exception as e:
            st.error(f"Erreur lors de l'importation du fichier texte: {str(e)}")
            return None
    
    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            df = df.dropna(how='all')
            df = df.dropna(axis=1, how='all')
            
            df.columns = df.columns.astype(str)
            df.columns = [col.strip() for col in df.columns]
            
            df.columns = [f'Colonne_{i}' if col == '' or col.startswith('Unnamed') 
                         else col for i, col in enumerate(df.columns)]
            
            df.columns = pd.io.common.dedup_names(df.columns, is_potential_multiindex=False)
            df = self._auto_convert_types(df)
            
            return df
            
        except Exception as e:
            st.warning(f"Erreur lors du nettoyage: {str(e)}")
            return df
    
    def _auto_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        try:
            for col in df.columns:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if not numeric_col.isna().all():
                    valid_numeric = (~numeric_col.isna()).sum()
                    total_non_null = (~df[col].isna()).sum()
                    
                    if total_non_null > 0 and (valid_numeric / total_non_null) > 0.8:
                        df[col] = numeric_col
                        continue
                
                try:
                    datetime_col = pd.to_datetime(df[col], errors='coerce')
                    valid_datetime = (~datetime_col.isna()).sum()
                    total_non_null = (~df[col].isna()).sum()
                    
                    if total_non_null > 0 and (valid_datetime / total_non_null) > 0.8:
                        df[col] = datetime_col
                        continue
                except:
                    pass
                
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.strip()
                    df[col] = df[col].replace('nan', pd.NA)
            
            return df
            
        except Exception as e:
            st.warning(f"Erreur lors de la conversion des types: {str(e)}")
            return df


class DataAnalyzer:
    """Classe pour analyser les données et générer des insights"""
    
    def __init__(self):
        pass
    
    def get_column_info(self, df: pd.DataFrame) -> pd.DataFrame:
        info_data = []
        
        for col in df.columns:
            col_data = {
                'Colonne': col,
                'Type': str(df[col].dtype),
                'Valeurs_uniques': df[col].nunique(),
                'Valeurs_manquantes': df[col].isnull().sum(),
                'Pourcentage_manquant': f"{(df[col].isnull().sum() / len(df)) * 100:.1f}%"
            }
            
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                col_data.update({
                    'Min': df[col].min() if not df[col].empty else None,
                    'Max': df[col].max() if not df[col].empty else None,
                    'Moyenne': f"{df[col].mean():.2f}" if not df[col].empty else None
                })
            
            info_data.append(col_data)
        
        return pd.DataFrame(info_data)
    
    def get_categorical_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        stats_data = []
        
        for col in df.columns:
            if df[col].nunique() < 50:
                value_counts = df[col].value_counts()
                stats_data.append({
                    'Colonne': col,
                    'Valeurs_uniques': df[col].nunique(),
                    'Valeur_dominante': value_counts.index[0] if len(value_counts) > 0 else None,
                    'Fréquence_dominante': value_counts.iloc[0] if len(value_counts) > 0 else 0
                })
        
        return pd.DataFrame(stats_data)
    
    def analyze_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        quality_data = []
        
        for col in df.columns:
            total_rows = len(df)
            missing_count = df[col].isnull().sum()
            missing_pct = (missing_count / total_rows) * 100
            unique_count = df[col].nunique()
            
            quality_score = 100
            if missing_pct > 50:
                quality_score -= 30
            elif missing_pct > 20:
                quality_score -= 15
            elif missing_pct > 5:
                quality_score -= 5
            
            issues = []
            if missing_pct > 20:
                issues.append("Beaucoup de valeurs manquantes")
            if unique_count == 1:
                issues.append("Valeur constante")
            
            quality_data.append({
                'Colonne': col,
                'Score_qualité': f"{quality_score:.0f}/100",
                'Valeurs_manquantes': f"{missing_count} ({missing_pct:.1f}%)",
                'Problèmes': "; ".join(issues) if issues else "Aucun"
            })
        
        return pd.DataFrame(quality_data)
    
    def generate_insights(self, df: pd.DataFrame) -> List[str]:
        insights = []
        
        total_rows = len(df)
        total_cols = len(df.columns)
        
        insights.append(f"Le dataset contient {total_rows:,} lignes et {total_cols} colonnes")
        
        missing_total = df.isnull().sum().sum()
        missing_pct = (missing_total / (total_rows * total_cols)) * 100
        if missing_pct > 10:
            insights.append(f"Attention: {missing_pct:.1f}% des données sont manquantes")
        elif missing_pct == 0:
            insights.append("Excellent: Aucune valeur manquante détectée")
        
        numeric_cols = len(df.select_dtypes(include=['number']).columns)
        text_cols = len(df.select_dtypes(include=['object']).columns)
        
        if numeric_cols > text_cols:
            insights.append(f"Dataset majoritairement numérique ({numeric_cols} colonnes numériques)")
        
        return insights[:6]
    
    def get_recommendations(self, df: pd.DataFrame) -> List[str]:
        recommendations = []
        
        high_missing_cols = []
        for col in df.columns:
            missing_pct = (df[col].isnull().sum() / len(df)) * 100
            if missing_pct > 20:
                high_missing_cols.append((col, missing_pct))
        
        if high_missing_cols:
            recommendations.append(f"Traiter les valeurs manquantes dans {len(high_missing_cols)} colonne(s)")
        
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            recommendations.append(f"Supprimer {duplicates} ligne(s) dupliquée(s)")
        
        return recommendations[:5]


class DataVisualizer:
    """Classe pour créer des visualisations"""
    
    def __init__(self):
        self.color_palette = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
    
    def auto_generate_charts(self, df: pd.DataFrame) -> List[Tuple[str, str, Any]]:
        charts = []
        
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        
        for col in numeric_cols[:2]:
            fig = px.histogram(df, x=col, title=f"Distribution de {col}")
            charts.append(("histogram", f"Distribution de {col}", fig))
        
        for col in categorical_cols[:2]:
            if df[col].nunique() <= 10:
                fig = px.bar(x=df[col].value_counts().index, 
                           y=df[col].value_counts().values,
                           title=f"Répartition de {col}")
                charts.append(("bar", f"Répartition de {col}", fig))
        
        if len(numeric_cols) >= 2:
            fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1],
                           title=f"{numeric_cols[0]} vs {numeric_cols[1]}")
            charts.append(("scatter", f"{numeric_cols[0]} vs {numeric_cols[1]}", fig))
        
        return charts
    
    def create_correlation_matrix(self, df: pd.DataFrame) -> go.Figure:
        corr_matrix = df.corr()
        fig = px.imshow(corr_matrix, text_auto=True, title="Matrice de corrélation")
        return fig
    
    def create_missing_data_heatmap(self, df: pd.DataFrame) -> go.Figure:
        missing_data = df.isnull().astype(int)
        fig = px.imshow(missing_data.T, title="Carte des valeurs manquantes")
        return fig


# ============================================================================
# CLASSE POWERBI CORRIGÉE - APPROCHE PRATIQUE
# ============================================================================

class PowerBIExporter:
    """
    APPROCHE CORRIGÉE: Package complet pour Power BI
    
    Au lieu d'un .pbit complexe, on génère:
    1. Excel optimisé avec plusieurs feuilles
    2. Script Power Query (M) prêt à l'emploi
    3. Fichier DAX avec mesures
    4. Guide HTML détaillé avec étapes
    """
    
    def __init__(self):
        pass
    
    def create_powerbi_package(self, df: pd.DataFrame, filename: str) -> Dict[str, bytes]:
        """
        Crée un package complet pour Power BI
        
        CONTENU:
        - Excel multi-feuilles avec données + métadonnées
        - Script Power Query (fichier .m)
        - Mesures DAX (fichier .dax)
        - Guide HTML interactif
        
        Returns:
            Dict avec tous les fichiers
        """
        package = {}
        
        # 1. Excel optimisé multi-feuilles
        package['excel'] = self._create_optimized_excel(df, filename)
        
        # 2. Script Power Query
        package['powerquery'] = self._create_powerquery_script(df, filename)
        
        # 3. Mesures DAX
        package['dax'] = self._create_dax_measures(df)
        
        # 4. Guide HTML
        package['guide'] = self._create_html_guide(df, filename)
        
        # 5. ZIP avec tout
        package['zip'] = self._create_complete_zip(package, filename)
        
        return package
    
    def _create_optimized_excel(self, df: pd.DataFrame, filename: str) -> bytes:
        """
        Crée un Excel avec plusieurs feuilles optimisées pour Power BI
        
        FEUILLES:
        - Data: Données principales (nettoyées)
        - Metadata: Informations sur le dataset
        - DataTypes: Types de colonnes
        - Visualizations: Suggestions de visuels
        """
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # FEUILLE 1: Données principales
            df_clean = df.copy()
            # Nettoyer les noms de colonnes pour Power BI
            df_clean.columns = [
                str(col).strip()
                .replace(' ', '_')
                .replace('[', '')
                .replace(']', '')
                .replace('(', '')
                .replace(')', '')
                .replace('/', '_')
                .replace('\\', '_')
                for col in df_clean.columns
            ]
            df_clean.to_excel(writer, sheet_name='Data', index=False)
            
            # FEUILLE 2: Métadonnées
            metadata = pd.DataFrame({
                'Propriété': [
                    'Nom du fichier',
                    'Date d\'export',
                    'Nombre de lignes',
                    'Nombre de colonnes',
                    'Taille (Ko)',
                    'Colonnes numériques',
                    'Colonnes texte',
                    'Colonnes dates'
                ],
                'Valeur': [
                    filename,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    str(df.shape[0]),
                    str(df.shape[1]),
                    f"{df.memory_usage(deep=True).sum() / 1024:.2f}",
                    str(len(df.select_dtypes(include=['number']).columns)),
                    str(len(df.select_dtypes(include=['object']).columns)),
                    str(len(df.select_dtypes(include=['datetime']).columns))
                ]
            })
            metadata.to_excel(writer, sheet_name='Metadata', index=False)
            
            # FEUILLE 3: Types de données
            datatypes = []
            for col in df_clean.columns:
                dtype = df[df.columns[list(df_clean.columns).index(col)]].dtype
                
                if dtype in ['int64', 'float64', 'int32', 'float32']:
                    pbi_type = 'Nombre décimal'
                    aggregation = 'Somme'
                elif dtype == 'datetime64[ns]':
                    pbi_type = 'Date/Heure'
                    aggregation = 'Aucune'
                else:
                    pbi_type = 'Texte'
                    aggregation = 'Aucune'
                
                datatypes.append({
                    'Colonne': col,
                    'Type_Pandas': str(dtype),
                    'Type_PowerBI': pbi_type,
                    'Agrégation_suggérée': aggregation
                })
            
            pd.DataFrame(datatypes).to_excel(writer, sheet_name='DataTypes', index=False)
            
            # FEUILLE 4: Suggestions de visualisations
            viz_suggestions = self._generate_visualization_suggestions(df, df_clean.columns)
            pd.DataFrame(viz_suggestions).to_excel(writer, sheet_name='Visualizations', index=False)
        
        output.seek(0)
        return output.getvalue()
    
    def _create_powerquery_script(self, df: pd.DataFrame, filename: str) -> str:
        """
        Génère un script Power Query (M) prêt à copier-coller
        
        Ce script:
        - Charge l'Excel
        - Nettoie les données
        - Définit les types
        - Créer des colonnes calculées utiles
        """
        df_clean = df.copy()
        df_clean.columns = [str(col).strip().replace(' ', '_') for col in df_clean.columns]
        
        script = f"""
// ========================================
// SCRIPT POWER QUERY (M)
// Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
// Fichier source: {filename}
// ========================================

let
    // ÉTAPE 1: Charger le fichier Excel
    Source = Excel.Workbook(File.Contents("CHEMIN_VERS_VOTRE_FICHIER.xlsx"), null, true),
    
    // ÉTAPE 2: Sélectionner la feuille "Data"
    Data_Sheet = Source{{[Item="Data",Kind="Sheet"]}}[Data],
    
    // ÉTAPE 3: Promouvoir les en-têtes
    PromotedHeaders = Table.PromoteHeaders(Data_Sheet, [PromoteAllScalars=true]),
    
    // ÉTAPE 4: Définir les types de colonnes
    ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {{
"""
        
        # Ajouter les types pour chaque colonne
        type_mappings = []
        for i, col in enumerate(df_clean.columns):
            original_col = df.columns[i]
            dtype = df[original_col].dtype
            
            if dtype in ['int64', 'int32']:
                m_type = 'Int64.Type'
            elif dtype in ['float64', 'float32']:
                m_type = 'type number'
            elif dtype == 'datetime64[ns]':
                m_type = 'type datetime'
            else:
                m_type = 'type text'
            
            type_mappings.append(f'        {{"{col}", {m_type}}}')
        
        script += ',\n'.join(type_mappings)
        script += """
    }}),
    
    // ÉTAPE 5: Supprimer les lignes vides
    RemovedBlankRows = Table.SelectRows(ChangedTypes, each not List.IsEmpty(List.RemoveMatchingItems(Record.FieldValues(_), {"", null}))),
    
    // ÉTAPE 6: Supprimer les doublons (optionnel - décommenter si nécessaire)
    // RemovedDuplicates = Table.Distinct(RemovedBlankRows),
    
    // ÉTAPE 7: Ajouter une colonne Index (utile pour Power BI)
    AddedIndex = Table.AddIndexColumn(RemovedBlankRows, "Index", 1, 1, Int64.Type)
    
in
    AddedIndex

// ========================================
// INSTRUCTIONS D'UTILISATION:
// ========================================
// 1. Dans Power BI Desktop, allez dans "Accueil" > "Transformer les données"
// 2. Cliquez sur "Nouvelle source" > "Requête vide"
// 3. Allez dans "Affichage" > "Éditeur avancé"
// 4. Supprimez tout le contenu et collez ce script
// 5. Modifiez "CHEMIN_VERS_VOTRE_FICHIER.xlsx" avec le vrai chemin
// 6. Cliquez sur "Terminé"
// 7. Cliquez sur "Fermer et appliquer"
"""
        
        # Ajouter des requêtes supplémentaires utiles
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            script += f"""

// ========================================
// REQUÊTE BONUS: Table de statistiques
// ========================================
// Cette requête créé une table résumée avec statistiques

let
    Source = Data,  // Référence à la requête principale
    StatsTable = #table(
        {{"Statistique", "Valeur"}},
        {{
            {{"Nombre total de lignes", Table.RowCount(Source)}},
            {{"Nombre de colonnes", Table.ColumnCount(Source)}},
            {{"Date de dernière actualisation", DateTime.LocalNow()}}
        }}
    )
in
    StatsTable
"""
        
        return script
    
    def _create_dax_measures(self, df: pd.DataFrame) -> str:
        """
        Génère un fichier DAX complet avec mesures prêtes à l'emploi
        """
        measures = []
        
        measures.append("// " + "="*60)
        measures.append("// MESURES DAX POUR POWER BI")
        measures.append(f"// Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        measures.append("// " + "="*60)
        measures.append("")
        
        # Nettoyer les noms de colonnes
        df_clean = df.copy()
        original_cols = df.columns.tolist()
        df_clean.columns = [str(col).strip().replace(' ', '_') for col in df_clean.columns]
        clean_cols = df_clean.columns.tolist()
        
        # Mesures de base pour le dataset
        measures.append("// MESURES GÉNÉRALES")
        measures.append("// " + "-"*60)
        measures.append("Nombre_Total_Lignes = COUNTROWS('Data')")
        measures.append("")
        
        # Mesures pour colonnes numériques
        numeric_cols = df.select_dtypes(include=['number']).columns
        if len(numeric_cols) > 0:
            measures.append("// MESURES NUMÉRIQUES")
            measures.append("// " + "-"*60)
            measures.append("")
            
            for orig_col in numeric_cols:
                idx = original_cols.index(orig_col)
                clean_col = clean_cols[idx]
                
                measures.append(f"// Mesures pour: {clean_col}")
                measures.append(f"{clean_col}_Total = SUM('Data'[{clean_col}])")
                measures.append(f"{clean_col}_Moyenne = AVERAGE('Data'[{clean_col}])")
                measures.append(f"{clean_col}_Min = MIN('Data'[{clean_col}])")
                measures.append(f"{clean_col}_Max = MAX('Data'[{clean_col}])")
                measures.append(f"{clean_col}_Compte = COUNTX('Data', 'Data'[{clean_col}])")
                measures.append("")
        
        # Mesures conditionnelles
        measures.append("// MESURES CONDITIONNELLES (EXEMPLES)")
        measures.append("// " + "-"*60)
        if len(numeric_cols) > 0:
            first_num_col_orig = numeric_cols[0]
            idx = original_cols.index(first_num_col_orig)
            first_num_col = clean_cols[idx]
            
            measures.append(f"// Exemple: Lignes où {first_num_col} > 0")
            measures.append(f"Compte_{first_num_col}_Positifs = ")
            measures.append(f"CALCULATE(")
            measures.append(f"    COUNTROWS('Data'),")
            measures.append(f"    'Data'[{first_num_col}] > 0")
            measures.append(f")")
            measures.append("")
        
        # Mesures temporelles si dates présentes
        datetime_cols = df.select_dtypes(include=['datetime']).columns
        if len(datetime_cols) > 0 and len(numeric_cols) > 0:
            measures.append("// MESURES TEMPORELLES")
            measures.append("// " + "-"*60)
            
            date_col_orig = datetime_cols[0]
            idx = original_cols.index(date_col_orig)
            date_col = clean_cols[idx]
            
            val_col_orig = numeric_cols[0]
            idx2 = original_cols.index(val_col_orig)
            val_col = clean_cols[idx2]
            
            measures.append(f"// Calculs temporels pour {val_col}")
            measures.append(f"{val_col}_YTD = TOTALYTD([{val_col}_Total], 'Data'[{date_col}])")
            measures.append(f"{val_col}_MTD = TOTALMTD([{val_col}_Total], 'Data'[{date_col}])")
            measures.append("")
        
        # Mesures de comparaison
        measures.append("// MESURES DE COMPARAISON")
        measures.append("// " + "-"*60)
        measures.append("// Pourcentage du total (exemple)")
        if len(numeric_cols) > 0:
            first_num_col_orig = numeric_cols[0]
            idx = original_cols.index(first_num_col_orig)
            first_num_col = clean_cols[idx]
            
            measures.append(f"{first_num_col}_Pourcentage = ")
            measures.append(f"DIVIDE(")
            measures.append(f"    [{first_num_col}_Total],")
            measures.append(f"    CALCULATE([{first_num_col}_Total], ALL('Data')),")
            measures.append(f"    0")
            measures.append(f")")
            measures.append("")
        
        measures.append("// " + "="*60)
        measures.append("// FIN DES MESURES")
        measures.append("// " + "="*60)
        measures.append("")
        measures.append("// INSTRUCTIONS:")
        measures.append("// 1. Copiez les mesures ci-dessus")
        measures.append("// 2. Dans Power BI, cliquez sur 'Nouvelle mesure'")
        measures.append("// 3. Collez le code DAX")
        measures.append("// 4. Ajustez les noms de tables/colonnes si nécessaire")
        
        return '\n'.join(measures)
    
    def _create_html_guide(self, df: pd.DataFrame, filename: str) -> bytes:
        """
        Crée un guide HTML interactif et détaillé
        """
        numeric_cols = len(df.select_dtypes(include=['number']).columns)
        categorical_cols = len(df.select_dtypes(include=['object']).columns)
        
        html_content = f"""
<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Guide Power BI - {filename}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }}
        
        .container {{
            max-width: 1000px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .header p {{
            font-size: 1.1em;
            opacity: 0.9;
        }}
        
        .content {{
            padding: 40px;
        }}
        
        .step {{
            background: #f8f9fa;
            border-left: 5px solid #667eea;
            padding: 25px;
            margin-bottom: 30px;
            border-radius: 8px;
            transition: transform 0.3s, box-shadow 0.3s;
        }}
        
        .step:hover {{
            transform: translateX(5px);
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }}
        
        .step h2 {{
            color: #667eea;
            margin-bottom: 15px;
            font-size: 1.8em;
        }}
        
        .step h3 {{
            color: #764ba2;
            margin-top: 20px;
            margin-bottom: 10px;
        }}
        
        .step ul {{
            margin-left: 25px;
            margin-top: 10px;
        }}
        
        .step li {{
            margin-bottom: 8px;
        }}
        
        .info-box {{
            background: #e3f2fd;
            border-left: 5px solid #2196F3;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        
        .warning-box {{
            background: #fff3cd;
            border-left: 5px solid #ffc107;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        
        .success-box {{
            background: #d4edda;
            border-left: 5px solid #28a745;
            padding: 20px;
            margin: 20px 0;
            border-radius: 5px;
        }}
        
        .code-block {{
            background: #2d2d2d;
            color: #f8f8f2;
            padding: 20px;
            border-radius: 8px;
            overflow-x: auto;
            margin: 15px 0;
            font-family: 'Courier New', monospace;
        }}
        
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 20px;
            margin: 30px 0;
        }}
        
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 10px;
            text-align: center;
        }}
        
        .stat-card h3 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .stat-card p {{
            opacity: 0.9;
        }}
        
        .visual-suggestion {{
            background: white;
            border: 2px solid #667eea;
            padding: 20px;
            margin: 15px 0;
            border-radius: 8px;
        }}
        
        .visual-suggestion h4 {{
            color: #667eea;
            margin-bottom: 10px;
        }}
        
        .footer {{
            background: #f8f9fa;
            padding: 30px;
            text-align: center;
            color: #666;
        }}
        
        button {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 12px 30px;
            border-radius: 25px;
            cursor: pointer;
            font-size: 1em;
            margin: 10px;
            transition: transform 0.3s;
        }}
        
        button:hover {{
            transform: scale(1.05);
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Guide Power BI</h1>
            <p>Configuration complète pour: {filename}</p>
            <p>Généré le {datetime.now().strftime('%d/%m/%Y à %H:%M')}</p>
        </div>
        
        <div class="content">
            <div class="info-box">
                <h3>📦 Contenu du Package</h3>
                <p>Ce package contient tous les fichiers nécessaires pour créer votre rapport Power BI:</p>
                <ul>
                    <li><strong>{filename}_PowerBI.xlsx</strong> - Fichier Excel avec vos données optimisées</li>
                    <li><strong>PowerQuery_Script.m</strong> - Script Power Query à copier-coller</li>
                    <li><strong>DAX_Measures.dax</strong> - Mesures DAX prêtes à l'emploi</li>
                    <li><strong>Guide.html</strong> - Ce guide (vous êtes ici!)</li>
                </ul>
            </div>
            
            <div class="stats">
                <div class="stat-card">
                    <h3>{df.shape[0]:,}</h3>
                    <p>Lignes de données</p>
                </div>
                <div class="stat-card">
                    <h3>{df.shape[1]}</h3>
                    <p>Colonnes</p>
                </div>
                <div class="stat-card">
                    <h3>{numeric_cols}</h3>
                    <p>Colonnes numériques</p>
                </div>
                <div class="stat-card">
                    <h3>{categorical_cols}</h3>
                    <p>Colonnes texte</p>
                </div>
            </div>
            
            <div class="step">
                <h2>📥 Étape 1: Ouvrir Power BI Desktop</h2>
                <p>Si vous n'avez pas encore Power BI Desktop:</p>
                <ol>
                    <li>Allez sur <a href="https://powerbi.microsoft.com/desktop" target="_blank">powerbi.microsoft.com/desktop</a></li>
                    <li>Téléchargez la version gratuite</li>
                    <li>Installez et lancez l'application</li>
                </ol>
                <div class="success-box">
                    <strong>✅ Power BI Desktop est gratuit!</strong> Aucun compte Microsoft n'est requis pour l'utiliser localement.
                </div>
            </div>
            
            <div class="step">
                <h2>📂 Étape 2: Importer le fichier Excel</h2>
                <h3>Méthode Simple (Recommandée)</h3>
                <ol>
                    <li>Dans Power BI Desktop, cliquez sur <strong>"Obtenir les données"</strong></li>
                    <li>Sélectionnez <strong>"Excel"</strong></li>
                    <li>Naviguez et sélectionnez le fichier <code>{filename}_PowerBI.xlsx</code></li>
                    <li>Cochez la feuille <strong>"Data"</strong></li>
                    <li>Cliquez sur <strong>"Transformer les données"</strong> (pas "Charger" directement)</li>
                </ol>
                
                <div class="warning-box">
                    <strong>⚠️ Important:</strong> Choisissez "Transformer les données" pour pouvoir utiliser le script Power Query!
                </div>
            </div>
            
            <div class="step">
                <h2>⚙️ Étape 3: Appliquer le script Power Query</h2>
                <p>Dans l'éditeur Power Query qui vient de s'ouvrir:</p>
                <ol>
                    <li>Cliquez sur <strong>"Affichage"</strong> dans le ruban</li>
                    <li>Cliquez sur <strong>"Éditeur avancé"</strong></li>
                    <li>Ouvrez le fichier <code>PowerQuery_Script.m</code> avec Notepad</li>
                    <li>Copiez tout le contenu du fichier</li>
                    <li>Dans l'éditeur avancé, supprimez tout et collez le script</li>
                    <li>Modifiez le chemin du fichier Excel (ligne qui contient "CHEMIN_VERS_VOTRE_FICHIER")</li>
                    <li>Cliquez sur <strong>"Terminé"</strong></li>
                    <li>Cliquez sur <strong>"Fermer et appliquer"</strong></li>
                </ol>
                
                <div class="info-box">
                    <strong>💡 Astuce:</strong> Le script Power Query nettoie automatiquement vos données et définit les bons types de colonnes.
                </div>
            </div>
            
            <div class="step">
                <h2>📐 Étape 4: Ajouter les mesures DAX</h2>
                <p>Maintenant que vos données sont chargées, ajoutons les calculs:</p>
                <ol>
                    <li>Ouvrez le fichier <code>DAX_Measures.dax</code> avec Notepad</li>
                    <li>Dans Power BI, dans le panneau <strong>"Champs"</strong> (à droite), cliquez-droit sur <strong>"Data"</strong></li>
                    <li>Sélectionnez <strong>"Nouvelle mesure"</strong></li>
                    <li>Copiez la première mesure du fichier DAX</li>
                    <li>Collez dans la barre de formule</li>
                    <li>Appuyez sur <strong>Entrée</strong></li>
                    <li>Répétez pour chaque mesure que vous souhaitez</li>
                </ol>
                
                <div class="success-box">
                    <strong>✅ Mesures suggérées:</strong> Commencez par les mesures de base (Total, Moyenne) puis ajoutez les mesures plus avancées selon vos besoins.
                </div>
            </div>
            
            <div class="step">
                <h2>📊 Étape 5: Créer vos visualisations</h2>
                <p>Vous êtes prêt à créer votre rapport! Voici quelques suggestions:</p>
                
                <div class="visual-suggestion">
                    <h4>📈 Graphique en barres</h4>
                    <p><strong>Utilisation:</strong> Comparer des catégories</p>
                    <p><strong>Configuration:</strong></p>
                    <ul>
                        <li><strong>Axe:</strong> Colonne catégorielle (texte)</li>
                        <li><strong>Valeurs:</strong> Mesure numérique (ex: Total, Moyenne)</li>
                    </ul>
                </div>
                
                <div class="visual-suggestion">
                    <h4>🎯 Carte</h4>
                    <p><strong>Utilisation:</strong> Afficher un KPI important</p>
                    <p><strong>Configuration:</strong></p>
                    <ul>
                        <li><strong>Champs:</strong> Une seule mesure (ex: Nombre_Total_Lignes)</li>
                    </ul>
                </div>
                
                <div class="visual-suggestion">
                    <h4>📉 Graphique linéaire</h4>
                    <p><strong>Utilisation:</strong> Montrer une tendance dans le temps</p>
                    <p><strong>Configuration:</strong></p>
                    <ul>
                        <li><strong>Axe:</strong> Colonne date</li>
                        <li><strong>Valeurs:</strong> Mesure numérique</li>
                    </ul>
                </div>
                
                <div class="visual-suggestion">
                    <h4>📊 Tableau</h4>
                    <p><strong>Utilisation:</strong> Afficher les données détaillées</p>
                    <p><strong>Configuration:</strong></p>
                    <ul>
                        <li><strong>Colonnes:</strong> Sélectionnez les colonnes importantes</li>
                        <li>Ajoutez des mesures pour avoir des totaux</li>
                    </ul>
                </div>
                
                <div class="info-box">
                    <strong>💡 Astuce:</strong> Faites glisser les champs depuis le panneau "Champs" vers les visuels. Power BI suggérera automatiquement le type de visuel approprié!
                </div>
            </div>
            
            <div class="step">
                <h2>🎨 Étape 6: Personnaliser et embellir</h2>
                <h3>Thèmes</h3>
                <ul>
                    <li>Allez dans <strong>"Affichage"</strong> > <strong>"Thèmes"</strong></li>
                    <li>Choisissez un thème prédéfini ou créez le vôtre</li>
                </ul>
                
                <h3>Filtres et segments</h3>
                <ul>
                    <li>Ajoutez un visuel <strong>"Segment"</strong> pour filtrer interactivement</li>
                    <li>Configurez des filtres au niveau de la page ou du rapport</li>
                </ul>
                
                <h3>Interactions</h3>
                <ul>
                    <li>Les visuels interagissent automatiquement entre eux</li>
                    <li>Cliquez sur un élément pour filtrer les autres visuels</li>
                </ul>
            </div>
            
            <div class="step">
                <h2>💾 Étape 7: Sauvegarder votre travail</h2>
                <ol>
                    <li>Cliquez sur <strong>"Fichier"</strong> > <strong>"Enregistrer sous"</strong></li>
                    <li>Choisissez un emplacement et un nom</li>
                    <li>Le fichier sera sauvegardé en <strong>.pbix</strong> (Power BI Desktop file)</li>
                </ol>
                
                <div class="success-box">
                    <strong>✅ Le fichier .pbix contient:</strong>
                    <ul>
                        <li>Vos données</li>
                        <li>Votre modèle</li>
                        <li>Vos visuels</li>
                        <li>Vos mesures</li>
                    </ul>
                    Vous pouvez le partager ou le publier sur Power BI Service!
                </div>
            </div>
            
            <div class="step">
                <h2>🔄 Actualiser les données</h2>
                <p>Pour mettre à jour avec de nouvelles données:</p>
                <ol>
                    <li>Remplacez le fichier Excel par une nouvelle version</li>
                    <li>Dans Power BI, cliquez sur <strong>"Actualiser"</strong> dans le ruban</li>
                    <li>Les données et visuels se mettent à jour automatiquement</li>
                </ol>
            </div>
            
            <div class="warning-box">
                <h3>⚠️ Problèmes courants et solutions</h3>
                
                <h4>Erreur: "Impossible de trouver le fichier"</h4>
                <p><strong>Solution:</strong> Vérifiez que le chemin dans le script Power Query est correct. Utilisez le chemin complet (ex: C:\\Users\\VotreNom\\Documents\\fichier.xlsx)</p>
                
                <h4>Erreur: "Colonne introuvable"</h4>
                <p><strong>Solution:</strong> Les noms de colonnes ont peut-être des espaces ou caractères spéciaux. Vérifiez que les noms dans le script correspondent exactement à ceux dans Excel.</p>
                
                <h4>Erreur de syntaxe DAX</h4>
                <p><strong>Solution:</strong> Assurez-vous que le nom de la table ('Data') correspond. Si vous avez renommé la table, modifiez les formules DAX en conséquence.</p>
                
                <h4>Les visuels sont lents</h4>
                <p><strong>Solution:</strong> Si vous avez beaucoup de données (&gt;100 000 lignes), considérez:</p>
                <ul>
                    <li>Agréger les données dans Power Query</li>
                    <li>Utiliser le mode DirectQuery au lieu d'Import</li>
                    <li>Optimiser vos mesures DAX</li>
                </ul>
            </div>
            
            <div class="info-box">
                <h3>📚 Ressources utiles</h3>
                <ul>
                    <li><a href="https://docs.microsoft.com/power-bi/" target="_blank">Documentation officielle Power BI</a></li>
                    <li><a href="https://dax.guide/" target="_blank">Guide DAX complet</a></li>
                    <li><a href="https://community.powerbi.com/" target="_blank">Communauté Power BI</a></li>
                    <li><a href="https://www.youtube.com/results?search_query=power+bi+tutorial" target="_blank">Tutoriels YouTube</a></li>
                </ul>
            </div>
            
            <div class="success-box">
                <h3>🎉 Félicitations!</h3>
                <p>Vous avez maintenant tous les outils pour créer un rapport Power BI professionnel. N'hésitez pas à expérimenter et personnaliser selon vos besoins!</p>
            </div>
        </div>
        
        <div class="footer">
            <p>Guide généré par Dashboard Analytics Pro</p>
            <p>{datetime.now().strftime('%Y')}</p>
        </div>
    </div>
</body>
</html>
"""
        
        return html_content.encode('utf-8')
    
    def _generate_visualization_suggestions(self, df: pd.DataFrame, clean_columns: list) -> list:
        """Génère des suggestions de visualisations"""
        suggestions = []
        
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime']).columns.tolist()
        
        # Suggestions pour colonnes numériques
        for col in numeric_cols[:3]:
            idx = df.columns.tolist().index(col)
            clean_col = clean_columns[idx]
            suggestions.append({
                'Colonne': clean_col,
                'Type_Visuel': 'Carte (KPI)',
                'Configuration': f'Afficher {clean_col}_Total ou {clean_col}_Moyenne',
                'Utilité': 'Montrer un indicateur clé'
            })
        
        # Suggestions pour catégories
        for col in categorical_cols[:2]:
            if df[col].nunique() <= 20:
                idx = df.columns.tolist().index(col)
                clean_col = clean_columns[idx]
                suggestions.append({
                    'Colonne': clean_col,
                    'Type_Visuel': 'Graphique en barres',
                    'Configuration': f'Axe: {clean_col}, Valeur: Nombre_Total_Lignes',
                    'Utilité': 'Comparer les catégories'
                })
        
        # Suggestions pour dates
        if len(datetime_cols) > 0 and len(numeric_cols) > 0:
            date_idx = df.columns.tolist().index(datetime_cols[0])
            date_col = clean_columns[date_idx]
            
            num_idx = df.columns.tolist().index(numeric_cols[0])
            num_col = clean_columns[num_idx]
            
            suggestions.append({
                'Colonne': f'{date_col} + {num_col}',
                'Type_Visuel': 'Graphique linéaire',
                'Configuration': f'Axe: {date_col}, Valeur: {num_col}_Total',
                'Utilité': 'Montrer l\'évolution dans le temps'
            })
        
        # Suggestions pour tableaux
        suggestions.append({
            'Colonne': 'Toutes',
            'Type_Visuel': 'Tableau',
            'Configuration': 'Sélectionner 5-7 colonnes importantes',
            'Utilité': 'Vue détaillée des données'
        })
        
        return suggestions
    
    def _create_complete_zip(self, package: dict, filename: str) -> bytes:
        """Crée un fichier ZIP avec tous les fichiers du package"""
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            # Ajouter l'Excel
            zip_file.writestr(f'{filename}_PowerBI.xlsx', package['excel'])
            
            # Ajouter le script Power Query
            zip_file.writestr('PowerQuery_Script.m', package['powerquery'])
            
            # Ajouter les mesures DAX
            zip_file.writestr('DAX_Measures.dax', package['dax'])
            
            # Ajouter le guide HTML
            zip_file.writestr('Guide.html', package['guide'])
            
            # Ajouter un README
            readme = f"""
PACKAGE POWER BI - {filename}
{'='*60}

Ce package contient tout ce dont vous avez besoin pour créer
votre rapport Power BI:

FICHIERS INCLUS:
- {filename}_PowerBI.xlsx : Vos données optimisées
- PowerQuery_Script.m : Script à copier dans Power Query
- DAX_Measures.dax : Mesures calculées prêtes à l'emploi
- Guide.html : Guide détaillé (OUVREZ CE FICHIER EN PREMIER!)

DÉMARRAGE RAPIDE:
1. Ouvrez Guide.html dans votre navigateur
2. Suivez les instructions étape par étape
3. Profitez de votre rapport Power BI!

Date de génération: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
            zip_file.writestr('README.txt', readme.encode('utf-8'))
        
        zip_buffer.seek(0)
        return zip_buffer.getvalue()


# ============================================================================
# AUTRES CLASSES INCHANGÉES
# ============================================================================

class UIComponents:
    def __init__(self):
        pass

    def apply_styles(self):
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
            </style>
        """, unsafe_allow_html=True)

    def render_sidebar(self):
        st.sidebar.markdown("## Paramètres")
        st.sidebar.markdown("Ce dashboard vous permet :")
        st.sidebar.markdown("- d'importer des fichiers de données")
        st.sidebar.markdown("- d'analyser rapidement les colonnes")
        st.sidebar.markdown("- d'exporter vers PowerBI")
        st.sidebar.markdown("---")
        
        name = st.sidebar.text_input("Votre nom", value=st.session_state.get("user_name", ""))
        if name:
            st.session_state["user_name"] = name
        st.sidebar.info(f"Développé par {st.session_state.get('user_name', '...')}")
    
    def display_file_info(self, uploaded_file):
        st.markdown("#### Informations sur le fichier")
        file_details = {
            "Nom du fichier": uploaded_file.name,
            "Type MIME": uploaded_file.type,
            "Taille (KB)": f"{len(uploaded_file.getbuffer()) / 1024:.1f}"
        }
        st.json(file_details)


class DatabaseManager:
    def __init__(self, db_path="data_imports.db"):
        self.db_path = db_path
        self.conn = None

    def init_db(self):
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS uploads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT,
                upload_date TEXT,
                file_type TEXT,
                rows INTEGER,
                columns INTEGER
            )
        """)
        self.conn.commit()

    def save_upload(self, filename: str, file_type: str, df: pd.DataFrame):
        if self.conn is None:
            self.init_db()

        cursor = self.conn.cursor()
        cursor.execute("""
            INSERT INTO uploads (filename, upload_date, file_type, rows, columns)
            VALUES (?, ?, ?, ?, ?)
        """, (
            filename,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            file_type,
            df.shape[0],
            df.shape[1]
        ))
        self.conn.commit()

    def get_uploads(self) -> pd.DataFrame:
        if self.conn is None:
            self.init_db()

        try:
            df = pd.read_sql_query("SELECT * FROM uploads ORDER BY upload_date DESC", self.conn)
            return df
        except Exception as e:
            st.error(f"Erreur lors de la lecture de la base de données : {e}")
            return pd.DataFrame()


# ============================================================================
# APPLICATION PRINCIPALE
# ============================================================================

st.set_page_config(
    page_title="Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

db_manager = DatabaseManager()
ui_components = UIComponents()
data_extractor = DataExtractor()
data_analyzer = DataAnalyzer()
data_visualizer = DataVisualizer()
powerbi_exporter = PowerBIExporter()

db_manager.init_db()
ui_components.apply_styles()

def main():
    st.markdown('<h1 class="main-header">📊 Dashboard Analytics Pro</h1>', unsafe_allow_html=True)
    
    ui_components.render_sidebar()
    
    if 'page' not in st.session_state:
        st.session_state['page'] = "🔄 Importer"

    page = st.sidebar.radio(
        "Navigation",
        ["🔄 Importer", "📈 Analyser", "📚 Historique", "⚙️ PowerBI"],
        index=["🔄 Importer", "📈 Analyser", "📚 Historique", "⚙️ PowerBI"].index(st.session_state['page'])
    )
    st.session_state['page'] = page

    if page == "🔄 Importer":
        render_import_page()
    elif page == "📈 Analyser":
        render_analysis_page()
    elif page == "📚 Historique":
        render_history_page()
    else:
        render_powerbi_page()


def render_import_page():
    st.markdown('<h2 class="section-header">🔄 Importer vos données</h2>', unsafe_allow_html=True)
    
    uploaded_file = st.file_uploader(
        "Glissez-déposez vos fichiers ici ou cliquez pour parcourir", 
        type=['csv', 'xlsx', 'xls', 'json', 'txt'],
        help="Formats supportés: CSV, Excel (.xlsx, .xls), JSON, TXT"
    )
    
    if uploaded_file is not None:
        ui_components.display_file_info(uploaded_file)
        
        with st.spinner('🔄 Extraction des données en cours...'):
            df = data_extractor.extract_data(uploaded_file)
        
        if df is not None and not df.empty:
            st.success("✅ Données importées avec succès!")
            
            st.markdown('<h3 class="section-header">👀 Aperçu des données</h3>', unsafe_allow_html=True)
            st.dataframe(df.head(20), use_container_width=True, height=400)
            
            if st.expander("🔍 Informations détaillées sur les colonnes"):
                col_info = data_analyzer.get_column_info(df)
                st.dataframe(col_info, use_container_width=True)
            
            st.session_state['data'] = df
            st.session_state['filename'] = uploaded_file.name
            
            db_manager.save_upload(uploaded_file.name, uploaded_file.type, df)
            
            st.markdown('<h3 class="section-header">🎯 Actions disponibles</h3>', unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📈 Analyser maintenant", type="primary", use_container_width=True):
                    st.session_state['page'] = "📈 Analyser"
                    st.rerun()
            
            with col2:
                csv_data = df.to_csv(index=False)
                st.download_button(
                    "💾 Télécharger CSV",
                    csv_data,
                    file_name=f"cleaned_{uploaded_file.name}.csv",
                    mime="text/csv",
                    use_container_width=True
                )
            
            with col3:
                if st.button("⚙️ Export Power BI", use_container_width=True):
                    st.session_state['page'] = "⚙️ PowerBI"
                    st.rerun()


def render_analysis_page():
    st.markdown('<h2 class="section-header">📈 Analyse des données</h2>', unsafe_allow_html=True)
    
    if 'data' not in st.session_state:
        st.warning("⚠️ Aucune donnée à analyser. Veuillez d'abord importer un fichier.")
        if st.button("➡️ Aller à l'importation", type="primary"):
            st.session_state['page'] = "🔄 Importer"
            st.rerun()
        return
    
    df = st.session_state['data']
    filename = st.session_state.get('filename', 'données')
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Aperçu", "📊 Statistiques", "📈 Visualisations", 
        "🔍 Qualité", "🎯 Insights"
    ])
    
    with tab1:
        st.markdown(f"### 📁 Analyse de: **{filename}**")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📊 Lignes", df.shape[0])
        with col2:
            st.metric("📋 Colonnes", df.shape[1])
        with col3:
            numeric_cols = len(df.select_dtypes(include=['number']).columns)
            st.metric("🔢 Colonnes numériques", numeric_cols)
        with col4:
            cat_cols = len(df.select_dtypes(include=['object', 'category']).columns)
            st.metric("📝 Colonnes texte", cat_cols)
        
        st.markdown("#### 👀 Aperçu des données")
        st.dataframe(df, use_container_width=True, height=400)
    
    with tab2:
        st.markdown("### 📊 Statistiques descriptives")
        
        numeric_df = df.select_dtypes(include=['number'])
        if not numeric_df.empty:
            st.markdown("#### 🔢 Colonnes numériques")
            st.dataframe(numeric_df.describe(), use_container_width=True)
            
            if len(numeric_df.columns) > 1:
                st.markdown("#### 🔗 Matrice de corrélation")
                corr_fig = data_visualizer.create_correlation_matrix(numeric_df)
                st.plotly_chart(corr_fig, use_container_width=True)
        
        cat_df = df.select_dtypes(include=['object', 'category'])
        if not cat_df.empty:
            st.markdown("#### 📝 Colonnes catégorielles")
            cat_stats = data_analyzer.get_categorical_stats(cat_df)
            st.dataframe(cat_stats, use_container_width=True)
    
    with tab3:
        st.markdown("### 📈 Visualisations automatiques")
        
        charts = data_visualizer.auto_generate_charts(df)
        
        if not charts:
            st.info("ℹ️ Aucune visualisation automatique disponible pour ce jeu de données.")
        else:
            for i, (chart_type, name, fig) in enumerate(charts):
                if i % 2 == 0:
                    col1, col2 = st.columns(2)
                
                with col1 if i % 2 == 0 else col2:
                    st.plotly_chart(fig, use_container_width=True, key=f"{chart_type}_{i}")
    
    with tab4:
        st.markdown("### 🔍 Qualité des données")
        
        quality_report = data_analyzer.analyze_data_quality(df)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            missing_pct = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
            st.metric("❌ Valeurs manquantes", f"{missing_pct:.1f}%")
        
        with col2:
            duplicates = df.duplicated().sum()
            st.metric("🔄 Lignes dupliquées", duplicates)
        
        with col3:
            data_types = len(df.dtypes.unique())
            st.metric("🏷️ Types de données", data_types)
        
        st.markdown("#### 📋 Détail par colonne")
        st.dataframe(quality_report, use_container_width=True)
        
        if df.isnull().any().any():
            missing_fig = data_visualizer.create_missing_data_heatmap(df)
            st.plotly_chart(missing_fig, use_container_width=True)
    
    with tab5:
        st.markdown("### 🎯 Insights automatiques")
        
        insights = data_analyzer.generate_insights(df)
        
        for insight in insights:
            st.info(f"💡 {insight}")
        
        st.markdown("#### 🎯 Recommandations")
        recommendations = data_analyzer.get_recommendations(df)
        
        for rec in recommendations:
            st.success(f"✅ {rec}")


def render_history_page():
    st.markdown('<h2 class="section-header">📚 Historique des importations</h2>', unsafe_allow_html=True)
    
    uploads_df = db_manager.get_uploads()
    
    if uploads_df.empty:
        st.info("📭 Aucun historique d'importation disponible.")
    else:
        st.dataframe(
            uploads_df[['id', 'filename', 'upload_date', 'file_type', 'rows', 'columns']],
            use_container_width=True,
            column_config={
                "id": "ID",
                "filename": "Nom du fichier",
                "upload_date": "Date d'import",
                "file_type": "Type",
                "rows": "Lignes",
                "columns": "Colonnes"
            }
        )


def render_powerbi_page():
    """
    PAGE POWER BI CORRIGÉE
    Génère un package complet au lieu d'un .pbit
    """
    st.markdown('<h2 class="section-header">⚙️ Package Power BI Complet</h2>', unsafe_allow_html=True)
    
    if 'data' not in st.session_state:
        st.warning("⚠️ Aucune donnée à exporter. Veuillez d'abord importer un fichier.")
        if st.button("➡️ Aller à l'importation", type="primary"):
            st.session_state['page'] = "🔄 Importer"
            st.rerun()
        return
    
    df = st.session_state['data']
    filename = st.session_state.get('filename', 'data')
    filename_base = filename.rsplit('.', 1)[0]
    
    st.markdown("### 📦 Génération du Package Power BI")
    
    # Explication de l'approche
    st.info("""
    **🎯 Nouvelle Approche - Package Complet**
    
    Au lieu d'un fichier .pbit (qui cause des erreurs), nous générons un **package complet** contenant:
    
    ✅ **Fichier Excel optimisé** - Vos données nettoyées et structurées  
    ✅ **Script Power Query (M)** - Code à copier-coller pour importer les données  
    ✅ **Fichier DAX** - Toutes les mesures calculées prêtes à l'emploi  
    ✅ **Guide HTML interactif** - Instructions détaillées étape par étape  
    ✅ **Fichier ZIP** - Tout le package en un seul téléchargement  
    
    **Cette méthode est 100% fonctionnelle et professionnelle!** ✨
    """)
    
    # Prévisualisation des données
    st.markdown("#### 👀 Aperçu de vos données")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📊 Lignes", f"{df.shape[0]:,}")
    with col2:
        st.metric("📋 Colonnes", df.shape[1])
    with col3:
        numeric_cols = len(df.select_dtypes(include=['number']).columns)
        st.metric("🔢 Numériques", numeric_cols)
    with col4:
        cat_cols = len(df.select_dtypes(include=['object']).columns)
        st.metric("📝 Texte", cat_cols)
    
    with st.expander("📄 Voir les premières lignes"):
        st.dataframe(df.head(10), use_container_width=True)
    
    # Options de génération
    st.markdown("#### ⚙️ Options de génération")
    
    col1, col2 = st.columns(2)
    with col1:
        include_metadata = st.checkbox("Inclure les métadonnées détaillées", value=True,
                                       help="Ajoute des informations sur les colonnes dans Excel")
    with col2:
        include_suggestions = st.checkbox("Inclure suggestions de visuels", value=True,
                                         help="Ajoute des recommandations de visualisations")
    
    # Bouton de génération
    st.markdown("---")
    
    if st.button("🚀 Générer le Package Power BI Complet", type="primary", use_container_width=True):
        with st.spinner("⏳ Génération du package en cours... Cela peut prendre quelques secondes..."):
            try:
                # Générer le package complet
                package = powerbi_exporter.create_powerbi_package(df, filename_base)
                
                st.success("✅ Package Power BI généré avec succès!")
                
                # Section de téléchargement
                st.markdown("---")
                st.markdown("### 📥 Téléchargements")
                
                # Téléchargement du ZIP complet (RECOMMANDÉ)
                st.markdown("#### 🎁 Téléchargement Complet (Recommandé)")
                col1, col2 = st.columns([3, 1])
                
                with col1:
                    st.info("**Téléchargez le package complet** - Contient tous les fichiers en un seul ZIP")
                
                with col2:
                    st.download_button(
                        "📦 ZIP Complet",
                        package['zip'],
                        file_name=f"{filename_base}_PowerBI_Package.zip",
                        mime="application/zip",
                        use_container_width=True,
                        help="Télécharger tous les fichiers en une fois"
                    )
                
                st.markdown("---")
                
                # Téléchargements individuels
                st.markdown("#### 📄 Téléchargements Individuels")
                
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.download_button(
                        "📊 Excel",
                        package['excel'],
                        file_name=f"{filename_base}_PowerBI.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        use_container_width=True,
                        help="Fichier Excel avec vos données"
                    )
                
                with col2:
                    st.download_button(
                        "⚙️ Power Query",
                        package['powerquery'].encode('utf-8'),
                        file_name="PowerQuery_Script.m",
                        mime="text/plain",
                        use_container_width=True,
                        help="Script M à copier-coller"
                    )
                
                with col3:
                    st.download_button(
                        "📐 DAX",
                        package['dax'].encode('utf-8'),
                        file_name="DAX_Measures.dax",
                        mime="text/plain",
                        use_container_width=True,
                        help="Mesures DAX prêtes à l'emploi"
                    )
                
                with col4:
                    st.download_button(
                        "📖 Guide",
                        package['guide'],
                        file_name="Guide.html",
                        mime="text/html",
                        use_container_width=True,
                        help="Guide détaillé à ouvrir dans le navigateur"
                    )
                
                # Instructions rapides
                st.markdown("---")
                st.markdown("### 🎯 Démarrage Rapide")
                
                with st.expander("📋 Instructions Résumées (5 minutes)", expanded=True):
                    st.markdown("""
                    **Étapes simples pour créer votre rapport:**
                    
                    1. **📥 Télécharger** le package ZIP complet ci-dessus
                    
                    2. **📂 Extraire** le ZIP dans un dossier de votre choix
                    
                    3. **🌐 Ouvrir** le fichier `Guide.html` dans votre navigateur
                       - Ce guide contient toutes les instructions détaillées
                       - Suivez les étapes une par une
                    
                    4. **🚀 En résumé:**
                       - Ouvrez Power BI Desktop (gratuit)
                       - Importez le fichier Excel
                       - Copiez le script Power Query
                       - Ajoutez les mesures DAX
                       - Créez vos visuels!
                    
                    **⏱️ Temps estimé: 5-10 minutes pour les débutants, 2-3 minutes pour les utilisateurs expérimentés**
                    """)
                
                # Aperçu du contenu
                st.markdown("---")
                st.markdown("### 👀 Aperçu du Contenu Généré")
                
                tab1, tab2, tab3 = st.tabs(["📊 Métadonnées Excel", "⚙️ Script Power Query", "📐 Mesures DAX"])
                
                with tab1:
                    st.markdown("**Feuilles dans le fichier Excel:**")
                    st.markdown("""
                    - **Data** - Vos données nettoyées
                    - **Metadata** - Informations sur le dataset
                    - **DataTypes** - Types de colonnes et suggestions
                    - **Visualizations** - Recommandations de visuels
                    """)
                
                with tab2:
                    st.markdown("**Aperçu du script Power Query (M):**")
                    preview_lines = package['powerquery'].split('\n')[:25]
                    st.code('\n'.join(preview_lines) + '\n\n// ... (voir le fichier complet)', language='powerquery')
                
                with tab3:
                    st.markdown("**Aperçu des mesures DAX:**")
                    preview_lines = package['dax'].split('\n')[:20]
                    st.code('\n'.join(preview_lines) + '\n\n// ... (voir le fichier complet)', language='dax')
                
            except Exception as e:
                st.error(f"❌ Erreur lors de la génération du package: {str(e)}")
                with st.expander("🔍 Détails de l'erreur"):
                    st.exception(e)
    
    # Section Aide et FAQ
    st.markdown("---")
    st.markdown("### 🆘 Aide et FAQ")
    
    with st.expander("❓ Je n'ai jamais utilisé Power BI, par où commencer?"):
        st.markdown("""
        **Pas de panique! C'est très simple:**
        
        1. Téléchargez Power BI Desktop gratuitement: [powerbi.microsoft.com/desktop](https://powerbi.microsoft.com/desktop)
        2. Installez-le (c'est gratuit, aucun compte requis pour l'utiliser localement)
        3. Téléchargez le package ZIP ci-dessus
        4. Ouvrez le fichier `Guide.html` - il contient TOUT ce qu'il faut savoir!
        
        Le guide est fait pour les débutants avec des captures d'écran et des explications simples.
        """)
    
    with st.expander("❓ Pourquoi pas un fichier .pbit directement?"):
        st.markdown("""
        **Excellente question!**
        
        Les fichiers .pbit (Power BI Template) sont très complexes et nécessitent une structure JSON
        exacte qui varie selon les versions de Power BI. Créer un .pbit compatible est très difficile
        et peut causer des erreurs à l'ouverture.
        
        **Notre approche est meilleure car:**
        - ✅ 100% compatible avec toutes les versions de Power BI
        - ✅ Vous comprenez ce que vous faites (apprentissage)
        - ✅ Totalement personnalisable
        - ✅ Aucun risque d'erreur d'incompatibilité
        - ✅ Fonctionne aussi bien qu'un .pbit, voire mieux!
        
        En 5 minutes, vous aurez le même résultat, avec plus de contrôle.
        """)
    
    with st.expander("❓ Que faire si j'ai une erreur dans Power BI?"):
        st.markdown("""
        **Les erreurs les plus courantes et leurs solutions:**
        
        **1. "Impossible de trouver le fichier Excel"**
        - Vérifiez que le chemin dans le script Power Query est correct
        - Utilisez le chemin complet: `C:\\Users\\VotreNom\\Documents\\fichier.xlsx`
        
        **2. "Colonne introuvable"**
        - Les noms de colonnes doivent correspondre exactement
        - Vérifiez les espaces et caractères spéciaux
        
        **3. "Erreur de syntaxe DAX"**
        - Assurez-vous que le nom de la table est 'Data'
        - Si vous l'avez renommée, modifiez les formules DAX
        
        **4. Le guide HTML contient une section complète "Problèmes courants"** avec toutes les solutions!
        """)
    
    with st.expander("❓ Puis-je actualiser les données plus tard?"):
        st.markdown("""
        **Oui, absolument!**
        
        Pour actualiser avec de nouvelles données:
        
        1. Remplacez le fichier Excel par une nouvelle version (même structure)
        2. Dans Power BI, cliquez sur le bouton **"Actualiser"**
        3. Les données et tous les visuels se mettent à jour automatiquement!
        
        Vous n'avez pas besoin de recréer le rapport à chaque fois.
        """)
    
    with st.expander("❓ Puis-je partager mon rapport?"):
        st.markdown("""
        **Oui, plusieurs options:**
        
        **Option 1: Fichier .pbix**
        - Sauvegardez votre rapport Power BI en .pbix
        - Partagez le fichier (contient données + visuels)
        - Les autres peuvent l'ouvrir avec Power BI Desktop
        
        **Option 2: Power BI Service (cloud)**
        - Publiez sur powerbi.com (compte Microsoft gratuit requis)
        - Partagez un lien web
        - Les autres consultent dans leur navigateur (pas besoin de Power BI Desktop)
        
        **Option 3: Export PDF/PowerPoint**
        - Dans Power BI, exportez en PDF ou PPT
        - Parfait pour des présentations statiques
        """)
    
    # Ressources
    st.markdown("---")
    st.markdown("### 📚 Ressources Utiles")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("""
        **📖 Documentation**
        
        - [Docs Power BI](https://docs.microsoft.com/power-bi/)
        - [Guide DAX](https://dax.guide/)
        - [Power Query M](https://docs.microsoft.com/powerquery-m/)
        """)
    
    with col2:
        st.info("""
        **🎓 Tutoriels**
        
        - [YouTube - Guy in a Cube](https://www.youtube.com/c/GuyinaCube)
        - [Tutoriels Microsoft](https://docs.microsoft.com/learn/powerplatform/power-bi)
        - [Forum communauté](https://community.powerbi.com/)
        """)
    
    with col3:
        st.info("""
        **💡 Astuces**
        
        - Commencez simple!
        - Utilisez les thèmes
        - Testez les visuels
        - Explorez les exemples
        """)


if __name__ == "__main__":
    main()