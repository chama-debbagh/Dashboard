
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import json
import io
import sqlite3
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
        """
        Extrait les données du fichier uploadé
        
        Args:
            uploaded_file: Fichier uploadé via Streamlit
            
        Returns:
            pd.DataFrame ou None si erreur
        """
        try:
            file_extension = self._get_file_extension(uploaded_file.name)
            
            if file_extension not in self.supported_formats:
                st.error(f"Format de fichier non supporté: {file_extension}")
                return None
            
            # Dispatcher vers la méthode appropriée
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
        """Obtient l'extension du fichier"""
        return Path(filename).suffix.lower().lstrip('.')
    
    def _extract_from_excel(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Extrait les données d'un fichier Excel"""
        try:
            # Lire le fichier Excel avec gestion des erreurs
            excel_file = pd.ExcelFile(uploaded_file)
            
            # Si plusieurs feuilles, demander à l'utilisateur de choisir
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
            
            # Nettoyage initial
            df = self._clean_dataframe(df)
            
            st.success(f"Fichier Excel importé: {df.shape[0]} lignes, {df.shape[1]} colonnes")
            return df
            
        except Exception as e:
            st.error(f"Erreur lors de l'importation Excel: {str(e)}")
            return None
    
    def _extract_from_csv(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Extrait les données d'un fichier CSV avec détection automatique"""
        try:
            # Lire les premiers octets pour détecter l'encodage
            raw_data = uploaded_file.read()
            uploaded_file.seek(0)  # Remettre le curseur au début
            
            # Détecter l'encodage
            encoding_result = chardet.detect(raw_data)
            encoding = encoding_result['encoding'] if encoding_result['confidence'] > 0.7 else 'utf-8'
            
            st.info(f"🔍 Encodage détecté: {encoding} (confiance: {encoding_result['confidence']:.2f})")
            
            # Essayer différents séparateurs et configurations
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
                    
                    # Garder le DataFrame avec le plus de colonnes cohérentes
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
            
            # Nettoyage
            best_df = self._clean_dataframe(best_df)
            return best_df
            
        except Exception as e:
            st.error(f"Erreur lors de l'importation CSV: {str(e)}")
            return None
    
    def _extract_from_json(self, uploaded_file) -> Optional[pd.DataFrame]:
        """Extrait les données d'un fichier JSON"""
        try:
            json_data = json.load(uploaded_file)
            
            # Gestion de différentes structures JSON
            if isinstance(json_data, list):
                df = pd.json_normalize(json_data)
            elif isinstance(json_data, dict):
                # Essayer de trouver une liste dans le dictionnaire
                for key, value in json_data.items():
                    if isinstance(value, list) and len(value) > 0:
                        df = pd.json_normalize(value)
                        st.info(f"Données extraites de la clé: '{key}'")
                        break
                else:
                    # Si pas de liste trouvée, normaliser le dictionnaire
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
        """Extrait les données d'un fichier texte"""
        try:
            # Lire le contenu du fichier
            content = uploaded_file.read().decode('utf-8')
            lines = content.strip().split('\n')
            
            if not lines:
                st.error("Fichier texte vide")
                return None
            
            # Essayer de détecter un format tabulaire
            first_line = lines[0]
            
            # Détecter le séparateur le plus probable
            separators = ['\t', ',', ';', '|', ' ']
            best_sep = None
            max_cols = 0
            
            for sep in separators:
                cols = len(first_line.split(sep))
                if cols > max_cols:
                    max_cols = cols
                    best_sep = sep
            
            if max_cols < 2:
                # Traiter comme texte simple
                df = pd.DataFrame({'Contenu': lines})
                st.info("Fichier traité comme texte simple")
            else:
                # Traiter comme données tabulaires
                data = []
                headers = lines[0].split(best_sep)
                
                for line in lines[1:]:
                    if line.strip():
                        values = line.split(best_sep)
                        # Ajuster la longueur si nécessaire
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
        """Nettoie le DataFrame après importation"""
        try:
            # Supprimer les lignes entièrement vides
            df = df.dropna(how='all')
            
            # Supprimer les colonnes entièrement vides
            df = df.dropna(axis=1, how='all')
            
            # Nettoyer les noms de colonnes
            df.columns = df.columns.astype(str)
            df.columns = [col.strip() for col in df.columns]
            
            # Remplacer les noms de colonnes vides
            df.columns = [f'Colonne_{i}' if col == '' or col.startswith('Unnamed') 
                         else col for i, col in enumerate(df.columns)]
            
            # Supprimer les doublons de noms de colonnes
            df.columns = pd.io.common.dedup_names(df.columns, is_potential_multiindex=False)
            
            # Tentative de conversion automatique des types
            df = self._auto_convert_types(df)
            
            return df
            
        except Exception as e:
            st.warning(f"Erreur lors du nettoyage: {str(e)}")
            return df
    
    def _auto_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Conversion automatique des types de données"""
        try:
            for col in df.columns:
                # Essayer de convertir en numérique
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                if not numeric_col.isna().all():
                    # Si plus de 80% des valeurs sont numériques, convertir
                    valid_numeric = (~numeric_col.isna()).sum()
                    total_non_null = (~df[col].isna()).sum()
                    
                    if total_non_null > 0 and (valid_numeric / total_non_null) > 0.8:
                        df[col] = numeric_col
                        continue
                
                # Essayer de convertir en datetime
                try:
                    datetime_col = pd.to_datetime(df[col], errors='coerce')
                    valid_datetime = (~datetime_col.isna()).sum()
                    total_non_null = (~df[col].isna()).sum()
                    
                    if total_non_null > 0 and (valid_datetime / total_non_null) > 0.8:
                        df[col] = datetime_col
                        continue
                except:
                    pass
                
                # Nettoyer les colonnes texte
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
        """
        Retourne des informations détaillées sur chaque colonne
        
        Args:
            df: DataFrame à analyser
            
        Returns:
            DataFrame avec les informations des colonnes
        """
        info_data = []
        
        for col in df.columns:
            col_data = {
                'Colonne': col,
                'Type': str(df[col].dtype),
                'Valeurs_uniques': df[col].nunique(),
                'Valeurs_manquantes': df[col].isnull().sum(),
                'Pourcentage_manquant': f"{(df[col].isnull().sum() / len(df)) * 100:.1f}%",
                'Taille_mémoire_KB': f"{df[col].memory_usage(deep=True) / 1024:.1f}"
            }
            
            # Ajouter des statistiques spécifiques selon le type
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32']:
                col_data.update({
                    'Min': df[col].min() if not df[col].empty else None,
                    'Max': df[col].max() if not df[col].empty else None,
                    'Moyenne': f"{df[col].mean():.2f}" if not df[col].empty else None,
                    'Médiane': f"{df[col].median():.2f}" if not df[col].empty else None
                })
            elif df[col].dtype == 'object':
                col_data.update({
                    'Longueur_min': df[col].astype(str).str.len().min() if not df[col].empty else None,
                    'Longueur_max': df[col].astype(str).str.len().max() if not df[col].empty else None,
                    'Longueur_moyenne': f"{df[col].astype(str).str.len().mean():.1f}" if not df[col].empty else None,
                    'Valeur_fréquente': df[col].mode().iloc[0] if not df[col].mode().empty else None
                })
            
            info_data.append(col_data)
        
        return pd.DataFrame(info_data)
    
    def get_categorical_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Statistiques pour les colonnes catégorielles
        
        Args:
            df: DataFrame avec colonnes catégorielles
            
        Returns:
            DataFrame avec statistiques catégorielles
        """
        stats_data = []
        
        for col in df.columns:
            if df[col].nunique() < 50:  # Seulement pour colonnes avec peu de valeurs uniques
                value_counts = df[col].value_counts()
                stats_data.append({
                    'Colonne': col,
                    'Valeurs_uniques': df[col].nunique(),
                    'Valeur_dominante': value_counts.index[0] if len(value_counts) > 0 else None,
                    'Fréquence_dominante': value_counts.iloc[0] if len(value_counts) > 0 else 0,
                    'Pourcentage_dominante': f"{(value_counts.iloc[0] / len(df)) * 100:.1f}%" if len(value_counts) > 0 else "0%",
                    'Entropie': self._calculate_entropy(df[col])
                })
        
        return pd.DataFrame(stats_data)
    
    def analyze_data_quality(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Analyse la qualité des données
        
        Args:
            df: DataFrame à analyser
            
        Returns:
            DataFrame avec rapport de qualité
        """
        quality_data = []
        
        for col in df.columns:
            # Calculs de base
            total_rows = len(df)
            missing_count = df[col].isnull().sum()
            missing_pct = (missing_count / total_rows) * 100
            unique_count = df[col].nunique()
            unique_pct = (unique_count / total_rows) * 100
            
            # Score de qualité (0-100)
            quality_score = 100
            if missing_pct > 50:
                quality_score -= 30
            elif missing_pct > 20:
                quality_score -= 15
            elif missing_pct > 5:
                quality_score -= 5
            
            # Problèmes potentiels
            issues = []
            if missing_pct > 20:
                issues.append("Beaucoup de valeurs manquantes")
            if unique_pct < 1 and df[col].dtype in ['object']:
                issues.append("Peu de diversité")
            if unique_count == 1:
                issues.append("Valeur constante")
            if df[col].dtype == 'object':
                # Vérifier la cohérence des formats
                sample_values = df[col].dropna().astype(str).str.strip()
                if len(sample_values) > 0:
                    lengths = sample_values.str.len()
                    if lengths.std() > lengths.mean():
                        issues.append("Formats incohérents")
            
            quality_data.append({
                'Colonne': col,
                'Score_qualité': f"{quality_score:.0f}/100",
                'Valeurs_manquantes': f"{missing_count} ({missing_pct:.1f}%)",
                'Valeurs_uniques': f"{unique_count} ({unique_pct:.1f}%)",
                'Problèmes': "; ".join(issues) if issues else "Aucun",
                'Recommandation': self._get_quality_recommendation(missing_pct, unique_pct, issues)
            })
        
        return pd.DataFrame(quality_data)
    
    def generate_insights(self, df: pd.DataFrame) -> List[str]:
        """
        Génère des insights automatiques sur les données
        
        Args:
            df: DataFrame à analyser
            
        Returns:
            Liste d'insights
        """
        insights = []
        
        # Insights généraux
        total_rows = len(df)
        total_cols = len(df.columns)
        
        insights.append(f"Le dataset contient {total_rows:,} lignes et {total_cols} colonnes")
        
        # Insights sur les valeurs manquantes
        missing_total = df.isnull().sum().sum()
        missing_pct = (missing_total / (total_rows * total_cols)) * 100
        if missing_pct > 10:
            insights.append(f"Attention: {missing_pct:.1f}% des données sont manquantes")
        elif missing_pct == 0:
            insights.append("Excellent: Aucune valeur manquante détectée")
        
        # Insights sur les types de données
        numeric_cols = len(df.select_dtypes(include=['number']).columns)
        text_cols = len(df.select_dtypes(include=['object']).columns)
        date_cols = len(df.select_dtypes(include=['datetime']).columns)
        
        if numeric_cols > text_cols:
            insights.append(f"Dataset majoritairement numérique ({numeric_cols} colonnes numériques vs {text_cols} textuelles)")
        elif text_cols > numeric_cols:
            insights.append(f"Dataset majoritairement textuel ({text_cols} colonnes textuelles vs {numeric_cols} numériques)")
        
        if date_cols > 0:
            insights.append(f"Dataset temporel détecté avec {date_cols} colonne(s) de dates")
        
        # Insights sur la distribution
        for col in df.select_dtypes(include=['number']).columns[:3]:  # Top 3 colonnes numériques
            skewness = df[col].skew()
            if abs(skewness) > 2:
                skew_type = "très asymétrique à droite" if skewness > 0 else "très asymétrique à gauche"
                insights.append(f"La colonne '{col}' a une distribution {skew_type}")
        
        # Insights sur les corrélations
        numeric_df = df.select_dtypes(include=['number'])
        if len(numeric_df.columns) > 1:
            corr_matrix = numeric_df.corr()
            # Trouver les corrélations les plus fortes (hors diagonale)
            corr_pairs = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i+1, len(corr_matrix.columns)):
                    corr_val = corr_matrix.iloc[i, j]
                    if abs(corr_val) > 0.7:
                        corr_pairs.append((corr_matrix.columns[i], corr_matrix.columns[j], corr_val))
            
            if corr_pairs:
                best_corr = max(corr_pairs, key=lambda x: abs(x[2]))
                insights.append(f"Forte corrélation détectée entre '{best_corr[0]}' et '{best_corr[1]}' (r={best_corr[2]:.2f})")
        
        # Insights sur les outliers
        for col in df.select_dtypes(include=['number']).columns[:2]:
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
            
            if len(outliers) > 0:
                outlier_pct = (len(outliers) / len(df)) * 100
                insights.append(f"La colonne '{col}' contient {len(outliers)} valeurs aberrantes ({outlier_pct:.1f}%)")
        
        return insights[:8]  # Limiter à 8 insights
    
    def get_recommendations(self, df: pd.DataFrame) -> List[str]:
        """
        Génère des recommandations pour améliorer les données
        
        Args:
            df: DataFrame à analyser
            
        Returns:
            Liste de recommandations
        """
        recommendations = []
        
        # Recommandations sur les valeurs manquantes
        high_missing_cols = []
        for col in df.columns:
            missing_pct = (df[col].isnull().sum() / len(df)) * 100
            if missing_pct > 20:
                high_missing_cols.append((col, missing_pct))
        
        if high_missing_cols:
            recommendations.append(f"Traiter les valeurs manquantes dans {len(high_missing_cols)} colonne(s): " + 
                                 ", ".join([f"{col} ({pct:.1f}%)" for col, pct in high_missing_cols[:3]]))
        
        # Recommandations sur les doublons
        duplicates = df.duplicated().sum()
        if duplicates > 0:
            recommendations.append(f"Supprimer {duplicates} ligne(s) dupliquée(s)")
        
        # Recommandations sur les types de données
        for col in df.select_dtypes(include=['object']).columns:
            # Vérifier si la colonne pourrait être numérique
            try:
                numeric_converted = pd.to_numeric(df[col], errors='coerce')
                non_null_original = df[col].notna().sum()
                non_null_converted = numeric_converted.notna().sum()
                
                if non_null_converted / non_null_original > 0.8:
                    recommendations.append(f"Convertir la colonne '{col}' en type numérique")
            except:
                pass
            
            # Vérifier si la colonne pourrait être catégorielle
            if df[col].nunique() < 20 and df[col].nunique() / len(df) < 0.1:
                recommendations.append(f"Convertir la colonne '{col}' en type catégoriel pour optimiser la mémoire")
        
        # Recommandations sur la normalisation
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            if df[col].std() > 0:
                col_range = df[col].max() - df[col].min()
                col_mean = df[col].mean()
                if col_range > 1000 or col_mean > 1000:
                    recommendations.append(f"Considérer la normalisation de la colonne '{col}' pour les analyses")
        
        # Recommandations sur l'indexation
        if len(df) > 10000:
            recommendations.append("Considérer l'ajout d'un index pour améliorer les performances sur ce large dataset")
        
        # Recommandations sur les visualisations
        if len(numeric_cols) >= 2:
            recommendations.append("Créer des graphiques de corrélation pour explorer les relations entre variables")
        
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns
        if len(categorical_cols) > 0:
            recommendations.append("Analyser la distribution des variables catégorielles avec des graphiques en barres")
        
        return recommendations[:6]  # Limiter à 6 recommandations
    
    def _calculate_entropy(self, series: pd.Series) -> float:
        """Calcule l'entropie d'une série (mesure de diversité)"""
        try:
            value_counts = series.value_counts()
            probabilities = value_counts / len(series)
            entropy = -np.sum(probabilities * np.log2(probabilities + 1e-10))
            return round(entropy, 3)
        except:
            return 0.0
    
    def _get_quality_recommendation(self, missing_pct: float, unique_pct: float, issues: List[str]) -> str:
        """Génère une recommandation basée sur la qualité de la colonne"""
        if missing_pct > 50:
            return "Considérer la suppression de cette colonne"
        elif missing_pct > 20:
            return "Imputer les valeurs manquantes"
        elif "Valeur constante" in issues:
            return "Supprimer cette colonne (pas d'information)"
        elif "Formats incohérents" in issues:
            return "Standardiser le format des données"
        elif unique_pct < 1:
            return "Vérifier la cohérence des données"
        else:
            return "Colonne de bonne qualité"




class DataVisualizer:
    """Classe pour créer des visualisations automatiques des données"""
    
    def __init__(self):
        # Palette de couleurs moderne
        self.color_palette = [
            '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd',
            '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf'
        ]
        
        # Template de style pour les graphiques
        self.layout_template = {
            'font': {'family': 'Arial, sans-serif', 'size': 12},
            'title': {'x': 0.5, 'xanchor': 'center'},
            'plot_bgcolor': 'white',
            'paper_bgcolor': 'white',
            'margin': {'l': 60, 'r': 60, 't': 80, 'b': 60}
        }
    
    def auto_generate_charts(self, df: pd.DataFrame) -> List[Tuple[str, str, Any]]:
        """
        Génère automatiquement des graphiques appropriés selon les données
        
        Args:
            df: DataFrame à visualiser
            
        Returns:
            Liste de tuples (type_graphique, nom, figure_plotly)
        """
        charts = []
        
        # Séparer les colonnes par type
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        categorical_cols = df.select_dtypes(include=['object', 'category']).columns.tolist()
        datetime_cols = df.select_dtypes(include=['datetime']).columns.tolist()
        
        # 1. Histogrammes pour colonnes numériques
        for col in numeric_cols[:4]:  # Limiter à 4 pour éviter la surcharge
            fig = self._create_histogram(df, col)
            charts.append(("histogram", f"Distribution de {col}", fig))
        
        # 2. Graphiques en barres pour colonnes catégorielles
        for col in categorical_cols[:3]:
            if df[col].nunique() <= 15:  # Seulement si peu de catégories
                fig = self._create_bar_chart(df, col)
                charts.append(("bar", f"Répartition de {col}", fig))
        
        # 3. Scatter plots pour paires de variables numériques
        if len(numeric_cols) >= 2:
            # Créer scatter plot pour les 2 premières colonnes numériques
            fig = self._create_scatter_plot(df, numeric_cols[0], numeric_cols[1])
            charts.append(("scatter", f"{numeric_cols[0]} vs {numeric_cols[1]}", fig))
            
            # Si plus de 2 colonnes numériques, ajouter un autre scatter plot
            if len(numeric_cols) >= 3:
                fig = self._create_scatter_plot(df, numeric_cols[0], numeric_cols[2])
                charts.append(("scatter", f"{numeric_cols[0]} vs {numeric_cols[2]}", fig))
        
        # 4. Box plots pour distribution par catégorie
        if len(numeric_cols) >= 1 and len(categorical_cols) >= 1:
            # Prendre la première colonne catégorielle avec peu de valeurs uniques
            cat_col = None
            for col in categorical_cols:
                if df[col].nunique() <= 10:
                    cat_col = col
                    break
            
            if cat_col:
                fig = self._create_box_plot(df, numeric_cols[0], cat_col)
                charts.append(("box", f"{numeric_cols[0]} par {cat_col}", fig))
        
        # 5. Graphiques temporels si colonnes datetime
        if len(datetime_cols) >= 1 and len(numeric_cols) >= 1:
            fig = self._create_time_series(df, datetime_cols[0], numeric_cols[0])
            charts.append(("timeseries", f"Évolution de {numeric_cols[0]}", fig))
        
        # 6. Heatmap de corrélation si suffisamment de colonnes numériques
        if len(numeric_cols) >= 3:
            fig = self.create_correlation_matrix(df[numeric_cols])
            charts.append(("heatmap", "Matrice de corrélation", fig))
        
        return charts
    
    def _create_histogram(self, df: pd.DataFrame, column: str) -> go.Figure:
        """Crée un histogramme pour une colonne numérique"""
        fig = px.histogram(
            df, 
            x=column,
            nbins=30,
            title=f"Distribution de {column}",
            color_discrete_sequence=[self.color_palette[0]]
        )
        
        fig.update_layout(
            **self.layout_template,
            xaxis_title=column,
            yaxis_title="Fréquence",
            showlegend=False
        )
        
        # Ajouter ligne de moyenne
        mean_val = df[column].mean()
        fig.add_vline(
            x=mean_val, 
            line_dash="dash", 
            line_color="red",
            annotation_text=f"Moyenne: {mean_val:.2f}"
        )
        
        return fig
    
    def _create_bar_chart(self, df: pd.DataFrame, column: str) -> go.Figure:
        """Crée un graphique en barres pour une colonne catégorielle"""
        value_counts = df[column].value_counts().head(10)  # Top 10
        
        fig = px.bar(
            x=value_counts.index,
            y=value_counts.values,
            title=f"Répartition de {column}",
            color=value_counts.values,
            color_continuous_scale="viridis"
        )
        
        fig.update_layout(
            **self.layout_template,
            xaxis_title=column,
            yaxis_title="Fréquence",
            showlegend=False
        )
        
        # Rotation des labels si nécessaires
        if max([len(str(x)) for x in value_counts.index]) > 10:
            fig.update_xaxes(tickangle=45)
        
        return fig
    
    def _create_scatter_plot(self, df: pd.DataFrame, x_col: str, y_col: str) -> go.Figure:
        """Crée un scatter plot entre deux colonnes numériques"""
        fig = px.scatter(
            df,
            x=x_col,
            y=y_col,
            title=f"Relation entre {x_col} et {y_col}",
            color_discrete_sequence=[self.color_palette[2]],
            opacity=0.7
        )
        
        # Ajouter ligne de tendance
        try:
            fig.add_traces(
                px.scatter(df, x=x_col, y=y_col, trendline="ols").data[1]
            )
        except:
            pass
        
        fig.update_layout(
            **self.layout_template,
            xaxis_title=x_col,
            yaxis_title=y_col
        )
        
        # Calculer et afficher la corrélation
        correlation = df[x_col].corr(df[y_col])
        fig.add_annotation(
            x=0.02, y=0.98,
            xref="paper", yref="paper",
            text=f"Corrélation: {correlation:.3f}",
            showarrow=False,
            bgcolor="white",
            bordercolor="black",
            borderwidth=1
        )
        
        return fig
    
    def _create_box_plot(self, df: pd.DataFrame, numeric_col: str, cat_col: str) -> go.Figure:
        """Crée un box plot pour analyser la distribution d'une variable numérique par catégorie"""
        fig = px.box(
            df,
            x=cat_col,
            y=numeric_col,
            title=f"Distribution de {numeric_col} par {cat_col}",
            color=cat_col
        )
        
        fig.update_layout(
            **self.layout_template,
            xaxis_title=cat_col,
            yaxis_title=numeric_col
        )
        
        if df[cat_col].nunique() > 5:
            fig.update_xaxes(tickangle=45)
        
        return fig
    
    def _create_time_series(self, df: pd.DataFrame, date_col: str, value_col: str) -> go.Figure:
        """Crée un graphique temporel"""
        # Trier par date
        df_sorted = df.sort_values(date_col)
        
        fig = px.line(
            df_sorted,
            x=date_col,
            y=value_col,
            title=f"Évolution temporelle de {value_col}",
            color_discrete_sequence=[self.color_palette[1]]
        )
        
        fig.update_layout(
            **self.layout_template,
            xaxis_title="Date",
            yaxis_title=value_col
        )
        
        return fig
    
    def create_correlation_matrix(self, df: pd.DataFrame) -> go.Figure:
        """Crée une heatmap de corrélation"""
        # Calculer la matrice de corrélation
        corr_matrix = df.corr()
        
        # Créer la heatmap
        fig = px.imshow(
            corr_matrix,
            text_auto=True,
            aspect="auto",
            title="Matrice de corrélation",
            color_continuous_scale="RdBu_r",
            zmin=-1,
            zmax=1
        )
        
        fig.update_layout(
            **self.layout_template,
            width=600,
            height=500
        )
        
        return fig
    
    def create_missing_data_heatmap(self, df: pd.DataFrame) -> go.Figure:
        """Crée une heatmap des valeurs manquantes"""
        # Créer matrice des valeurs manquantes
        missing_data = df.isnull().astype(int)
        
        fig = px.imshow(
            missing_data.T,  # Transposer pour avoir colonnes en y
            title="Carte des valeurs manquantes (blanc = manquant)",
            color_continuous_scale=["white", "red"],
            aspect="auto"
        )
        
        fig.update_layout(
            **self.layout_template,
            xaxis_title="Index des lignes",
            yaxis_title="Colonnes",
            height=400
        )
        
        return fig
    
    def create_distribution_comparison(self, df: pd.DataFrame, columns: List[str]) -> go.Figure:
        """Compare la distribution de plusieurs colonnes numériques"""
        fig = make_subplots(
            rows=1, cols=len(columns),
            subplot_titles=columns,
            shared_yaxes=True
        )
        
        for i, col in enumerate(columns):
            fig.add_trace(
                go.Histogram(
                    x=df[col],
                    name=col,
                    marker_color=self.color_palette[i % len(self.color_palette)],
                    opacity=0.7
                ),
                row=1, col=i+1
            )
        
        fig.update_layout(
            title="Comparaison des distributions",
            **self.layout_template,
            height=400,
            showlegend=False
        )
        
        return fig
    
    def create_statistical_summary_chart(self, df: pd.DataFrame) -> go.Figure:
        """Crée un graphique résumé des statistiques"""
        numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
        
        if len(numeric_cols) == 0:
            return None
        
        # Calculer les statistiques
        stats = df[numeric_cols].describe().T
        
        fig = go.Figure()
        
        # Ajouter les barres pour moyenne et médiane
        fig.add_trace(go.Bar(
            name='Moyenne',
            x=stats.index,
            y=stats['mean'],
            marker_color=self.color_palette[0]
        ))
        
        fig.add_trace(go.Bar(
            name='Médiane',
            x=stats.index,
            y=stats['50%'],
            marker_color=self.color_palette[1]
        ))
        
        fig.update_layout(
            title="Comparaison Moyenne vs Médiane",
            **self.layout_template,
            barmode='group',
            xaxis_title="Variables",
            yaxis_title="Valeurs"
        )
        
        return fig






class PowerBIExporter:
    """Classe pour exporter les données vers PowerBI"""
    
    def __init__(self):
        pass
    
    def create_powerbi_export(self, df: pd.DataFrame, filename: str, include_metadata: bool = True) -> Dict[str, Any]:
        """
        Crée tous les fichiers nécessaires pour PowerBI
        
        Args:
            df: DataFrame à exporter
            filename: Nom du fichier original
            include_metadata: Inclure les métadonnées
            
        Returns:
            Dictionnaire avec les fichiers générés
        """
        exports = {}
        
        # 1. Export Excel optimisé pour PowerBI
        exports['excel'] = self._create_excel_export(df, filename, include_metadata)
        
        # 2. Export CSV propre
        exports['csv'] = self._create_csv_export(df)
        
        # 3. Template PowerBI JSON
        exports['template'] = self._create_powerbi_template(df, filename, include_metadata)
        
        # 4. Fichier de mesures DAX
        exports['dax_measures'] = self._create_dax_measures(df)
        
        return exports
    
    def _create_excel_export(self, df: pd.DataFrame, filename: str, include_metadata: bool) -> bytes:
        """Crée un fichier Excel optimisé pour PowerBI"""
        output = io.BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Feuille principale avec les données
            df_clean = self._prepare_dataframe_for_powerbi(df)
            df_clean.to_excel(writer, sheet_name='Data', index=False)
            
            if include_metadata:
                # Feuille avec métadonnées
                metadata = self._generate_metadata(df, filename)
                metadata_df = pd.DataFrame(list(metadata.items()), columns=['Propriété', 'Valeur'])
                metadata_df.to_excel(writer, sheet_name='Metadata', index=False)
                
                # Feuille avec informations sur les colonnes
                column_info = self._generate_column_info(df)
                column_info.to_excel(writer, sheet_name='Column_Info', index=False)
                
                # Feuille avec suggestions de visualisations
                viz_suggestions = self._generate_visualization_suggestions(df)
                viz_df = pd.DataFrame(viz_suggestions)
                viz_df.to_excel(writer, sheet_name='Viz_Suggestions', index=False)
        
        output.seek(0)
        return output.getvalue()
    
    def _create_csv_export(self, df: pd.DataFrame) -> str:
        """Crée un export CSV propre pour PowerBI"""
        df_clean = self._prepare_dataframe_for_powerbi(df)
        return df_clean.to_csv(index=False, encoding='utf-8-sig')  # UTF-8 avec BOM pour PowerBI
    
    def _create_powerbi_template(self, df: pd.DataFrame, filename: str, include_metadata: bool) -> str:
        """Crée un template JSON pour PowerBI"""
        template = {
            "version": "1.0",
            "name": f"Template for {filename}",
            "created_at": datetime.now().isoformat(),
            "columns": [{
                "name": col,
                "type": str(dtype)
            } for col, dtype in df.dtypes.items()]
        }
        
        if include_metadata:
            template["metadata"] = self._generate_metadata(df, filename)
        
        return json.dumps(template, indent=4, ensure_ascii=False)

    def _create_dax_measures(self, df: pd.DataFrame) -> str:
        """Crée un script DAX avec des mesures courantes"""
        measures = []
        for col in df.select_dtypes(include='number').columns[:5]:  # Limiter à 5 colonnes
            measures.append(f"{col}_Average = AVERAGE('{col}')")
            measures.append(f"{col}_Sum = SUM('{col}')")
            measures.append(f"{col}_Max = MAX('{col}')")
            measures.append(f"{col}_Min = MIN('{col}')")
        return "\n".join(measures)

    def _prepare_dataframe_for_powerbi(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prépare un DataFrame nettoyé pour PowerBI"""
        df_clean = df.copy()
        df_clean.columns = [str(col).strip().replace(' ', '_') for col in df_clean.columns]
        return df_clean

    def _generate_metadata(self, df: pd.DataFrame, filename: str) -> Dict[str, str]:
        """Génère les métadonnées générales"""
        return {
            "Nom du fichier": filename,
            "Nombre de lignes": str(len(df)),
            "Nombre de colonnes": str(len(df.columns)),
            "Date d'export": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def _generate_column_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """Génère un tableau d'information sur les colonnes"""
        data = []
        for col in df.columns:
            data.append({
                "Colonne": col,
                "Type": str(df[col].dtype),
                "Valeurs uniques": df[col].nunique(),
                "Valeurs manquantes": df[col].isnull().sum()
            })
        return pd.DataFrame(data)

    def _generate_visualization_suggestions(self, df: pd.DataFrame) -> list:
        """Génère des suggestions de visualisation"""
        suggestions = []
        for col in df.columns:
            dtype = df[col].dtype
            if dtype in ['int64', 'float64']:
                suggestions.append({
                    "Colonne": col,
                    "Suggestion": "Histogramme ou boîte à moustaches (boxplot)"
                })
            elif dtype == 'object' and df[col].nunique() < 30:
                suggestions.append({
                    "Colonne": col,
                    "Suggestion": "Diagramme en barres ou camembert"
                })
        return suggestions




class UIComponents:
    """Composants UI réutilisables pour le dashboard"""

    def __init__(self):
        pass

    def apply_styles(self):
        """Appliquer les styles CSS globaux"""
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
                .stButton>button {
                    height: 3em;
                    width: 100%;
                    font-size: 1em;
                }
                .css-1cpxqw2 edgvbvh3 {
                    margin-top: -20px;
                }
            </style>
        """, unsafe_allow_html=True)

    def render_sidebar(self):
        """Afficher des infos ou logos dans la sidebar si besoin"""
        st.sidebar.markdown("## Paramètres")
        st.sidebar.markdown("Ce dashboard vous permet :")
        st.sidebar.markdown("- d’importer des fichiers de données")
        st.sidebar.markdown("- d’analyser rapidement les colonnes")
        st.sidebar.markdown("- d’exporter vers PowerBI")
        st.sidebar.markdown("---")
        #st.sidebar.info("Développé avec par [Votre Nom]")
        
        name = st.sidebar.text_input("Votre nom", value=st.session_state.get("user_name", ""))
        if name:
            st.session_state["user_name"] = name
        st.sidebar.info(f"Développé par {st.session_state.get('user_name', '...')}")

    
    def display_file_info(self, uploaded_file):
        """Affiche les métadonnées du fichier importé"""
        st.markdown("#### Informations sur le fichier")
        file_details = {
            "Nom du fichier": uploaded_file.name,
            "Type MIME": uploaded_file.type,
            "Taille (KB)": f"{len(uploaded_file.getbuffer()) / 1024:.1f}"
        }
        st.json(file_details)




class DatabaseManager:
    """Gère la base de données SQLite pour stocker les importations"""

    def __init__(self, db_path="data_imports.db"):
        self.db_path = db_path
        self.conn = None

    def init_db(self):
        """Initialise la base de données et la table si elle n'existe pas"""
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
        """Sauvegarde les informations d'un fichier importé"""
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
        """Récupère l'historique des fichiers importés"""
        if self.conn is None:
            self.init_db()

        try:
            df = pd.read_sql_query("SELECT * FROM uploads ORDER BY upload_date DESC", self.conn)
            return df
        except Exception as e:
            st.error(f"Erreur lors de la lecture de la base de données : {e}")
            return pd.DataFrame()

import streamlit as st
import pandas as pd
from pathlib import Path
import sys

# Ajouter le dossier utils au path
sys.path.append(str(Path(__file__).parent / "utils"))

#from data_extractor import DataExtractor
#from data_analyzer import DataAnalyzer
#from visualizer import DataVisualizer
#from powerbi_exporter import PowerBIExporter
#from database_manager import DatabaseManager
#from ui_components import UIComponents

# Configuration de la page
st.set_page_config(
    page_title="Data Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialiser les composants
db_manager = DatabaseManager()
ui_components = UIComponents()
data_extractor = DataExtractor()
data_analyzer = DataAnalyzer()
data_visualizer = DataVisualizer()
powerbi_exporter = PowerBIExporter()

# Initialiser la base de données
db_manager.init_db()

# Appliquer les styles CSS
ui_components.apply_styles()

def main():
    st.markdown('<h1 class="main-header">📊 Dashboard Analytics Pro</h1>', unsafe_allow_html=True)
    
    # Barre latérale
    ui_components.render_sidebar()
    #page = st.sidebar.radio("Navigation", ["🔄 Importer", "📈 Analyser", "📚 Historique", "⚙️ PowerBI"], label_visibility="collapsed")
    if 'page' not in st.session_state:
        st.session_state['page'] = "🔄 Importer"

    page = st.sidebar.radio(
        "Navigation",
        ["🔄 Importer", "📈 Analyser", "📚 Historique", "⚙️ PowerBI"],
        index=["🔄 Importer", "📈 Analyser", "📚 Historique", "⚙️ PowerBI"].index(st.session_state['page'])
    )
    st.session_state['page'] = page


    # Navigation entre les pages
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
    
    # Zone de drag & drop améliorée
    uploaded_file = st.file_uploader(
        "Glissez-déposez vos fichiers ici ou cliquez pour parcourir", 
        type=['csv', 'xlsx', 'xls', 'json', 'txt'],
        help="Formats supportés: CSV, Excel (.xlsx, .xls), JSON, TXT"
    )
    
    if uploaded_file is not None:
        # Afficher les détails du fichier
        ui_components.display_file_info(uploaded_file)
        
        # Extraction des données
        with st.spinner('🔄 Extraction des données en cours...'):
            df = data_extractor.extract_data(uploaded_file)
        
        if df is not None and not df.empty:
            st.success("✅ Données importées avec succès!")
            
            # Aperçu des données
            st.markdown('<h3 class="section-header">👀 Aperçu des données</h3>', unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)

            with col1:
                if st.button("📈 Analyser maintenant", type="primary", use_container_width=True, key="analyze_btn_1"):
                    st.switch_page("pages/analyze.py") if hasattr(st, 'switch_page') else st.rerun()

            with col2:
                csv_data = df.to_csv(index=False)
                st.download_button(
                    "💾 Télécharger CSV",
                    csv_data,
                    file_name=f"cleaned_{uploaded_file.name}.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key="download_csv_main"
                )


            with col3:
                if st.button("📈 Analyser maintenant", type="primary", use_container_width=True, key="analyze_btn_2"):
                    st.session_state['page'] = "📈 Analyser"
                    st.rerun()

            
            # Prévisualisation avec pagination
            st.dataframe(
                df.head(20),
                use_container_width=True,
                height=400
            )
            
            # Informations sur les colonnes
            if st.expander("🔍 Informations détaillées sur les colonnes"):
                col_info = data_analyzer.get_column_info(df)
                st.dataframe(col_info, use_container_width=True)
            
            # Sauvegarder dans la session et la base
            st.session_state['data'] = df
            st.session_state['filename'] = uploaded_file.name
            
            # Sauvegarder dans la base de données
            db_manager.save_upload(uploaded_file.name, uploaded_file.type, df)
            
            # Actions disponibles
            st.markdown('<h3 class="section-header">🎯 Actions disponibles</h3>', unsafe_allow_html=True)
            col1, col2, col3 = st.columns(3)
            
            with col1:
                if st.button("📈 Analyser maintenant", type="primary", use_container_width=True , key="analyze_now_btn_1"):
                    st.switch_page("pages/analyze.py") if hasattr(st, 'switch_page') else st.rerun()
            
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
                if st.button("📈 Analyser maintenant", type="primary", use_container_width=True , key="analyze_now_btn_2"):
                    st.session_state['page'] = "📈 Analyser"
                    st.rerun()


def render_analysis_page():
    st.markdown('<h2 class="section-header">📈 Analyse des données</h2>', unsafe_allow_html=True)
    
    if 'data' not in st.session_state:
        st.warning("⚠️ Aucune donnée à analyser. Veuillez d'abord importer un fichier.")
        if st.button("➡️ Aller à l'importation", type="primary"):
            st.rerun()
        return
    
    df = st.session_state['data']
    filename = st.session_state.get('filename', 'données')
    
    # Onglets d'analyse
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 Aperçu", "📊 Statistiques", "📈 Visualisations", 
        "🔍 Qualité", "🎯 Insights"
    ])
    
    with tab1:
        st.markdown(f"### 📁 Analyse de: **{filename}**")
        
        # Métriques générales
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
        
        # Aperçu des données avec options de filtrage
        st.markdown("#### 👀 Aperçu des données")
        
        # Options de filtrage
        if st.checkbox("🔍 Activer le filtrage"):
            selected_cols = st.multiselect(
                "Sélectionner les colonnes à afficher",
                df.columns.tolist(),
                default=df.columns.tolist()[:10]
            )
            if selected_cols:
                st.dataframe(df[selected_cols], use_container_width=True, height=400)
            else:
                st.dataframe(df, use_container_width=True, height=400)
        else:
            st.dataframe(df, use_container_width=True, height=400)
    
    with tab2:
        st.markdown("### 📊 Statistiques descriptives")
        
        # Statistiques pour colonnes numériques
        numeric_df = df.select_dtypes(include=['number'])
        if not numeric_df.empty:
            st.markdown("#### 🔢 Colonnes numériques")
            st.dataframe(numeric_df.describe(), use_container_width=True)
            
            # Matrice de corrélation
            if len(numeric_df.columns) > 1:
                st.markdown("#### 🔗 Matrice de corrélation")
                corr_fig = data_visualizer.create_correlation_matrix(numeric_df)
                st.plotly_chart(corr_fig, use_container_width=True)
        
        # Statistiques pour colonnes catégorielles
        cat_df = df.select_dtypes(include=['object', 'category'])
        if not cat_df.empty:
            st.markdown("#### 📝 Colonnes catégorielles")
            cat_stats = data_analyzer.get_categorical_stats(cat_df)
            st.dataframe(cat_stats, use_container_width=True)
    
    with tab3:
        st.markdown("### 📈 Visualisations automatiques")
        
        # Générer les visualisations
        charts = data_visualizer.auto_generate_charts(df)
        
        if not charts:
            st.info("ℹ️ Aucune visualisation automatique disponible pour ce jeu de données.")
        else:
            # Organisation en colonnes pour un meilleur affichage
            for i, (chart_type, name, fig) in enumerate(charts):
                if i % 2 == 0:
                    col1, col2 = st.columns(2)
                
                with col1 if i % 2 == 0 else col2:
                    #st.plotly_chart(fig, use_container_width=True)
                    st.plotly_chart(fig, use_container_width=True, key=f"{chart_type}_{i}")

    
    with tab4:
        st.markdown("### 🔍 Qualité des données")
        
        # Analyse de la qualité
        quality_report = data_analyzer.analyze_data_quality(df)
        
        # Métriques de qualité
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
        
        # Détail par colonne
        st.markdown("#### 📋 Détail par colonne")
        st.dataframe(quality_report, use_container_width=True)
        
        # Visualisation des valeurs manquantes
        if df.isnull().any().any():
            missing_fig = data_visualizer.create_missing_data_heatmap(df)
            st.plotly_chart(missing_fig, use_container_width=True)
    
    with tab5:
        st.markdown("### 🎯 Insights automatiques")
        
        # Générer des insights
        insights = data_analyzer.generate_insights(df)
        
        for insight in insights:
            st.info(f"💡 {insight}")
        
        # Recommandations
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
        # Affichage avec colonnes personnalisées
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
        
        # Sélection et rechargement
        if len(uploads_df) > 0:
            selected_id = st.selectbox(
                "🔄 Sélectionner une importation à recharger",
                uploads_df['id'].tolist(),
                format_func=lambda x: f"ID {x}: {uploads_df[uploads_df['id']==x]['filename'].iloc[0]}"
            )
            
            if st.button("🔄 Recharger cette importation", type="primary"):
                # Recharger les données (simulation)
                selected_row = uploads_df[uploads_df['id'] == selected_id].iloc[0]
                st.session_state['filename'] = selected_row['filename']
                st.success(f"✅ Importation {selected_row['filename']} rechargée!")

def render_powerbi_page():
    st.markdown('<h2 class="section-header">⚙️ Export PowerBI</h2>', unsafe_allow_html=True)
    
    if 'data' not in st.session_state:
        st.warning("⚠️ Aucune donnée à exporter. Veuillez d'abord importer un fichier.")
        return
    
    df = st.session_state['data']
    filename = st.session_state.get('filename', 'data')
    
    st.markdown("### 🎯 Préparation pour PowerBI")
    
    # Options d'export
    col1, col2 = st.columns(2)
    
    with col1:
        export_format = st.selectbox(
            "Format d'export",
            ["Excel (.xlsx)", "CSV", "JSON", "Template PowerBI"]
        )
    
    with col2:
        include_metadata = st.checkbox("Inclure les métadonnées", value=True)
    
    # Prévisualisation
    st.markdown("#### 👀 Prévisualisation des données à exporter")
    st.dataframe(df.head(), use_container_width=True)
    
    # Génération des fichiers d'export
    if st.button("🚀 Générer l'export PowerBI", type="primary"):
        with st.spinner("⏳ Génération en cours..."):
            exports = powerbi_exporter.create_powerbi_export(df, filename, include_metadata)
        
        st.success("✅ Export généré avec succès!")
        
        # Boutons de téléchargement
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.download_button(
                "📊 Télécharger Excel",
                exports['excel'],
                file_name=f"{filename}_powerbi.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                key="download_excel_pbi"
            )
        
        with col2:
            st.download_button(
                "📋 Télécharger CSV",
                exports['csv'],
                file_name=f"{filename}_powerbi.csv",
                mime="text/csv",
                key="download_csv_pbi"
            )
        
        with col3:
            st.download_button(
                "⚙️ Template PowerBI",
                exports['template'],
                file_name=f"{filename}_template.json",
                mime="application/json",
                key="download_template_pbi"
            )
        
        # Instructions PowerBI
        with st.expander("📖 Instructions d'importation PowerBI"):
            st.markdown("""
            **Pour importer dans PowerBI Desktop:**
            
            1. **Via Excel:**
               - Téléchargez le fichier Excel
               - Dans PowerBI: Accueil → Obtenir les données → Excel
               - Sélectionnez le fichier téléchargé
            
            2. **Via CSV:**
               - Téléchargez le fichier CSV
               - Dans PowerBI: Accueil → Obtenir les données → Texte/CSV
               - Sélectionnez le fichier CSV
            
            3. **Via Template:**
               - Téléchargez le template JSON
               - Utilisez-le comme référence pour configurer vos visualisations
            """)

if __name__ == "__main__":
    main()