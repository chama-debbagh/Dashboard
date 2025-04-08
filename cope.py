# Importation des bibliothèques nécessaires
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from dash import Dash, html, dcc, callback, Output, Input, callback_context
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os
import json
import base64
import io
from dash import State

# Classe pour gérer la connexion et l'extraction des données
class DataConnector:
    def __init__(self, source_type, connection_params=None):
        """
        Initialise le connecteur de données
        source_type: 'csv', 'excel', 'sql', 'api'
        connection_params: paramètres de connexion spécifiques à la source
        """
        self.source_type = source_type
        self.connection_params = connection_params
        self.data = None
        
    def connect(self):
        """Établit la connexion à la source de données"""
        try:
            if self.source_type == 'csv':
                self.data = pd.read_csv(self.connection_params['file_path'])
                return True
            elif self.source_type == 'excel':
                self.data = pd.read_excel(self.connection_params['file_path'], 
                                         sheet_name=self.connection_params.get('sheet_name', 0))
                return True
            elif self.source_type == 'sql':
                engine = create_engine(self.connection_params['connection_string'])
                self.data = pd.read_sql(self.connection_params['query'], engine)
                return True
            # Ajout d'un type 'memory' pour utiliser des données déjà en mémoire
            elif self.source_type == 'memory':
                # Pas besoin de connexion, les données sont déjà fournies
                return True
            else:
                print(f"Source type {self.source_type} not supported yet")
                return False
        except Exception as e:
            print(f"Error connecting to data source: {e}")
            return False
    
    def get_data(self):
        """Retourne les données obtenues"""
        return self.data
    
    def set_data(self, data):
        """Définit les données directement"""
        self.data = data
        return self
    
    def export_to_tableau(self, output_path):
        """Exporte les données dans un format compatible avec Tableau (.hyper ou .csv)"""
        if self.data is not None:
            if output_path.endswith('.csv'):
                self.data.to_csv(output_path, index=False)
                print(f"Data exported to Tableau format at {output_path}")
                return True
            else:
                # Pour les formats .hyper, il faudrait utiliser l'API Tableau Hyper
                print("Hyper format requires Tableau Hyper API, exporting as CSV instead")
                csv_path = output_path.replace('.hyper', '.csv')
                self.data.to_csv(csv_path, index=False)
                print(f"Data exported to Tableau CSV format at {csv_path}")
                return True
        return False
    
    def export_to_powerbi(self, output_path):
        """Exporte les données dans un format compatible avec Power BI (.pbix via .csv)"""
        if self.data is not None:
            # Power BI peut importer directement depuis CSV
            csv_path = output_path.replace('.pbix', '.csv')
            self.data.to_csv(csv_path, index=False)
            print(f"Data exported for Power BI import at {csv_path}")
            
            # Création d'un fichier de métadonnées pour faciliter l'import
            metadata = {
                "columns": list(self.data.columns),
                "data_types": {col: str(self.data[col].dtype) for col in self.data.columns}
            }
            
            with open(csv_path + '.metadata.json', 'w') as f:
                json.dump(metadata, f)
            
            print("Metadata file created for Power BI")
            return True
        return False

# Classe pour la préparation et la transformation des données
class DataProcessor:
    def __init__(self, data):
        """
        Initialise le processeur de données
        data: DataFrame pandas contenant les données brutes
        """
        self.raw_data = data
        self.processed_data = data.copy()
    
    def clean_data(self):
        """Nettoie les données (valeurs manquantes, doublons, erreurs)"""
        # Supprimer les lignes avec des valeurs manquantes
        self.processed_data = self.processed_data.dropna()
        
        # Supprimer les doublons
        self.processed_data = self.processed_data.drop_duplicates()
        
        return self
    
    def transform_data(self, transformations=None):
        """Applique des transformations aux données"""
        if transformations:
            for col, transform_func in transformations.items():
                if col in self.processed_data.columns:
                    self.processed_data[col] = self.processed_data[col].apply(transform_func)
        
        return self
    
    def aggregate_data(self, group_by, agg_dict):
        """Agrège les données selon les spécifications"""
        self.processed_data = self.processed_data.groupby(group_by).agg(agg_dict).reset_index()
        return self
    
    def get_processed_data(self):
        """Retourne les données transformées"""
        return self.processed_data

# Classe pour créer le dashboard avec Dash
class DashboardCreator:
    def __init__(self, data):
        """
        Initialise le créateur de dashboard
        data: DataFrame pandas contenant les données à visualiser
        """
        self.data = data
        self.app = Dash(__name__)
        
    def create_layout(self, title="Dashboard Python"):
        """Crée la mise en page du dashboard"""
        # Composant pour télécharger des fichiers
        upload_component = html.Div([
            html.H3("Charger vos données"),
            dcc.Upload(
                id='upload-data',
                children=html.Div([
                    'Glissez-déposez ou ',
                    html.A('sélectionnez un fichier CSV ou Excel')
                ]),
                style={
                    'width': '100%',
                    'height': '60px',
                    'lineHeight': '60px',
                    'borderWidth': '1px',
                    'borderStyle': 'dashed',
                    'borderRadius': '5px',
                    'textAlign': 'center',
                    'margin': '10px'
                },
                multiple=False
            ),
            html.Div(id='upload-status')
        ])
        
        # Créer un dropdown initial vide mais avec l'ID défini
        default_filter_values = html.Div([
            html.Label("Sélectionner une valeur:"),
            dcc.Dropdown(
                id='value-filter',
                options=[],
                value=None
            )
        ], id='filter-value-container')
        
        self.app.layout = html.Div([
            html.H1(title, style={'textAlign': 'center'}),
            upload_component,
            
            html.Div([
                html.Div([
                    html.H3("Filtres"),
                    # Exemple de filtre par colonne (à adapter selon vos données)
                    html.Label("Sélectionner une colonne:"),
                    dcc.Dropdown(
                        id='column-filter',
                        options=[{'label': col, 'value': col} for col in self.data.columns 
                                if self.data[col].dtype == 'object'],
                        value=None
                    ),
                    default_filter_values  # Ajout de l'élément initial vide mais avec ID
                ], style={'width': '30%', 'display': 'inline-block', 'vertical-align': 'top'}),
                
                html.Div([
                    html.H3("Graphiques"),
                    dcc.Tabs([
                        dcc.Tab(label="Graphique 1", children=[
                            dcc.Graph(id='graph1')
                        ]),
                        dcc.Tab(label="Graphique 2", children=[
                            dcc.Graph(id='graph2')
                        ]),
                        dcc.Tab(label="Tableau de données", children=[
                            html.Div(id='data-table')
                        ])
                    ])
                ], style={'width': '70%', 'display': 'inline-block'})
            ]),
            
            html.Div([
                html.H3("Exporter les données"),
                html.Button("Exporter pour Tableau", id="btn-export-tableau", n_clicks=0),  # Initialisation n_clicks
                html.Button("Exporter pour Power BI", id="btn-export-powerbi", n_clicks=0, style={'marginLeft': '10px'}),  # Initialisation n_clicks
                html.Div(id='export-status')
            ], style={'marginTop': '20px'})
        ])
        
       # Modification du callback d'upload pour mieux mettre à jour les filtres
@self.app.callback(
    [Output('upload-status', 'children'),
     Output('column-filter', 'options'),
     Output('column-filter', 'value')],
    Input('upload-data', 'contents'),
    State('upload-data', 'filename')
)
def update_output(contents, filename):
    if contents is not None:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)
        try:
            if 'csv' in filename.lower():
                df = pd.read_csv(io.StringIO(decoded.decode('utf-8')))
            elif 'xls' in filename.lower():
                df = pd.read_excel(io.BytesIO(decoded))
            else:
                return 'Format de fichier non supporté. Utilisez CSV ou Excel.', [], None
                
            # Mise à jour des données du dashboard
            self.data = df
            
            # Mise à jour des options de colonnes - ici on inclut TOUTES les colonnes
            options = [{'label': col, 'value': col} for col in df.columns]
            
            return html.Div([
                f'Fichier "{filename}" chargé avec succès. {len(df)} lignes et {len(df.columns)} colonnes.'
            ]), options, options[0]['value'] if options else None  # Sélectionner la première colonne par défaut
        except Exception as e:
            return html.Div([
                'Erreur lors du traitement du fichier: ' + str(e)
            ]), [], None
    return '', [], None
        
        # Callback pour mettre à jour les valeurs du filtre
        @self.app.callback(
            Output('filter-value-container', 'children'),
            Input('column-filter', 'value')
        )
        def update_filter_values(selected_column):
            if selected_column is None:
                # Retourner un dropdown vide mais avec l'ID défini
                return [
                    html.Label("Sélectionner une valeur:"),
                    dcc.Dropdown(
                        id='value-filter',
                        options=[],
                        value=None
                    )
                ]
            
            values = sorted(self.data[selected_column].unique())
            return [
                html.Label("Sélectionner une valeur:"),
                dcc.Dropdown(
                    id='value-filter',
                    options=[{'label': str(val), 'value': str(val)} for val in values],
                    value=str(values[0]) if len(values) > 0 else None,
                    multi=True
                )
            ]
        
        # Callback pour mettre à jour le graphique 1
        # Amélioration du callback pour le graphique 1
@self.app.callback(
    Output('graph1', 'figure'),
    [Input('column-filter', 'value'),
     Input('value-filter', 'value')]
)
def update_graph1(column, values):
    # Si pas de colonne sélectionnée ou pas de valeurs, afficher une explication
    if column is None or values is None or values == []:
        # Créer un graphique par défaut plus descriptif
        fig = px.bar(
            self.data.iloc[:10], 
            x=self.data.columns[0], 
            y=self.data.columns[1] if len(self.data.columns) > 1 else self.data.columns[0],
            title="Sélectionnez une colonne et des valeurs pour visualiser les données"
        )
        fig.update_layout(
            xaxis_title=self.data.columns[0],
            yaxis_title=self.data.columns[1] if len(self.data.columns) > 1 else "Count",
            template="plotly_white"
        )
        return fig
    
    # Filtrer les données selon la sélection
    if isinstance(values, list):
        filtered_data = self.data[self.data[column].isin(values)]
    else:
        filtered_data = self.data[self.data[column] == values]
    
    # S'assurer que filtered_data n'est pas vide
    if filtered_data.empty:
        fig = px.bar(
            self.data.iloc[:10],
            x=self.data.columns[0],
            y=self.data.columns[1] if len(self.data.columns) > 1 else self.data.columns[0],
            title="Aucune donnée disponible pour cette sélection"
        )
        return fig
    
    # Créer un graphique basé sur les données filtrées
    numeric_cols = filtered_data.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        # Utiliser la première colonne numérique
        y_column = numeric_cols[0]
        
        # Créer un graphique plus descriptif
        fig = px.bar(
            filtered_data,
            x=column,
            y=y_column,
            title=f"Distribution de {y_column} par {column}",
            labels={column: column, y_column: y_column},
            text_auto=True  # Afficher les valeurs sur les barres
        )
        fig.update_layout(
            xaxis_title=column,
            yaxis_title=y_column,
            legend_title="Légende",
            template="plotly_white"
        )
        return fig
    else:
        # Utiliser un compte si pas de colonne numérique
        counts = filtered_data[column].value_counts().reset_index()
        counts.columns = [column, 'count']
        fig = px.bar(
            counts,
            x=column,
            y='count',
            title=f"Nombre d'occurrences par {column}",
            text_auto=True  # Afficher les valeurs sur les barres
        )
        fig.update_layout(
            xaxis_title=column,
            yaxis_title="Nombre d'occurrences",
            template="plotly_white"
        )
        return fig
        
        # Callback pour mettre à jour le graphique 2
        @self.app.callback(
            Output('graph2', 'figure'),
            [Input('column-filter', 'value'),
             Input('value-filter', 'value')]
        )
        def update_graph2(column, values):
            # Graphique par défaut
            if column is None or values is None or values == []:
                numeric_cols = self.data.select_dtypes(include=['number']).columns
                if len(numeric_cols) >= 2:
                    fig = px.scatter(self.data.iloc[:50], x=numeric_cols[0], y=numeric_cols[1],
                                    title="Exemple de nuage de points")
                else:
                    fig = px.line(self.data.iloc[:20], title="Exemple de graphique linéaire")
                return fig
            
            # Filtrer les données selon la sélection
            if isinstance(values, list):
                filtered_data = self.data[self.data[column].isin(values)]
            else:
                filtered_data = self.data[self.data[column] == values]
            
            # S'assurer que filtered_data n'est pas vide
            if filtered_data.empty:
                numeric_cols = self.data.select_dtypes(include=['number']).columns
                if len(numeric_cols) >= 2:
                    fig = px.scatter(self.data.iloc[:50], x=numeric_cols[0], y=numeric_cols[1],
                                    title="Données filtrées indisponibles - Graphique par défaut")
                else:
                    fig = px.line(self.data.iloc[:20], title="Données filtrées indisponibles - Graphique par défaut")
                return fig
            
            # Créer un graphique basé sur les données filtrées
            numeric_cols = filtered_data.select_dtypes(include=['number']).columns
            if len(numeric_cols) >= 2:
                fig = px.scatter(filtered_data, x=numeric_cols[0], y=numeric_cols[1],
                                color=column if len(filtered_data[column].unique()) <= 10 else None,
                                title=f"Relation entre {numeric_cols[0]} et {numeric_cols[1]}")
                return fig
            else:
                # Utiliser un autre type de graphique
                counts = filtered_data[column].value_counts().reset_index()
                counts.columns = [column, 'count']
                fig = px.pie(counts, values='count', names=column, 
                            title=f"Répartition des valeurs de {column}")
                return fig
        
        # Callback pour mettre à jour le tableau de données
        # Amélioration de l'affichage du tableau de données
@self.app.callback(
    Output('data-table', 'children'),
    [Input('column-filter', 'value'),
     Input('value-filter', 'value')]
)
def update_table(column, values):
    if column is None or values is None or values == []:
        df_display = self.data.head(10)
        table_title = "Aperçu des 10 premières lignes de données"
    else:
        # Filtrer les données selon la sélection
        if isinstance(values, list):
            df_display = self.data[self.data[column].isin(values)].head(20)
            table_title = f"Données filtrées pour {column} = {', '.join(values)} (20 premières lignes)"
        else:
            df_display = self.data[self.data[column] == values].head(20)
            table_title = f"Données filtrées pour {column} = {values} (20 premières lignes)"
    
    # S'assurer que df_display n'est pas vide
    if df_display.empty:
        df_display = self.data.head(10)
        table_title = "Aucune donnée ne correspond à ce filtre - Affichage des 10 premières lignes"
    
    return html.Div([
        html.H4(table_title, style={'textAlign': 'center'}),
        html.Table(
            # En-tête
            [html.Tr([html.Th(col, style={'backgroundColor': '#f0f0f0', 'fontWeight': 'bold'}) 
                      for col in df_display.columns])] +
            # Corps
            [html.Tr([
                html.Td(df_display.iloc[i][col]) for col in df_display.columns
            ], style={'backgroundColor': '#f9f9f9' if i % 2 else 'white'}) 
             for i in range(min(len(df_display), 20))]
        , style={'width': '100%', 'border': '1px solid #ddd', 'borderCollapse': 'collapse'})
    ])
        
        # Callback pour les exportations
        @self.app.callback(
            Output('export-status', 'children'),
            [Input('btn-export-tableau', 'n_clicks'),
             Input('btn-export-powerbi', 'n_clicks')]
        )
        def export_data(tableau_clicks, powerbi_clicks):
            if tableau_clicks is None and powerbi_clicks is None:
                return ""
            
            ctx = callback_context
            if not ctx.triggered:
                return ""
            
            button_id = ctx.triggered[0]['prop_id'].split('.')[0]
            
            if button_id == "btn-export-tableau" and tableau_clicks > 0:
                # Au lieu d'enregistrer sur le serveur, créer un lien de téléchargement
                csv_string = self.data.to_csv(index=False, encoding='utf-8')
                csv_b64 = base64.b64encode(csv_string.encode()).decode()
                href = f'data:text/csv;charset=utf-8;base64,{csv_b64}'
                
                return html.Div([
                    html.A(
                        'Télécharger les données pour Tableau (CSV)',
                        download="dashboard_export_tableau.csv",
                        href=href,
                        target="_blank",
                        style={'color': 'green', 'textDecoration': 'underline'}
                    )
                ])
            
            elif button_id == "btn-export-powerbi" and powerbi_clicks > 0:
                # Même chose pour Power BI
                csv_string = self.data.to_csv(index=False, encoding='utf-8')
                csv_b64 = base64.b64encode(csv_string.encode()).decode()
                href = f'data:text/csv;charset=utf-8;base64,{csv_b64}'
                
                return html.Div([
                    html.A(
                        'Télécharger les données pour Power BI (CSV)',
                        download="dashboard_export_powerbi.csv",
                        href=href,
                        target="_blank",
                        style={'color': 'green', 'textDecoration': 'underline'}
                    )
                ])
            
            return ""
    
    def run_dashboard(self, debug=True, port=8050):
        """Lance le dashboard"""
        self.app.run(debug=debug, port=port)

# Exemple d'utilisation du code ci-dessus
def main():
    # Création de données d'exemple (à remplacer par vos propres données)
    data = {
        'Date': pd.date_range(start='2023-01-01', periods=100),
        'Catégorie': np.random.choice(['A', 'B', 'C', 'D'], 100),
        'Région': np.random.choice(['Nord', 'Sud', 'Est', 'Ouest', 'Centre'], 100),
        'Ventes': np.random.randint(100, 1000, 100),
        'Profit': np.random.randint(-100, 500, 100),
        'Clients': np.random.randint(10, 100, 100)
    }
    df = pd.DataFrame(data)
    
    # Traitement des données
    processor = DataProcessor(df)
    processed_df = (processor
                    .clean_data()
                    .transform_data({
                        'Ventes': lambda x: x * 1.1,  # exemple: augmentation des ventes de 10%
                        'Date': lambda x: x.strftime('%Y-%m')  # regroupement par mois
                    })
                    .aggregate_data(
                        group_by=['Date', 'Catégorie', 'Région'],
                        agg_dict={
                            'Ventes': 'sum',
                            'Profit': 'sum',
                            'Clients': 'mean'
                        }
                    )
                    .get_processed_data())
    
    # Création et lancement du dashboard
    dashboard = DashboardCreator(processed_df)
    dashboard.create_layout(title="Dashboard de Ventes et Profits")
    
    print("Dashboard créé avec succès!")
    print("Utilisez les commandes suivantes pour lancer le dashboard:")
    print("dashboard.run_dashboard()")
    
    return dashboard

# Cette condition permet d'exécuter le code uniquement lorsque le script est lancé directement
if __name__ == "__main__":
    dashboard = main()
    dashboard.run_dashboard()