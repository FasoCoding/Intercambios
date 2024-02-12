# LIBRERIAS
import pandas as pd
import sqlalchemy as sa
import networkx as nx
import textwrap

import sqlalchemy_access as sa_a
import sqlalchemy_access.pyodbc as sa_a_pyodbc

# PATHS
# especificación de las rutas y configuraciones a utilizar en el código.
# **En producción** la ruta de DB y NODES debería estar direccionada a la
# carpeta antecedentes.
DRIVER = r"{Microsoft Access Driver (*.mdb, *.accdb)}"
PATH_DB = r"/Antecedentes/Model PRGdia_Full_Definitivo Solution.accdb"
PATH_NODES = r"/Antecedentes/Topologia.xlsx"


def neighborhood(G, node, n):
    """función para encontrar las barras adyacentes

    Args:
        G (nx): sistema de nodos.
        node (str): nodo de referencia.
        n (int): distancía sobre barra de referencia.

    Returns:
        list: set de barras a una distancia n de la referencia
    """
    path_lengths = nx.single_source_dijkstra_path_length(G, node)
    return [node for node, length in path_lengths.items() if length == n]


def check_cmg(criterio_query: str, data: pd.DataFrame) -> bool:
   
    check_cmg_bool = data.query(criterio_query)

    return check_cmg_bool.costo_marginal.empty


def main_loop(data_total: pd.DataFrame,
              data_filt: pd.DataFrame,
              ref: str,
              G):

    print("Iniciando loop de cálculo de curtailment en subzona...")

    total_nodes = data_total.Node.unique()
    total_curt_nodes = data_filt.Node.unique()
    curt_nodes = []
    node_ini = ref
    node_neig = neighborhood(G, node_ini, 1)
    counter = 0
    print(f"interación {counter} con barra de cálculo {node_ini}")

    # El loop se repite un máximo de veces igual al
    # total de barras con curtailment
    while counter < len(total_curt_nodes):

        for node in node_neig:
            if node in total_nodes and node not in total_curt_nodes:
                pass
            elif node in curt_nodes:
                # este paso es para evitar duplicidad en la lista
                pass
            else:
                curt_nodes += [node]

        if len(curt_nodes) > 0 and len(curt_nodes) > counter:
            node_ini = curt_nodes[counter]
            node_neig = neighborhood(G, node_ini, 1)
            counter += 1
            print(f"interación {counter} con barra de cálculo {node_ini}")
        else:
            break

    print('Loop terminado, guardando curtailment para subsistema\n')
    
    return curt_nodes


def run_loop(day: int, 
             node: str, 
             G,
             data: pd.DataFrame) -> pd.DataFrame:

    print(f'\nIniciando calculo para día {day}\n')

    criterio_cmg = 0
    criterio_dia = day
    criterio_node = [node]

    criterio_query = (
        f'Node == {criterio_node} and '
        f'day_id == {criterio_dia} and '
        f'costo_marginal <= {criterio_cmg}'
    )

    if not check_cmg(criterio_query, data):

        df_filter = data.query(criterio_query) \
            .filter(['period_of_day'])
        hours = [period for period in df_filter.period_of_day]

        query_filter = (
            f'period_of_day == {hours} and '
            f'costo_marginal <= {criterio_cmg}'
        )

        df_curt_total = data.query(query_filter)

        curt_nodes = main_loop(data, df_curt_total, node, G)
    
        # nuevo query para armar el csv final agrupado por día hora
        query_curt = (
            f'Node == {curt_nodes} and '
            f'day_id == {criterio_dia}'
        )
        df_curt = data.query(query_curt)
        df_curt = df_curt.groupby(by=[
            'datetime',
            'day_id',
            'period_of_day'
        ]).agg(
            {'curtailment': 'sum'}
        ).reset_index()
        mask = df_curt['period_of_day'].isin(hours)
        df_curt.loc[~mask, 'curtailment'] = 0

    else:
        print("No hay curtailment")

    return df_curt


def main() -> None:

    # se obtiene la dirección de trabajo
    # **En produccióm** el get dir debería ser la carpeta del
    # día de programación preguntada mediante promp
    print("Determinando ruta")
    path_dir = input("Agregue la dirección de la carpeta principal del programa:\n")
    print(f"Ruta de trabajo: {path_dir}\n")

    # CONEXION CON PLEXOS
    # string que establece los requerimientos para conexión con la BD
    connection_string = (
        f"DRIVER={DRIVER};"
        f"DBQ={path_dir + PATH_DB};"
        f"ExtendedAnsiSQL=1;"
    )

    # string para la creación de motor de conexión odbc para BD
    # con SQLAlchemy (SA)
    connection_url = sa.engine.URL.create(
        "access+pyodbc",
        query={"odbc_connect": connection_string}
    )

    # creación del motor de conexión con la BD
    print("Iniciando conexión con base de datos...")
    engine = sa.create_engine(connection_url)

    # CONSULTAS SQL
    # las consultas vienen de las utilizadas en la macro gen / out
    sql_curt = textwrap.dedent("""
            SELECT
            t_child.name AS child_name, t_period_0.datetime,t_period_0.day_id,
            t_period_0.period_of_day, SUM(d.value) AS curtailment
            FROM ((((((((((t_data_0 AS d
                INNER JOIN t_key AS k ON d.key_id = k.key_id)
                INNER JOIN t_period_0 ON d.period_id = t_period_0.interval_id)
                INNER JOIN t_membership AS m
                    ON k.membership_id = m.membership_id)
                INNER JOIN t_property AS p ON k.property_id = p.property_id)
                INNER JOIN t_unit ON p.unit_id = t_unit.unit_id)
                INNER JOIN t_collection AS c
                    ON m.collection_id = c.collection_id)
                INNER JOIN t_object AS t_parent
                    ON m.parent_object_id = t_parent.object_id)
                INNER JOIN t_object AS t_child
                    ON m.child_object_id = t_child.object_id)
                INNER JOIN t_category
                    ON (t_child.class_id = t_category.class_id)
                    AND (t_child.category_id = t_category.category_id))
                INNER JOIN t_model ON k.model_id = t_model.model_id)
                INNER JOIN t_class AS tc ON t_child.class_id = tc.class_id
            WHERE (p.name = 'Capacity Curtailed') And tc.name = 'Generator'
            GROUP BY t_child.name,  t_period_0.datetime,
            t_period_0.day_id, t_period_0.period_of_day;""")
    sql_gen = textwrap.dedent("""
        SELECT
        t_child.name AS child_name, t_period_0.datetime,
        Avg(d.value) as generation
        FROM (((((((((((t_data_0 AS d
            INNER JOIN t_key AS k ON d.key_id = k.key_id)
            INNER JOIN t_membership AS m ON k.membership_id = m.membership_id)
            INNER JOIN t_property AS p ON k.property_id = p.property_id)
            INNER JOIN t_unit ON p.unit_id = t_unit.unit_id)
            INNER JOIN t_collection AS c ON m.collection_id = c.collection_id)
            INNER JOIN t_object AS t_parent
                ON m.parent_object_id = t_parent.object_id)
            INNER JOIN t_object AS t_child
                ON m.child_object_id = t_child.object_id)
            INNER JOIN t_category
                ON (t_child.category_id = t_category.category_id)
                AND (t_child.class_id = t_category.class_id))
            INNER JOIN t_model ON k.model_id = t_model.model_id)
            INNER JOIN t_class AS tc ON t_child.class_id = tc.class_id)
            INNER JOIN t_phase_3 ON d.period_id = t_phase_3.period_id)
            INNER JOIN t_period_0
                ON t_phase_3.interval_id = t_period_0.interval_id
        WHERE (((tc.name)='Generator') AND ((p.Name) In ('Generation')))
        GROUP BY  t_child.name, t_period_0.datetime;""")
    sql_genbar = textwrap.dedent("""
        SELECT DISTINCT t_child.name AS child_name, t_object_1.name AS Node
        FROM (t_object AS t_object_1
            INNER JOIN t_class AS tc2 ON t_object_1.class_id = tc2.class_id)
            INNER JOIN (t_membership
            INNER JOIN ((((t_data_0 AS d INNER JOIN t_key AS k
                ON d.key_id = k.key_id)
            INNER JOIN t_membership AS m
                ON k.membership_id = m.membership_id)
            INNER JOIN t_property AS p ON k.property_id = p.property_id)
            INNER JOIN ((t_object AS t_child
            INNER JOIN t_class AS tc ON t_child.class_id = tc.class_id)
            INNER JOIN t_category
                ON (t_child.category_id = t_category.category_id)
                AND (t_child.class_id = t_category.class_id))
                ON m.child_object_id = t_child.object_id)
                ON t_membership.parent_object_id = t_child.object_id)
                ON t_object_1.object_id = t_membership.child_object_id
        GROUP BY t_child.object_id, t_child.name, d.value,
        t_object_1.name, t_object_1.object_id, tc2.name,
        p.name, tc.name, t_category.name, t_child.name
        HAVING (((tc2.name)='Node')
            AND ((p.name)='SRMC')
            AND ((tc.name)='Generator')
            AND ((t_category.name)<>'Termicas Ficticias'
            And (t_category.name)<>'Hydro Gen Group A'
            And (t_category.name)<>'Hydro Gen Group B'
            And (t_category.name)<>'Hydro Gen Group C'
            And (t_category.name)<>'Thermal Gen N. Zone'
            And (t_category.name)<>'Thermal Gen S. Zone'));""")
    sql_NodeCMg = textwrap.dedent("""
        SELECT t_child.name as child_name, t_period_0.datetime,
        t_period_0.day_id, t_period_0.period_of_day, Sum(d.value) AS valor
        FROM (((((((((((t_data_0 AS d
            INNER JOIN t_key AS k ON d.key_id = k.key_id)
            INNER JOIN t_membership AS m ON k.membership_id = m.membership_id)
            INNER JOIN t_property AS p ON k.property_id = p.property_id)
            INNER JOIN t_unit ON p.unit_id = t_unit.unit_id)
            INNER JOIN t_collection AS c ON m.collection_id = c.collection_id)
            INNER JOIN t_object AS t_parent
                ON m.parent_object_id = t_parent.object_id)
            INNER JOIN t_object AS t_child
                ON m.child_object_id = t_child.object_id)
            INNER JOIN t_category
                ON (t_child.category_id = t_category.category_id)
                AND (t_child.class_id = t_category.class_id))
            INNER JOIN t_model ON k.model_id = t_model.model_id)
            INNER JOIN t_class AS tc ON t_child.class_id = tc.class_id)
            INNER JOIN t_phase_3 ON d.period_id = t_phase_3.period_id)
            INNER JOIN t_period_0
                ON t_phase_3.interval_id = t_period_0.interval_id
        WHERE (((p.name)='Price') AND ((tc.name)='Node'))
        GROUP BY  t_child.name, t_period_0.datetime,
        t_period_0.day_id, t_period_0.period_of_day;""")

    # EJECUCION DE CONSULTAS SQL
    # se utiliza la conexión con contextualizador para ejecutar las consultas
    # SQL y que se cierren.
    with engine.connect() as conn:
        df_curt = pd.read_sql(sql_curt, con=conn)
        df_gen = pd.read_sql(sql_gen, con=conn)
        df_genbar = pd.read_sql(sql_genbar, con=conn)
        df_cmg = pd.read_sql(sql_NodeCMg, con=conn)
        conn.close()
        engine.dispose()
        print("conexión exitosa, datos cargados\n")

    # INICIO PROCESAMIENTO DE DATOS
    # en esta sección se establecen las relaciones entre los diferentes
    # datos obtenidos y se unifican en un dataframe
    print("iniciando procesamiento de datos..")

    # Unificación entre curtailment y generación.
    df_curt = df_curt.merge(df_gen,
                            on=["child_name", "datetime"],
                            how="left")

    # Unificación entre curtailment, generación y asignación central-barra.
    df_curt = df_curt.merge(df_genbar,
                            on="child_name",
                            how="inner")

    # Creación de una salida ordenada por barra.
    df_curt_node = df_curt.groupby(by=[
        'Node',
        'datetime',
        'day_id',
        'period_of_day']).agg({'curtailment': 'sum',
                               'generation': 'sum'})
    # Se borran los indices.
    df_curt_node.reset_index(inplace=True)

    # Creación de una copia de cmg para procesar los datos
    df_cmg_aux = df_cmg.copy()
    df_cmg_aux.rename(columns={
        'child_name': 'Node',
        'valor': 'costo_marginal'},
                      inplace=True)

    # Union con los datos marginales.
    # Este es el DataFrame con toda la información necesaria.
    df_curt_node_cmg = df_curt_node.merge(df_cmg_aux,
                                          on=['Node',
                                              'datetime',
                                              'day_id',
                                              'period_of_day'],
                                          how='left')
    df_curt_node_cmg.reset_index(inplace=True)

    # Se guardan los DataFrame en formato .CSV para referencia
    # **En producción** este código podría comentarse para no generar
    # salidas que nose utilicen. Lo dejare comentado.
    print("procesamiento listo.\n")
    # df_curt.to_csv('Salidas/Curtailment.csv', index=False)
    # df_curt_node.to_csv('Salidas/Curtailment_byNode.csv', index=False)
    # df_cmg.to_csv('Salidas/CMg.csv', index=False)
    df_curt_node_cmg.to_csv(path_dir + '/Antecedentes/Curtailment_total.csv', index=False)

    # LECTURA DE RELACIONES
    # **BETA** en vez de usar un excel se puede crear un
    # json con las relaciones procesadas y facilitar el uso.
    # with open("nodos.json", "r") as f:
    #     relacion = json.load(f)

    # CREACION DE NODOS
    # para poder iterar sobre la topología del sistema se crea un
    # objeto de grafos con networkx.

    # Creación del DataFrame con la topología del sistema
    df_nodes = pd.read_excel(path_dir+PATH_NODES, sheet_name=0)

    # Creación del objeto grafo para el sistema
    G = nx.from_pandas_edgelist(df_nodes, 'child_name', 'Node')

    # LOOP PARA CALCULO CURTAILMENT
    # El loop parte desde la barra de referencia, verificando que tiene
    # marginales 0. Si esto es verdad empiezar a iterar sobre las barras
    # adjacentes, acumulando el curtailment, hasta que no encuentre barras
    # con marginales 0, en las mismas horas que la barra de referencia.
    # El loop se realiza para los 2 primeros días, si se quiere agregar
    # más días se debe modificar la variable "criterio_dias"
    print("Inicio del proceso de determinación de curtailment...")

    # se define barra de inicio y criterios de inicio

    df_curt_dia2 = run_loop(2, "Andes220", G, df_curt_node_cmg)
    df_curt_dia3 = run_loop(3, "Andes220", G, df_curt_node_cmg)
    df_curt_dia4 = run_loop(4, "Andes220", G, df_curt_node_cmg)
    df_curt_dia5 = run_loop(5, "Andes220", G, df_curt_node_cmg)
    df_curt_dia6 = run_loop(6, "Andes220", G, df_curt_node_cmg)
    
    print('Guardando...')
    df_curt_final = pd.concat([df_curt_dia2,
                               df_curt_dia3,
                               df_curt_dia4,
                               df_curt_dia5,
                               df_curt_dia6],
                              axis=0)
    df_curt_final.to_csv(path_dir + '/Antecedentes/Curtailment_Andes.csv',
                         index=False)
    print('guardado csv final: Curtailment_Andes.csv')

    termino = input("Terminado! presione una tecla...")

if __name__ == "__main__":
    main()
