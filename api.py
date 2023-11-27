def crear_nodo_dept(conexion, deptno, dname, loc):
    query = (
        "CREATE (dept:DEPT {deptno: $deptno, dname: $dname, loc: $loc})"
        "RETURN dept"
    )
    parametros = {"deptno": deptno, "dname": dname, "loc": loc}
    return conexion.query(query, parametros)

def crear_nodo_emp(conexion,empno, ename, job, mgr, hiredate, sal, comm, deptno):
    if mgr != None:
        mgr = int(mgr)
    query = (
        "CREATE (emp:EMP {empno: $empno,ename: $ename, job: $job, mgr: $mgr, hiredate: $hiredate, sal: $sal, comm: $comm, deptno: $deptno})"
        "RETURN emp"
    )
    parametros = {"empno":empno,"ename": ename, "job": job, "mgr":mgr, "hiredate":hiredate, "sal":sal, "comm": comm, "deptno":deptno}
    nodo_res = conexion.query(query, parametros)

    #crear relación a departamento
    crear_relacion_emp_trabaja_en_departamento(conexion,empno,deptno)

    if mgr != None:
        crear_relacion_manager(conexion,empno,mgr)
    
    return nodo_res

def crear_relacion_emp_trabaja_en_departamento(conexion, empno, deptno):
    query = (
        "MATCH (emp:EMP {empno: $empno}), (dept:DEPT {deptno: $deptno})"
        "CREATE (emp)-[:WORKS_AT]->(dept)"
    )
    parametros = {"empno": empno, "deptno": deptno}
    return conexion.query(query, parametros)

def crear_relacion_manager(conexion, empno_subordinado, empno_manager):
    print("ENTRA")
    query = (
        "MATCH (subordinado:EMP {empno: $empno_subordinado}), (manager:EMP {empno: $empno_manager})"
        "CREATE (subordinado)-[:IS_SUBORDINATE_OF]->(manager)"
    )
    parametros = {"empno_subordinado": empno_subordinado, "empno_manager": empno_manager}
    return conexion.query(query, parametros)


def consultar_nodo(conexion, nodo_label, id_type, id_num):
    query = (
        f"MATCH (n:{nodo_label} {{{id_type}: $id_num}})"
        "RETURN n"
    )
    parametros = {"id_num": id_num}
    return conexion.query(query, parametros)

def consultar_relaciones_nodo(conexion, nodo_label, propiedad, valor):
    query = (
        f"MATCH (n:{nodo_label} {{{propiedad}: $valor}})-[r]-(m)"
        "RETURN n, r, m"
    )
    parametros = {"valor": valor}
    return conexion.query(query, parametros)

def obtener_resultados_formateados(resultados):
    resultados_formateados = []

    for record in resultados:
        nodo = record['n']
        relacion = record['r']
        nodo_relacionado = record['m']

        # Formatear la información del nodo y la relación en diccionarios
        info_nodo = dict(nodo.items())
        info_relacion = dict(relacion.items()) if relacion is not None else None
        info_nodo_relacionado = dict(nodo_relacionado.items()) if nodo_relacionado is not None else None

        # Agregar diccionarios al resultado formateado
        resultado_formateado = {
            "empleado": info_nodo,
            "departamento": info_nodo_relacionado
        }


    return resultado_formateado

def eliminar_nodo(conexion, nodo_label, propiedad, valor):
    query = (
        f"MATCH (n:{nodo_label} {{{propiedad}: $valor}})"
        "DETACH DELETE n"
    )
    parametros = {"valor": valor}
    return conexion.query(query, parametros)