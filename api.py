def crear_nodo_dept(conexion, deptno, dname, loc):
    query = (
        "CREATE (dept:DEPT {deptno: $deptno, dname: $dname, loc: $loc})"
        "RETURN dept"
    )
    parametros = {"deptno": deptno, "dname": dname, "loc": loc}
    return conexion.query(query, parametros)

def crear_nodo_emp(conexion,empno, ename, job, mgr, hiredate, sal, comm, deptno):
    if mgr != '':
        mgr = int(mgr)
    query = (
        "CREATE (emp:EMP {empno: $empno,ename: $ename, job: $job, mgr: $mgr, hiredate: $hiredate, sal: $sal, comm: $comm, deptno: $deptno})"
        "RETURN emp"
    )
    parametros = {"empno":empno,"ename": ename, "job": job, "mgr":mgr, "hiredate":hiredate, "sal":sal, "comm": comm, "deptno":deptno}
    nodo_res = conexion.query(query, parametros)

    #crear relación a departamento
    crear_relacion_emp_trabaja_en_departamento(conexion,empno,deptno)

    if mgr != '':
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


def obtener_datos_nodo_emp(conexion, empno):
    query = (
        "MATCH (emp:EMP {empno: $empno})"
        "RETURN emp"
    )
    parametros = {"empno": empno}
    resultados = conexion.query(query, parametros)

    if not resultados:
        print(f"No se encontró un nodo EMP con el número de empleado {empno}.")
        return

    nodo_emp = resultados[0]['emp']

    print(f"Datos del nodo EMP con número de empleado {empno}:")
    print(f"Número de Empleado: {nodo_emp['empno']}")
    print(f"Nombre: {nodo_emp['ename']}")
    print(f"Puesto: {nodo_emp['job']}")
    print(f"Fecha de Contratación: {nodo_emp['hiredate']}")
    print(f"Salario: {nodo_emp['sal']}")
    print(f"Comisión: {nodo_emp['comm']}")
    print(f"Número de Departamento: {nodo_emp['deptno']}")

    return nodo_emp


def obtener_datos_nodo_dept_y_empleados(conexion, deptno):
    query = (
        "MATCH (dept:DEPT {deptno: $deptno})"
        "OPTIONAL MATCH (dept)<-[:WORKS_AT]-(emp:EMP)"
        "RETURN dept, COLLECT(emp) AS empleados"
    )
    parametros = {"deptno": deptno}
    resultados = conexion.query(query, parametros)

    if not resultados:
        print(f"No se encontró un nodo DEPT con el número de departamento {deptno}.")
        return

    nodo_dept = resultados[0]['dept']
    empleados = resultados[0]['empleados']

    print(f"Datos del nodo DEPT con número de departamento {deptno}:")
    print(f"Número de Departamento: {nodo_dept['deptno']}")
    print(f"Nombre: {nodo_dept['dname']}")
    print(f"Ubicación: {nodo_dept['loc']}")
    print("\nEmpleados en este departamento:")

    for empleado in empleados:
        if empleado is not None:
            print(f"  Número de Empleado: {empleado['empno']}")
            print(f"  Nombre: {empleado['ename']}")
            print(f"  Puesto: {empleado['job']}")
            print(f"  Fecha de Contratación: {empleado['hiredate']}")
            print(f"  Salario: {empleado['sal']}")
            print(f"  Comisión: {empleado['comm']}")
            print(f"  -------------------------")
        else:
            print("  No hay empleados asociados a este departamento.")

    return nodo_dept, empleados



def actualizar_nodo_dept(conexion, deptno, nuevo_nombre, nueva_loc):
    set_query = ""
    parametros = {"deptno": deptno}

    if nuevo_nombre:
        set_query += "SET dept.dname = $nuevo_nombre "
        parametros["nuevo_nombre"] = nuevo_nombre

    if nueva_loc:
        set_query += "SET dept.loc = $nueva_loc "
        parametros["nueva_loc"] = nueva_loc

    query = (
        f"MATCH (dept:DEPT {{deptno: $deptno}}) "
        f"{set_query} "
        "RETURN dept"
    )
    return conexion.query(query, parametros)


def actualizar_nodo_emp(conexion, empno, nuevo_nombre=None, nueva_comision=None, nuevo_trabajo=None, nuevo_salario=None):
    set_query = ""
    parametros = {"empno": empno}

    if nuevo_nombre is not None:
        set_query += "SET emp.ename = $nuevo_nombre "
        parametros["nuevo_nombre"] = nuevo_nombre

    if nueva_comision is not None:
        set_query += "SET emp.comm = $nueva_comision "
        parametros["nueva_comision"] = nueva_comision

    if nuevo_trabajo is not None:
        set_query += "SET emp.job = $nuevo_trabajo "
        parametros["nuevo_trabajo"] = nuevo_trabajo

    if nuevo_salario is not None:
        set_query += "SET emp.sal = $nuevo_salario "
        parametros["nuevo_salario"] = nuevo_salario

    query = (
        f"MATCH (emp:EMP {{empno: $empno}}) "
        f"{set_query} "
        "RETURN emp"
    )
    return conexion.query(query, parametros)

def cambiar_valor_manager_empleado(conexion, empno, nuevo_manager):
    query = (
        "MATCH (e:EMP {empno: $empno}) "
        "SET e.mgr = $nuevo_manager "
        "RETURN e"
    )
    parametros = {"empno": empno, "nuevo_manager": nuevo_manager}
    return conexion.query(query, parametros)



def actualizar_manager_empleado(conexion, empno, nuevo_manager):
    query = (
        "MATCH (e:EMP {empno: $empno}), (m:EMP {empno: $nuevo_manager}) "
        "CREATE (e)-[r:REPORTS_TO]->(m) "
        "RETURN r"
    )
    parametros = {"empno": empno, "nuevo_manager": nuevo_manager}
    cambiar_valor_manager_empleado(conexion,empno,nuevo_manager)
    return conexion.query(query, parametros)


def cambiar_valor_departamento_empleado(conexion, empno, nuevo_departamento):
    query = (
        "MATCH (e:EMP {empno: $empno}) "
        "SET e.deptno = $nuevo_departamento "
        "RETURN e"
    )
    parametros = {"empno": empno, "nuevo_departamento": nuevo_departamento}
    return conexion.query(query, parametros)


def actualizar_departamento_empleado(conexion, empno, nuevo_departamento):
    query = (
        "MATCH (e:EMP {empno: $empno}), (d:DEPT {deptno: $nuevo_departamento}) "
        "CREATE (e)-[:WORKS_AT]->(d) "
        "RETURN e"
    )
    parametros = {"empno": empno, "nuevo_departamento": nuevo_departamento}
    cambiar_valor_departamento_empleado(conexion, empno, nuevo_departamento)
    return conexion.query(query, parametros)



