from neo4j import GraphDatabase
from datetime import date
import api

uri = "bolt://localhost:7687"
username="neo4j"
password="abcd1234"
db = "scott"

class Neo4jConnection:
    def __init__(self, uri, username, password):
        self._uri = uri
        self._username = username
        self._password = password
        self._driver = None

    def connect(self):
        self._driver = GraphDatabase.driver(self._uri, auth=(self._username, self._password))

    def close(self):
        if self._driver is not None:
            self._driver.close()

    def query(self, query, parameters=None):
        assert self._driver is not None, "Conexión no inicializada"
        session = self._driver.session(database=db) if db is not None else self._driver.session()
        result = list(session.run(query, parameters))
        session.close()
        return result

# Ejemplo de uso
conexion = Neo4jConnection(uri, username, password)
conexion.connect()

# Ejemplo de consulta
query = "MATCH (n) RETURN n LIMIT 5"
resultados = conexion.query(query)

for resultado in resultados:
    print(resultado)

#res = api.crear_nodo_dept(conexion,1,"ventas","NY")
#print(res)

#res = api.crear_nodo_emp(conexion,1,"Gil","QA",None,date.today(),1000,0,1)
#for resultado in res:
#    print(resultado)

#print("RELACION")
#res = api.crear_relacion_1_n(conexion,1,1)
#print(res)

#print("RELACIONES")
#res = api.consultar_relaciones_nodo(conexion,"EMP","empno",1)
#res = api.obtener_resultados_formateados(res)

#print(res)


#print("CONSULTA")
#res = api.consultar_nodo(conexion,"EMP","empno",1)
#print(res)

#print("VALORES")
#print(res[0].values())
#print(type(res))
#print(type(res[0]))


def seleccionar_tipo():
    print("Seleccione el tipo de nodo")
    print("1.- EMP")
    print("2.- DEPT")
    opt = int(input("Su opción: "))
    if  opt == 1:
        return "EMP"
    if opt == 2:
        return "DEPT"


#res = api.crear_relacion_manager(conexion,3,"1")
#print(res)

nodo_tipo = ""
opt = -99
while True:
    try:
        print("******Menú******")
        print("1.- Crear")
        print("2.- Leer")
        print("3.- Actualizar")
        print("4.- Eliminar")
        print("99.- Salir")
        opt = int(input("Su elección: "))

        if opt == 1:
            print("Crear")
            nodo_tipo = seleccionar_tipo()
            if nodo_tipo == "DEPT":
                deptno = int(input("Ingrese el número de departamento: "))
                dname = input("Ingrese el nombre del departamento: ")
                loc = input("Ingrese la localización del departamento: ")
                api.crear_nodo_dept(conexion,deptno,dname,loc)
            if nodo_tipo == "EMP":
                empno = int(input("Ingrese el número de empleado: "))
                ename = input("Ingrese el nombre del empleado: ")
                job = input("Ingrese el trabajo del empleado: ")
                mgr = input("Ingrese el id del manager (vacío sí no tiene manager): ")
                hiredate = date.today()
                sal = int(input("Ingrese el salario del empleado (sólo números): "))
                comm = input("Ingrese la comisión del usuario (vacío si no tiene comisión): ")
                deptno = int(input("Ingrese el número de departamento: "))
                api.crear_nodo_emp(conexion,empno,ename,job,mgr,hiredate,sal,comm,deptno)
                
        if opt == 2:
            print("Leer")
            nodo_tipo = seleccionar_tipo()
            if nodo_tipo == "DEPT":
                deptno = int(input("Ingrese el numero de departamento: "))
                api.obtener_datos_nodo_dept_y_empleados(conexion,deptno)
            if nodo_tipo == "EMP":
                empno = int(input("Ingrese el numero de empleado: "))
                api.obtener_datos_nodo_emp(conexion,empno)
        if opt == 3:
            nodo_tipo = seleccionar_tipo()
            if nodo_tipo == "DEPT":
                deptno = int(input("Ingrese el número de departamento"))
                dname = input("Ingrese el nuevo nombre (vacío para no actualizar)")
                loc = input("INgrese la nueva localización")
                #actualizar
                api.actualizar_nodo_dept(conexion,deptno,dname,loc)
            if nodo_tipo == "EMP":
                tipo = int(input("Actualizar nodo o relación? \n1)Nodo\n2)Relación\nSu opción: "))
                if tipo == 1:
                    empno = int(input("Ingrese el número del empleado"))
                    ename = input("Ingrese nombre (vacío para no cambiar)")
                    comm = input("Ingrese la comisión (vacío para no cambiar)")
                    job = input("Ingrese el nombre del trabajo (vacío para no actualizar): ")
                    salary = input("Ingrese el salario (vacío para no actualizar): ")
                    api.actualizar_nodo_emp(conexion, empno,ename if ename != '' else None,comm if comm != '' else None,job if job != '' else None,salary if salary != '' else None)
                if tipo == 2:
                    aux = int(input("Desea cambiar manager o departamento?\n1)Manager\n2)Departamento\nSu opción: "))
                    if aux == 1:
                        #manager
                        empno = int(input("Ingrese el número de empleado: "))
                        num = int(input("Ingrese el número de empleado del nuevo manager: "))
                        api.actualizar_manager_empleado(conexion, empno, num)

                    if aux == 2:
                        #departamento
                        empno = int(input("Ingrese el número de empleado: "))
                        num = int(input("Ingrese el número del nuevo departamento: "))
                        api.actualizar_departamento_empleado(conexion, empno, num)
        if opt == 4:
            print("Eliminar")
            nodo_tipo = seleccionar_tipo()
            if nodo_tipo == "DEPT":
                id = int(input("Ingrese el número del departamento a eliminar: "))
                api.eliminar_nodo(conexion,"DEPT","deptno",id)
                print("Departamento eliminado.")
            if nodo_tipo == "EMP":
                id = int(input("Ingrese el número del empleado a eliminar: "))
                api.eliminar_nodo(conexion,"EMP","empno",id)
                print("Empleado eliminado.")
        if opt == 99:
            break

        input("Pulse cualquier tecla para continuar...")
    except Exception as error:
    # Capturamos cualquier excepción y la almacenamos en la variable 'error'
        print(f"Error: {error}")
conexion.close()