import mysql.connector
from mysql.connector import Error
import time

def conexion():
    try: 
        conexion = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            password='',
            db='alquiler'
        )  
        return conexion
    except Error as ex:
        print("Error durante la conexion",ex)
        return None
    # finally:
    #     if conexion.is_connected():
    #         conexion.close()
    #         cursor.close()
    #         print("la conexion ha finalizado.")    


def menu():
    while True:
        conexion_db = conexion()
        print("********************************************\n")        
        print("Bienvenido a este simulador DML a partir de una base de datos de alquiler de vehículos\n")
        print("********************************************")
        print("¿Qué operación desea realizar el día de hoy?")
        print("1-Select")
        print("2-Update")
        print("3-Insert")
        print("4-Delete")
        print("5-Salir")
        opc = int(input("Digite opción: ->"))

        if opc == 1:
            select(conexion_db)
            time.sleep(2)
        elif opc == 2:
            update(conexion_db)
            
        elif opc == 3:
            insert(conexion_db)
            
        elif opc == 4:
            delete(conexion_db)
            
        elif opc == 5:
            return
        else:
            print("Por favor digite una opción correcta.")

def select(conexion):
    if conexion.is_connected():
            print("conexion exitosa.")
            cursor = conexion.cursor()
            cursor.execute("SELECT database();")
            registro = cursor.fetchone()
            print("conectado a la BD : ", registro)
            infoserver = conexion.get_server_info()
            cursor.execute("SELECT * FROM dataset_autos ORDER BY ID DESC LIMIT 10;")
            resutados = cursor.fetchall()
            for fila in resutados:
                print(fila[0], fila[1], fila[2], fila[3], fila[4], fila[5], fila[6], fila[7],fila[8],fila[9])
            print("informacion del servidor",infoserver)
            print("total de registrros: ",  cursor.rowcount)
            cursor.close()

def update(conexion):
    if conexion.is_connected():
        cursor = conexion.cursor()
        ID_vehiculo = int(input("Ingrese el id del vehículo a actualizar: "))
        nuevo_precio = float(input("Ingrese el nuevo precio: "))
        
        try:
            cursor.execute("UPDATE dataset_autos SET Precio = %s WHERE ID = %s", (nuevo_precio, ID_vehiculo))
            conexion.commit()
            print("¡Actualización exitosa!")
        except Error as ex:
            print("Error durante la actualización:", ex)
        finally:
            cursor.close()

def insert(conexion):
    if conexion.is_connected():
        cursor = conexion.cursor()
        marca = input("Ingrese la marca del vehículo: ")
        modelo = input("Ingrese el modelo del vehículo: ")
        precio = float(input("Ingrese el precio del vehículo: "))
        año = int(input("Ingrese el año del vehículo: "))
        kilometraje = float(input("Ingrese el kilometraje del vehículo: "))
        combustible = input("Ingrese el tipo de combustible: ")
        transmision = input("Ingrese el tipo de transmisión: ")
        color = input("Ingrese el color del vehículo: ")
        motor = input("Ingrese el motor del vehículo: ")
        
        try:
            cursor.execute("INSERT INTO dataset_autos (marca, modelo, precio, anio, kilometraje, comustible, transmision, color, motor) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", 
                           (marca, modelo, precio, año, kilometraje, combustible, transmision, color, motor))
            conexion.commit()
            print("¡Inserción exitosa!")
        except Error as ex:
            print("Error durante la inserción:", ex)
        finally:
            cursor.close()


def delete(conexion):
    if conexion.is_connected():
        cursor = conexion.cursor()
        id_vehiculo = int(input("Ingrese el id del vehículo a eliminar: "))
        
        try:
            cursor.execute("DELETE FROM dataset_autos WHERE ID = %s", (id_vehiculo,))
            conexion.commit()
            print("¡Eliminación exitosa!")
        except Error as ex:
            print("Error durante la eliminación:", ex)
        finally:
            cursor.close()

    
def main():

    conexion()
    time.sleep(2)
    menu()



if __name__ == "__main__":
    main()