from sqlalchemy import text

class DimensionLoader:
    def __init__(self, engine, logger, bulk_loader):
        self.engine = engine
        self.logger = logger
        self.bulk_loader = bulk_loader

    def truncate_tables(self):
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM [Fact].[Opiniones]"))

            conn.execute(text("DELETE FROM [Dim].[Cliente]"))
            conn.execute(text("DELETE FROM [Dim].[Producto]"))
            conn.execute(text("DELETE FROM [Dim].[Fecha]"))
            conn.execute(text("DELETE FROM [Dim].[Fuente]"))
            conn.commit()

    def load_clientes_from_facts(self, transformed_data):
        unique_clientes = list({
            int(row["cliente_id"])
            for row in transformed_data
            if row["cliente_id"] not in (None, "", "nan")
        })

        data = [
            {
                "Cliente_ID": cliente_id,
                "Nombre": f"Cliente_{cliente_id}",
                "Email": f"cliente{cliente_id}@mail.com"
            }
            for cliente_id in unique_clientes
        ]

        self.bulk_loader.bulk_insert("Cliente", data, schema="Dim")

    def load_productos_from_facts(self, transformed_data):
        unique_productos = list({
            int(row["producto_id"])
            for row in transformed_data
            if row["producto_id"] not in (None, "", "nan")
        })

        data = [
            {
                "Producto_ID": producto_id,
                "Nombre": f"Producto_{producto_id}",
                "Categoria_ID": None
            }
            for producto_id in unique_productos
        ]

        self.bulk_loader.bulk_insert("Producto", data, schema="Dim")

    def load_fuentes(self, fuentes):
        unique_fuentes = list(set(fuentes))

        data = [
            {
                "Fuente_ID": idx + 1,
                "Tipo": fuente
            }
            for idx, fuente in enumerate(unique_fuentes)
        ]

        self.bulk_loader.bulk_insert("Fuente", data, schema="Dim")

    def load_fecha(self, fechas):
        unique_fechas = list(set(fechas))

        data = [
            {
                "Fecha": fecha,
                "Año": fecha.year,
                "Mes": fecha.month,
                "Trimestre": (fecha.month - 1) // 3 + 1
            }
            for fecha in unique_fechas
        ]

        self.bulk_loader.bulk_insert("Fecha", data, schema="Dim")